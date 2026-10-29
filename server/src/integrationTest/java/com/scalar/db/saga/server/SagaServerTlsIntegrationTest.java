package com.scalar.db.saga.server;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaUnavailableException;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import com.scalar.db.saga.integration.IntegrationTestStore;
import com.sun.net.httpserver.HttpServer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end TLS coverage: a real {@link SagaServer} serving TLS on <b>both</b> transports from one
 * certificate, driven by clients that verify it — the only place Jetty's and Netty's consumption of
 * the shared key manager are proven to serve identical material, at boot and across a rotation.
 * Also pins the runtime policy around hostile-but-routine connections: plaintext clients, bare TCP
 * probes (load balancers, the smoke test) and clients still trusting a rotated-out certificate must
 * neither disturb the server nor push anything above INFO into the logs.
 *
 * <p>The BouncyCastle-free classpath here is deliberate: Netty widens its PEM parsing when BC is
 * present, and this suite exists to exercise the production parse path (see {@link TlsTestCerts}).
 */
class SagaServerTlsIntegrationTest extends ServerIntegrationTestSupport {

  private static final String DEFINITION =
      withService(
          """
          { "name": "saga", "mode": "SAGA", "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit" },
              "compensation": { "method": "POST", "path": "/reverse" } } ] }
          """);

  @TempDir static Path tlsDir;
  private static TlsTestCerts.PemPair tls;
  private static TlsTestCerts.PemPair rotated;

  // The files the server reads: a per-test copy of the boot pair, so a rotation test can overwrite
  // them without the next test booting on a rotated certificate.
  @TempDir Path liveDir;

  @BeforeAll
  static void generateTlsMaterial() {
    tls = TlsTestCerts.generateRsa(tlsDir, "server");
    rotated = TlsTestCerts.generateRsa(tlsDir, "rotated");
  }

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/debit", 200);
    route(participant, "/reverse", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, "saga", DEFINITION);
  }

  private Path liveCert() {
    return liveDir.resolve("tls.crt");
  }

  private Path liveKey() {
    return liveDir.resolve("tls.key");
  }

  @Override
  protected void configureProperties(Properties props) throws IOException {
    Files.copy(tls.certChainPath(), liveCert());
    Files.copy(tls.privateKeyPath(), liveKey());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, liveCert().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, liveKey().toString());
  }

  @Test
  void tls_oneCertificate_servesHttpsHealthAndGrpcSagaRoundTrip() throws Exception {
    // Assert (HTTPS): a client trusting the test CA verifies the certificate and reads /health.
    HttpResponse<String> health = httpsGet("https://localhost:" + httpPort() + "/health");
    assertThat(health.statusCode()).isEqualTo(200);
    assertThat(health.body()).contains("UP");

    // Assert (gRPC): the SDK's private-CA surface — trustCaCertificate + overrideAuthority while
    // dialing by IP — runs a full saga over the same certificate.
    GrpcSagaOrchestratorClient client =
        GrpcSagaOrchestratorClient.newBuilder()
            .target("127.0.0.1:" + grpcPort())
            .useTransportSecurity()
            .trustCaCertificate(tls.certChainPath())
            .overrideAuthority("localhost")
            .build();
    try {
      String sagaId = client.start("saga", Map.of());
      assertThat(client.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
    } finally {
      client.close();
    }
  }

  @Test
  void tls_httpsDialedByIp_succeedsWithoutSniCheck() throws Exception {
    // Kubernetes probes, port-forwards, and the smoke test dial by IP (no usable SNI). Jetty's
    // default sniHostCheck would answer 400 "Invalid SNI"; this pins the decision to disable it.
    HttpResponse<String> health = httpsGet("https://127.0.0.1:" + httpPort() + "/health");

    assertThat(health.statusCode()).isEqualTo(200);
  }

  @Test
  void plaintextClients_againstTlsPorts_failClientSideWithoutServerLogNoise() throws Exception {
    try (LogCapture logs = LogCapture.ofRoot()) {
      // Act / Assert (HTTP): the support's plaintext helper now talks to a TLS port and fails on
      // the client side — which is also the proof that no plaintext listener exists.
      assertThatThrownBy(() -> get("/health")).isInstanceOf(IOException.class);

      // Act / Assert (gRPC): a plaintext SDK client sees UNAVAILABLE, mapped to the api
      // exception.
      GrpcSagaOrchestratorClient plaintext =
          GrpcSagaOrchestratorClient.create("127.0.0.1:" + grpcPort());
      try {
        assertThatThrownBy(() -> plaintext.getStateSnapshot("nope"))
            .isInstanceOf(SagaUnavailableException.class);
      } finally {
        plaintext.close();
      }

      // Assert (policy): handshake garbage is routine (load balancers, scanners); it must stay
      // at DEBUG or below, or real deployments drown in it.
      assertThat(transportNoiseAtWarnOrAbove(logs.events())).isEmpty();
    }
  }

  @Test
  @SuppressFBWarnings(
      value = "UNENCRYPTED_SOCKET",
      justification =
          "The plaintext socket is the test subject: a bare TCP probe against the TLS port, the"
              + " shape load balancers and the smoke test produce")
  void bareTcpConnectAndClose_onTlsGrpcPort_serverKeepsServingQuietly() throws Exception {
    try (LogCapture logs = LogCapture.ofRoot()) {
      // Arrange — the LB health check / smoke probe shape: connect, send nothing, close.
      // The fixture binds 127.0.0.1, so parse that literal: getLoopbackAddress() is ::1 when the
      // JVM prefers IPv6, and nothing listens there.
      try (Socket probe = new Socket(InetAddress.ofLiteral("127.0.0.1"), grpcPort())) {
        assertThat(probe.isConnected()).isTrue();
      }

      // Act — the server must keep serving TLS afterwards. (Deliberately the one client that
      // uses trustCaCertificate without overrideAuthority, covering that builder cell.)
      GrpcSagaOrchestratorClient client =
          GrpcSagaOrchestratorClient.newBuilder()
              .target("localhost:" + grpcPort())
              .useTransportSecurity()
              .trustCaCertificate(tls.certChainPath())
              .build();
      try {
        String sagaId = client.start("saga", Map.of());
        assertThat(client.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      } finally {
        client.close();
      }

      // Assert
      assertThat(transportNoiseAtWarnOrAbove(logs.events())).isEmpty();
    }
  }

  @Test
  void constructor_mismatchedKeyAndCert_failsBeforeAnyBindWithoutPemContent(@TempDir Path dir)
      throws Exception {
    // Arrange — a full real-server config (sqlite store) whose key comes from a different
    // issuance than the certificate: the renewed-cert-with-stale-key case.
    TlsTestCerts.PemPair other = TlsTestCerts.generateRsa(dir, "other");
    Path db = dir.resolve("mismatch.db");
    Path definitions = dir.resolve("defs");
    Files.createDirectories(definitions);
    writeDefinition(definitions, "saga", DEFINITION);
    Properties props = new Properties();
    IntegrationTestStore.configure(props, db);
    props.setProperty("scalar.db.saga.store.scalardb.num_buckets", "1");
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, definitions.toString());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, tls.certChainPath().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, other.privateKeyPath().toString());

    // Act / Assert — the constructor throws (so no port ever binds; start() is never reachable),
    // names both keys, and leaks nothing from inside the files.
    assertThatThrownBy(() -> new SagaServer(SagaServerConfig.load(props)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageNotContaining("-----")
        .hasMessageNotContaining("MII")
        .hasNoCause();
  }

  @Test
  void
      reloadTlsNow_rotatedPairWritten_newConnectionsOnBothTransportsGetItAndAnEstablishedOneKeepsServing()
          throws Exception {
    try (LogCapture logs = LogCapture.ofRoot()) {
      // Arrange — a gRPC connection established under the boot certificate, with a saga behind it
      GrpcSagaOrchestratorClient established = grpcClient(tls.certChainPath());
      try {
        String sagaId = established.start("saga", Map.of());

        // Act — a rotation as the kubelet delivers one: both files replaced, then a pass
        Files.copy(rotated.certChainPath(), liveCert(), REPLACE_EXISTING);
        Files.copy(rotated.privateKeyPath(), liveKey(), REPLACE_EXISTING);
        reloadTlsNow();

        // Assert (HTTPS) — a new client trusting only the rotated certificate is served, and one
        // trusting only the boot certificate is refused: the proof the swap happened, not merely
        // that both are accepted.
        String health = "https://localhost:" + httpPort() + "/health";
        assertThat(httpsGet(health, rotated.certificate()).statusCode()).isEqualTo(200);
        assertThatThrownBy(() -> httpsGet(health, tls.certificate()))
            .isInstanceOf(IOException.class);

        // Assert (gRPC) — the same, over the SDK
        GrpcSagaOrchestratorClient fresh = grpcClient(rotated.certChainPath());
        try {
          assertThat(fresh.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
        } finally {
          fresh.close();
        }
        GrpcSagaOrchestratorClient stale = grpcClient(tls.certChainPath());
        try {
          assertThatThrownBy(() -> stale.getStateSnapshot(sagaId))
              .isInstanceOf(SagaUnavailableException.class);
        } finally {
          stale.close();
        }

        // Assert (established) — the pre-rotation connection is untouched: a key manager is
        // consulted only at handshake
        assertThat(established.getStateSnapshot(sagaId).getStatus())
            .isEqualTo(SagaStatus.COMPLETED);
      } finally {
        established.close();
      }

      // Assert (policy) — the stale clients' refused handshakes are routine during a rotation and
      // stay below WARN, like every other hostile-but-routine connection here
      assertThat(transportNoiseAtWarnOrAbove(logs.events())).isEmpty();
    }
  }

  @Test
  void reloadTlsNow_mismatchedPairWritten_keepsServingTheBootCertificateAndWarnsOnce()
      throws Exception {
    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Arrange — the torn rotation a kubelet symlink flip yields between the two reads: a new
      // key beside the old certificate
      Files.copy(rotated.privateKeyPath(), liveKey(), REPLACE_EXISTING);

      // Act — two passes: the same rejection must not be warned twice
      reloadTlsNow();
      reloadTlsNow();

      // Assert — both transports still serve the boot certificate, and the server is healthy
      assertThat(httpsGet("https://localhost:" + httpPort() + "/health").statusCode())
          .isEqualTo(200);
      GrpcSagaOrchestratorClient client = grpcClient(tls.certChainPath());
      try {
        String sagaId = client.start("saga", Map.of());
        assertThat(client.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      } finally {
        client.close();
      }

      // Assert — one WARN naming the rejection and the key, nothing from inside the files
      assertThat(logs.events().stream().filter(event -> event.getLevel() == Level.WARN))
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getFormattedMessage()).contains("rejected");
                assertThat(event.getFormattedMessage())
                    .contains(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
                assertThat(event.getFormattedMessage()).doesNotContain("-----");
              });
    }
  }

  private HttpResponse<String> httpsGet(String url) throws Exception {
    return httpsGet(url, tls.certificate());
  }

  private static HttpResponse<String> httpsGet(String url, X509Certificate trusted)
      throws Exception {
    return httpsClient(trusted)
        .send(HttpRequest.newBuilder(URI.create(url)).GET().build(), BodyHandlers.ofString());
  }

  /** A gRPC SDK client trusting exactly {@code trustedCertChain}, dialing by IP. */
  private GrpcSagaOrchestratorClient grpcClient(Path trustedCertChain) {
    return GrpcSagaOrchestratorClient.newBuilder()
        .target("127.0.0.1:" + grpcPort())
        .useTransportSecurity()
        .trustCaCertificate(trustedCertChain)
        .overrideAuthority("localhost")
        .build();
  }

  /** An HTTPS client trusting exactly {@code trusted} (a server's own self-signed certificate). */
  private static HttpClient httpsClient(X509Certificate trusted) throws Exception {
    KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
    trust.load(null, null);
    trust.setCertificateEntry("test-ca", trusted);
    TrustManagerFactory trustManagers =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(trust);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagers.getTrustManagers(), null);
    return HttpClient.newBuilder().sslContext(sslContext).build();
  }

  /**
   * WARN-or-above events from the loggers that carry handshake noise. Scoped to the transport
   * stacks rather than asserting a globally quiet root: a live server with background
   * recovery/retention running can emit an unrelated WARN at any moment, and failing a TLS test on
   * it would manufacture a flake with a misleading message.
   */
  private static List<String> transportNoiseAtWarnOrAbove(List<ILoggingEvent> logs) {
    return logs.stream()
        .filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
        .filter(
            event -> {
              String logger = event.getLoggerName();
              return logger.startsWith("org.eclipse.jetty")
                  || logger.startsWith("io.grpc")
                  || logger.startsWith("io.netty");
            })
        .map(event -> event.getLoggerName() + ": " + event.getFormattedMessage())
        .collect(Collectors.toList());
  }
}
