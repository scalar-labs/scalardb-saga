package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaUnavailableException;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import com.sun.net.httpserver.HttpServer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
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
 * certificate, driven by clients that verify it — the only place Jetty's in-memory-keystore path
 * and Netty's PEM path are proven to serve identical material. Also pins the runtime policy around
 * hostile-but-routine connections: plaintext clients and bare TCP probes (load balancers, the smoke
 * test) must neither disturb the server nor push anything above INFO into the logs.
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

  @BeforeAll
  static void generateTlsMaterial() {
    tls = TlsTestCerts.generateRsa(tlsDir, "server");
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

  @Override
  protected void configureProperties(Properties props) {
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, tls.certChainPath().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, tls.privateKeyPath().toString());
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
      try (Socket probe = new Socket("127.0.0.1", grpcPort())) {
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
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty("scalar.db.contact_points", "jdbc:sqlite:" + db.toAbsolutePath());
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
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

  private HttpResponse<String> httpsGet(String url) throws Exception {
    return httpsClient()
        .send(HttpRequest.newBuilder(URI.create(url)).GET().build(), BodyHandlers.ofString());
  }

  /** An HTTPS client trusting exactly the generated test CA (the server's own certificate). */
  private static HttpClient httpsClient() throws Exception {
    KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
    trust.load(null, null);
    trust.setCertificateEntry("test-ca", tls.certificate());
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
