package com.scalar.db.saga.server;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EgressTrustTest {

  // Generated once per class (keytool is a subprocess). Each pair is self-signed, so its
  // certificate is the CA a client must trust to reach a server presenting it.
  @TempDir static Path materialDir;
  private static TlsTestCerts.PemPair participantA;
  private static TlsTestCerts.PemPair participantB;

  @TempDir Path dir;
  private @Nullable HttpsServer server;

  @BeforeAll
  static void generateMaterial() {
    participantA = TlsTestCerts.generateRsa(materialDir, "participant-a");
    participantB = TlsTestCerts.generateEc(materialDir, "participant-b");
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }

  @Test
  void sslContext_bundleHoldsServersCaGiven_handshakeSucceeds() throws Exception {
    // Arrange
    int port = serve(participantA);
    EgressTrust trust = new EgressTrust(bundleOf(participantA));

    // Act
    int status = get(trust.sslContext(), port);

    // Assert
    assertThat(status).isEqualTo(200);
  }

  @Test
  void sslContext_bundleWithoutServersCaGiven_handshakeFails() throws Exception {
    // Arrange
    int port = serve(participantA);
    EgressTrust trust = new EgressTrust(bundleOf(participantB));

    // Act & Assert
    assertThatThrownBy(() -> get(trust.sslContext(), port))
        .isInstanceOf(SSLHandshakeException.class);
  }

  /**
   * Trusting the CA must not stop the hostname check: the forwarding trust manager hands the JDK's
   * own one the engine, which is what carries the endpoint-identification setting. The certificate
   * names localhost and 127.0.0.1 only, so the IPv6 loopback reaches the same server under a name
   * it does not cover.
   */
  @Test
  void sslContext_hostNotInCertificateGiven_handshakeFails() throws Exception {
    // Arrange
    int port = serve(participantA);
    EgressTrust trust = new EgressTrust(bundleOf(participantA));

    // Act & Assert
    assertThatThrownBy(() -> get(trust.sslContext(), "[::1]", port))
        .isInstanceOf(SSLHandshakeException.class);
  }

  @Test
  void sslContext_bundleOfTwoCasGiven_trustsBoth() throws Exception {
    // Arrange
    EgressTrust trust = new EgressTrust(bundleOf(participantA, participantB));
    int portA = serve(participantA);
    int statusA = get(trust.sslContext(), portA);
    stopServer();
    int portB = serve(participantB);

    // Act
    int statusB = get(trust.sslContext(), portB);

    // Assert
    assertThat(statusA).isEqualTo(200);
    assertThat(statusB).isEqualTo(200);
  }

  @Test
  void run_bundleRotatedToTheServersCa_newConnectionsTrustIt() throws Exception {
    // Arrange
    int port = serve(participantA);
    Path bundle = bundleOf(participantB);
    EgressTrust trust = new EgressTrust(bundle);
    assertThatThrownBy(() -> get(trust.sslContext(), port))
        .isInstanceOf(SSLHandshakeException.class);
    Files.copy(bundleOf(participantA, participantB), bundle, REPLACE_EXISTING);

    // Act
    trust.run();

    // Assert
    assertThat(get(trust.sslContext(), port)).isEqualTo(200);
  }

  @Test
  void run_caRemovedFromTheBundle_hostsAlreadyReachedAreRefusedToo() throws Exception {
    // Arrange — a session to A is cached by the first call. A resumed handshake never consults the
    // trust manager, so without invalidation the removed CA would keep vouching for A.
    int port = serve(participantA);
    Path bundle = bundleOf(participantA);
    EgressTrust trust = new EgressTrust(bundle);
    assertThat(get(trust.sslContext(), port)).isEqualTo(200);
    Files.copy(bundleOf(participantB), bundle, REPLACE_EXISTING);

    // Act
    trust.run();

    // Assert
    assertThatThrownBy(() -> get(trust.sslContext(), port))
        .isInstanceOf(SSLHandshakeException.class);
  }

  @Test
  void run_bundleReplacedByPrivateKey_keepsTheTrustLoadedBefore() throws Exception {
    // Arrange
    int port = serve(participantA);
    Path bundle = bundleOf(participantA);
    EgressTrust trust = new EgressTrust(bundle);
    Files.copy(participantA.privateKeyPath(), bundle, REPLACE_EXISTING);

    // Act
    trust.run();

    // Assert
    assertThat(get(trust.sslContext(), port)).isEqualTo(200);
  }

  @Test
  void constructor_privateKeyFileGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new EgressTrust(participantA.privateKeyPath()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.EGRESS_CA_CERT_PATH_KEY);
  }

  @Test
  void constructor_missingFileGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new EgressTrust(dir.resolve("absent.crt")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.EGRESS_CA_CERT_PATH_KEY);
  }

  @Test
  void trustManagerFor_jvmDefaultsAndBundleGiven_anchorsOnBoth() {
    // Arrange
    List<X509Certificate> jvmDefaults = EgressTrust.jvmDefaultIssuers();
    List<X509Certificate> bundle = List.of(participantA.certificate());

    // Act
    X509Certificate[] anchors =
        EgressTrust.trustManagerFor(jvmDefaults, bundle).getAcceptedIssuers();

    // Assert — added to the JVM's trust, never replacing it
    assertThat(jvmDefaults).isNotEmpty();
    assertThat(anchors).contains(participantA.certificate());
    assertThat(anchors).contains(jvmDefaults.toArray(new X509Certificate[0]));
  }

  /** Writes the given certificates, in order, into one PEM bundle file. */
  private Path bundleOf(TlsTestCerts.PemPair... pairs) throws IOException {
    StringBuilder pem = new StringBuilder();
    for (TlsTestCerts.PemPair pair : pairs) {
      pem.append(Files.readString(pair.certChainPath()));
    }
    Path bundle = Files.createTempFile(dir, "bundle", ".crt");
    Files.writeString(bundle, pem);
    return bundle;
  }

  /** Starts an HTTPS server presenting {@code pair} on every local address; returns the port. */
  private int serve(TlsTestCerts.PemPair pair) {
    server = TlsTestCerts.startHttpsServer(pair, "/", new byte[0]);
    return server.getAddress().getPort();
  }

  private static int get(SSLContext context, int port) throws IOException, InterruptedException {
    return get(context, "localhost", port);
  }

  /** One GET through a fresh client, so no pooled connection carries an earlier handshake. */
  private static int get(SSLContext context, String host, int port)
      throws IOException, InterruptedException {
    HttpClient client = HttpClient.newBuilder().sslContext(context).build();
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("https://" + host + ":" + port)).build();
    return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
  }
}
