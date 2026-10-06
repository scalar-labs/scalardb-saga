package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage of {@code egress.ca_cert_path}: a saga whose participant serves HTTPS under a
 * private CA. The daemon boots trusting an unrelated CA, so the call fails the handshake; once the
 * bundle names the participant's CA and the reload pass runs, the same daemon reaches it, with no
 * restart. Each certificate is self-signed, so it is the CA a client must trust to reach it.
 */
class SagaEgressTrustIntegrationTest extends ServerIntegrationTestSupport {

  private static final String SAGA = "private-ca-saga";
  private static final String DEFINITION =
      withService(
          """
          { "name": "$name", "mode": "SAGA",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "call", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge" },
              "compensation": { "method": "POST", "path": "/charge-undo" } } ] }
          """
              .replace("$name", SAGA));

  @TempDir static Path materialDir;
  private static TlsTestCerts.PemPair participantCert;
  private static TlsTestCerts.PemPair unrelatedCert;

  @TempDir Path bundleDir;
  private @Nullable Path bundle;
  private @Nullable HttpsServer httpsParticipant;

  @BeforeAll
  static void generateMaterial() {
    participantCert = TlsTestCerts.generateRsa(materialDir, "participant");
    unrelatedCert = TlsTestCerts.generateRsa(materialDir, "unrelated");
  }

  @AfterEach
  void stopHttpsParticipant() {
    if (httpsParticipant != null) {
      httpsParticipant.stop(0);
    }
  }

  @Override
  protected void configureParticipant(HttpServer participant) {
    // The plain-HTTP participant the support starts goes unused; the saga calls the HTTPS one.
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA, DEFINITION);
  }

  @Override
  protected void configureServices(Map<String, Properties> services) {
    Properties account =
        Objects.requireNonNull(services.get(SERVICE), "the fixture did not register " + SERVICE);
    HttpsServer started;
    try {
      started = startHttpsParticipant(participantCert);
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException("could not start the HTTPS participant", e);
    }
    httpsParticipant = started;
    account.setProperty("base_url", "https://localhost:" + started.getAddress().getPort());
  }

  @Override
  protected void configureProperties(Properties props) throws IOException {
    Path file = bundleDir.resolve("ca.crt");
    Files.copy(unrelatedCert.certChainPath(), file);
    bundle = file;
    props.setProperty(SagaServerConfig.EGRESS_CA_CERT_PATH_KEY, file.toString());
  }

  @Test
  void startSync_bundleWithoutTheParticipantsCa_doesNotComplete() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + SAGA + "\"}");

    assertThat(status(post)).isNotEqualTo("COMPLETED");
  }

  @Test
  void startSync_bundleRotatedToTheParticipantsCaAndReloaded_completes() throws Exception {
    // Old and new CA in one file, as a rotation stages it.
    Files.writeString(
        Objects.requireNonNull(bundle, "configureProperties did not run"),
        Files.readString(unrelatedCert.certChainPath())
            + Files.readString(participantCert.certChainPath()));
    reloadEgressTrustNow();

    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + SAGA + "\"}");

    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPLETED");
  }

  private static HttpsServer startHttpsParticipant(TlsTestCerts.PemPair pair)
      throws IOException, GeneralSecurityException {
    TlsMaterial material = TlsMaterial.load(pair.certChainPath(), pair.privateKeyPath());
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    char[] password = new char[0];
    keyStore.setKeyEntry(
        "participant",
        material.privateKey(),
        password,
        material.certChain().toArray(new X509Certificate[0]));
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keyStore, password);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers.getKeyManagers(), null, null);
    HttpsServer server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
    server.setHttpsConfigurator(new HttpsConfigurator(context));
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(200, 2);
          exchange.getResponseBody().write("{}".getBytes(StandardCharsets.UTF_8));
          exchange.close();
        });
    server.start();
    return server;
  }
}
