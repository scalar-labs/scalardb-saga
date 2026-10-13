package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
    HttpsServer started =
        TlsTestCerts.startHttpsServer(participantCert, "/", "{}".getBytes(StandardCharsets.UTF_8));
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
    // Arrange — the daemon booted trusting only the unrelated CA (configureProperties)

    // Act
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + SAGA + "\"}");

    // Assert
    assertThat(status(post)).isNotEqualTo("COMPLETED");
  }

  @Test
  void startSync_bundleRotatedToTheParticipantsCaAndReloaded_completes() throws Exception {
    // Arrange — old and new CA in one file, as a rotation stages it, then one reload pass
    Files.writeString(
        Objects.requireNonNull(bundle, "configureProperties did not run"),
        Files.readString(unrelatedCert.certChainPath())
            + Files.readString(participantCert.certChainPath()));
    reloadEgressTrustNow();

    // Act
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + SAGA + "\"}");

    // Assert
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPLETED");
  }
}
