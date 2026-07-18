package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.grpc.GrpcSagaAdminClient;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the <b>gRPC Admin API against a real, authenticated daemon</b>: a real
 * {@link GrpcSagaAdminClient} drives a live {@link SagaServer} configured with the {@code apikey}
 * security provider. This is the test the unit suites structurally cannot be — the admin RPCs run
 * through the actual {@code SagaSecurityInterceptor} wiring, so it proves the client can present a
 * credential and that RBAC is enforced over the wire. Two keys are configured: an {@code
 * saga:admin} key and a {@code saga:write}-only key.
 */
class AdminGrpcIntegrationTest extends DaemonIntegrationTestSupport {

  private static final String ADMIN_KEY = "admin-key-secret-value";
  private static final String WRITE_KEY = "write-key-secret-value";
  private static final String API_KEY_HEADER = "X-API-Key";

  // The apikey provider's property keys (ApiKeyConfig's constants are package-private to
  // .security).
  private static final String APIKEY_PREFIX = "scalar.db.saga.server.security.apikey.";
  private static final String HEADER_KEY = APIKEY_PREFIX + "header";
  private static final String KEY_PREFIX = APIKEY_PREFIX + "key.";

  private static final String SAGA_NAME = "saga";
  private static final String DEFINITION =
      withService(
          "{ \"name\": \"saga\", \"mode\": \"SAGA\", \"steps\": [\n"
              + "  { \"name\": \"s1\", \"service\": \"$svc\",\n"
              + "    \"execution\":    { \"method\": \"POST\", \"path\": \"/debit\" },\n"
              + "    \"compensation\": { \"method\": \"POST\", \"path\": \"/reverse\" } } ] }");

  private final List<GrpcSagaAdminClient> clients = new ArrayList<>();

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/debit", 200);
    route(participant, "/reverse", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
  }

  @Override
  protected void configureProperties(Properties props) {
    // Turn on real authentication: the apikey provider with an admin key and a write-only key. Each
    // key's secret must be a secret reference, so write it to a file and reference it.
    props.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "apikey");
    props.setProperty(HEADER_KEY, API_KEY_HEADER);
    configureKey(props, "admin", ADMIN_KEY, "saga:admin");
    configureKey(props, "writer", WRITE_KEY, "saga:write");
  }

  private static void configureKey(Properties props, String name, String secret, String roles) {
    props.setProperty(KEY_PREFIX + name + ".secret", fileSecretReference(secret));
    props.setProperty(KEY_PREFIX + name + ".roles", roles);
  }

  private static String fileSecretReference(String secret) {
    try {
      Path file = Files.createTempFile("saga-admin-it-key", ".secret");
      file.toFile().deleteOnExit();
      Files.write(file, secret.getBytes(StandardCharsets.UTF_8));
      return "${file:UTF-8:" + file + "}";
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @AfterEach
  void closeClients() {
    clients.forEach(GrpcSagaAdminClient::close);
  }

  /**
   * An admin client presenting {@code apiKey} in the configured header, or none when {@code null}.
   */
  private GrpcSagaAdminClient adminClient(@Nullable String apiKey) {
    GrpcSagaAdminClient.Builder builder =
        GrpcSagaAdminClient.newBuilder().target("localhost:" + grpcPort());
    if (apiKey != null) {
      builder.callCredentials(GrpcSagaAdminClient.staticHeaderCredentials(API_KEY_HEADER, apiKey));
    }
    GrpcSagaAdminClient client = builder.build();
    clients.add(client);
    return client;
  }

  @Test
  void adminKey_listSagas_succeeds() {
    // The saga:admin credential is authenticated and authorized; the real list round-trips.
    SagaPage<SagaStateSnapshot> page =
        adminClient(ADMIN_KEY).listSagas(SagaQuery.newBuilder().build());

    assertThat(page.getItems()).isEmpty(); // no sagas seeded, but the call succeeded end-to-end
    assertThat(page.getNextPageToken()).isNull();
  }

  @Test
  void adminKey_recoverUnknownSaga_throwsSagaNotFound() {
    // A real round-trip through the admin service and the server's error mapper back to the client:
    // auth passes, RBAC allows, the service runs, NOT_FOUND maps back to SagaNotFoundException.
    assertThatThrownBy(() -> adminClient(ADMIN_KEY).recoverSaga("does-not-exist", "why"))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void writeOnlyKey_adminMutation_isPermissionDenied() {
    // Authenticated but lacking saga:admin: the interceptor denies before the service runs.
    assertThatThrownBy(() -> adminClient(WRITE_KEY).recoverSaga("any", "why"))
        .isInstanceOf(SagaRuntimeException.class)
        .hasMessageContaining("PERMISSION_DENIED");
  }

  @Test
  void noCredential_adminRpc_isUnauthenticated() {
    // No credential presented: rejected before authorization.
    assertThatThrownBy(() -> adminClient(null).listSagas(SagaQuery.newBuilder().build()))
        .isInstanceOf(SagaRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }
}
