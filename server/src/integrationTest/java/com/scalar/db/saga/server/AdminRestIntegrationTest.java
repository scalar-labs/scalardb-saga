package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the <b>REST Admin API against a real, authenticated daemon</b> — the REST
 * counterpart to {@link AdminGrpcIntegrationTest}. A real HTTP client drives a live {@link
 * SagaServer} configured with the {@code apikey} provider, so the admin routes run through the
 * actual {@link com.scalar.db.saga.server.security.SagaSecurityHandler} RBAC wiring the mock-based
 * {@code SagaAdminResourceTest} structurally cannot reach.
 *
 * <p>Proves RBAC enforcement (admin key allowed, write-only key {@code 403}, no credential {@code
 * 401}), route reachability, and the daemon's admin status-code contract ({@code 200} list, {@code
 * 404} unknown saga, {@code 422} wrong-state precondition, {@code 400} bad request). Two keys are
 * configured by the shared fixture: a {@code saga:admin} key and a {@code saga:write}-only key.
 */
class AdminRestIntegrationTest extends ServerIntegrationTestSupport {

  private static final String SAGA_NAME = "saga";
  private static final String DEFINITION =
      withService(
          "{ \"name\": \"saga\", \"mode\": \"SAGA\", \"steps\": [\n"
              + "  { \"name\": \"s1\", \"service\": \"$svc\",\n"
              + "    \"execution\":    { \"method\": \"POST\", \"path\": \"/debit\" },\n"
              + "    \"compensation\": { \"method\": \"POST\", \"path\": \"/reverse\" } } ] }");

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
    enableApiKeyProvider(props);
  }

  /**
   * The {@value #API_KEY_HEADER} header carrying {@code apiKey}, or no header when {@code null}.
   */
  private static Map<String, String> auth(@Nullable String apiKey) {
    return apiKey == null ? Map.of() : Map.of(API_KEY_HEADER, apiKey);
  }

  @Test
  void adminKey_listSagas_returns200WithList() throws Exception {
    // The saga:admin credential authenticates and authorizes; the list envelope round-trips.
    HttpResponse<String> response = get("/sagas", auth(ADMIN_KEY));

    assertThat(response.statusCode()).isEqualTo(200);
    JsonNode body = MAPPER.readTree(response.body());
    assertThat(body.get("sagas").isArray()).isTrue();
    assertThat(body.get("sagas")).isEmpty(); // no sagas seeded, but the call succeeded end-to-end
  }

  @Test
  void adminKey_recoverUnknownSaga_returns404() throws Exception {
    // A real round-trip through the admin service and the error mapper: auth passes, RBAC allows,
    // the service runs, and a missing saga maps back to 404.
    HttpResponse<String> response =
        post("/sagas/does-not-exist/recover", "{\"reason\":\"why\"}", auth(ADMIN_KEY));

    assertThat(response.statusCode()).isEqualTo(404);
  }

  @Test
  void adminKey_resetCompletedSaga_returns422WrongState() throws Exception {
    // Seed a saga and let it complete (the admin role subsumes write), then reset it: reset targets
    // ESCALATED sagas, so a COMPLETED one is a wrong-state precondition failure (422), not a 409.
    String sagaId = startCompletedSaga();

    HttpResponse<String> response =
        post("/sagas/" + sagaId + "/reset", "{\"reason\":\"why\"}", auth(ADMIN_KEY));

    assertThat(response.statusCode()).isEqualTo(422);
    assertThat(MAPPER.readTree(response.body()).get("error").asText())
        .isEqualTo("SAGA_WRONG_STATE");
  }

  @Test
  void adminKey_mutationMissingReason_returns400() throws Exception {
    // The body's required 'reason' is validated at the edge before the saga is looked up.
    HttpResponse<String> response = post("/sagas/any/recover", "{}", auth(ADMIN_KEY));

    assertThat(response.statusCode()).isEqualTo(400);
  }

  @Test
  void writeOnlyKey_adminRoute_returns403() throws Exception {
    // Authenticated but lacking saga:admin: the security handler denies before the route runs.
    HttpResponse<String> response = get("/sagas", auth(WRITE_KEY));

    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void noCredential_adminRoute_returns401() throws Exception {
    // No credential presented: rejected before authorization.
    HttpResponse<String> response = get("/sagas", auth(null));

    assertThat(response.statusCode()).isEqualTo(401);
  }

  private String startCompletedSaga() throws Exception {
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}", auth(ADMIN_KEY));
    assertThat(start.statusCode()).isEqualTo(200);
    JsonNode body = MAPPER.readTree(start.body());
    assertThat(body.get("status").asText()).isEqualTo("COMPLETED");
    return body.get("sagaId").asText();
  }
}
