package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the per-service outbound policy configured under {@code
 * scalar.db.saga.server.service.<name>.*} — that a header and an SSRF allowlist set in the
 * properties file actually reach the outbound request the engine makes.
 *
 * <p>The header case is the one that decides whether the daemon can call an <b>authenticated</b>
 * downstream service at all: daemon mode is declarative-only, so an operator cannot supply a code
 * step that sets the credential, and the definition file must not carry secrets. A silently dropped
 * header would mean unauthenticated calls to the participant, so it is asserted on the participant
 * side rather than on the parsed config.
 *
 * <p>Counterpart: {@link SagaServiceStepIntegrationTest} covers the outbound transport itself.
 */
class SagaServicePolicyIntegrationTest extends ServerIntegrationTestSupport {

  private static final String AUTHENTICATED_SAGA = "authenticated-service-saga";
  private static final String BLOCKED_SAGA = "blocked-service-saga";
  // A second service pointed at the same participant, but with an allowlist that excludes its host.
  private static final String BLOCKED_SERVICE = "blocked";
  private static final String TOKEN = "Bearer downstream-token";

  private final AtomicReference<String> observedAuthorization = new AtomicReference<>();
  private final AtomicReference<String> observedTenant = new AtomicReference<>();

  private static final String AUTHENTICATED_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "steps": [
            { "name": "call", "service": "$svc",
              "execution":    { "method": "POST", "path": "/authenticated" },
              "compensation": { "method": "POST", "path": "/authenticated-undo" } } ] }
          """
              .replace("$name", AUTHENTICATED_SAGA));

  private static final String BLOCKED_DEF =
      """
      { "name": "$name", "mode": "SAGA",
        "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
        { "name": "call", "service": "$svc",
          "execution":    { "method": "POST", "path": "/blocked" },
          "compensation": { "method": "POST", "path": "/blocked-undo" } } ] }
      """
          .replace("$name", BLOCKED_SAGA)
          .replace("$svc", BLOCKED_SERVICE);

  @Override
  protected void configureParticipant(HttpServer participant) {
    participant.createContext(
        "/authenticated",
        exchange -> {
          observedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          observedTenant.set(exchange.getRequestHeaders().getFirst("X-Tenant"));
          respond(exchange, 200, "{}");
        });
    route(participant, "/authenticated-undo", 200);
    route(participant, "/blocked", 200);
    route(participant, "/blocked-undo", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, AUTHENTICATED_SAGA, AUTHENTICATED_DEF);
    writeDefinition(definitionsDir, BLOCKED_SAGA, BLOCKED_DEF);
  }

  @Override
  protected void configureProperties(Properties props) {
    props.setProperty(serviceKey(SERVICE, ".header.Authorization"), TOKEN);
    props.setProperty(serviceKey(SERVICE, ".header.X-Tenant"), "acme");
    props.setProperty(serviceKey(SERVICE, ".allowed_hosts"), "localhost");
    // The same participant, reached through a service whose allowlist does not name its host.
    props.setProperty(
        serviceKey(BLOCKED_SERVICE, ".base_url"),
        props.getProperty(serviceKey(SERVICE, ".base_url")));
    props.setProperty(serviceKey(BLOCKED_SERVICE, ".allowed_hosts"), "some-other-service");
  }

  @Test
  void startSync_serviceWithConfiguredHeaders_sendsThemToParticipant() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + AUTHENTICATED_SAGA + "\"}");

    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPLETED");
    assertThat(observedAuthorization.get()).isEqualTo(TOKEN);
    assertThat(observedTenant.get()).isEqualTo("acme");
  }

  @Test
  void startSync_serviceHostNotAllowed_neverCallsParticipant() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + BLOCKED_SAGA + "\"}");

    // The allowlist is enforced before the request leaves, so the saga cannot complete and the
    // participant is never reached — the evidence that the configured hosts are actually applied.
    assertThat(status(post)).isNotEqualTo("COMPLETED");
    assertThat(hits("/blocked")).isZero();
  }

  private static String serviceKey(String name, String attribute) {
    return SagaServerConfig.SERVICE_KEY_PREFIX + name + attribute;
  }
}
