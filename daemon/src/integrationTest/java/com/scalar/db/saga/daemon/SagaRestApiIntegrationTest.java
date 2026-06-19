package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the daemon's <b>client-facing REST contract</b> — how the {@link
 * SagaServer} responds to HTTP callers: synchronous vs {@code ?async} start, client-supplied IDs
 * via {@code PUT} ({@code 409} + the existing snapshot on conflict), status {@code GET} ({@code
 * 404} when unknown), request validation ({@code 400} for a missing {@code sagaName}, a malformed
 * body, or an unrecognized {@code ?async} value), and the synchronous outcome status codes ({@code
 * 200} for a terminal state, {@code 202} while still resolving). The sagas run trivial in-process
 * {@code NoopStep}/{@code FailingStep} code steps — they are inert vehicles so a saga can reach a
 * known state; this test asserts nothing about how a step does its work.
 *
 * <p>Counterpart: {@link SagaServiceStepIntegrationTest} covers the <b>outbound</b> side — a
 * declarative {@code service} step actually calling a participant over HTTP. This class is purely
 * the inbound REST/HTTP behavior and never makes an outbound call.
 */
class SagaRestApiIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SAGA_NAME = "noop-saga";
  private static final String DEFINITION =
      "{\"name\":\""
          + SAGA_NAME
          + "\",\"steps\":[{\"name\":\"s1\",\"stepClass\":\"com.scalar.db.saga.daemon.NoopStep\"}]}";

  // Backward-recovery saga: s1 (NoopStep) succeeds, s2 (FailingStep) fails → s1 compensates
  // cleanly.
  private static final String COMPENSATING_SAGA = "compensating-saga";
  private static final String COMPENSATING_DEF =
      "{\"name\":\""
          + COMPENSATING_SAGA
          + "\",\"mode\":\"SAGA\",\"recoveryStrategy\":\"BACKWARD\","
          + "\"defaultRetryPolicy\":{\"maxAttempts\":1,\"initialIntervalMillis\":1},"
          + "\"steps\":[{\"name\":\"s1\",\"stepClass\":\"com.scalar.db.saga.daemon.NoopStep\"},"
          + "{\"name\":\"s2\",\"stepClass\":\"com.scalar.db.saga.daemon.FailingStep\"}]}";

  // Like above, but s1 (FailingCompensationStep) fails to compensate → saga stuck COMPENSATING.
  private static final String COMPENSATION_FAILING_SAGA = "compensation-failing-saga";
  private static final String COMPENSATION_FAILING_DEF =
      "{\"name\":\""
          + COMPENSATION_FAILING_SAGA
          + "\",\"mode\":\"SAGA\",\"recoveryStrategy\":\"BACKWARD\","
          + "\"defaultRetryPolicy\":{\"maxAttempts\":1,\"initialIntervalMillis\":1},"
          + "\"steps\":[{\"name\":\"s1\",\"stepClass\":"
          + "\"com.scalar.db.saga.daemon.FailingCompensationStep\"},"
          + "{\"name\":\"s2\",\"stepClass\":\"com.scalar.db.saga.daemon.FailingStep\"}]}";

  private final HttpClient http = HttpClient.newHttpClient();

  private Path tempDbPath;
  private Path definitionsDir;
  private SagaServer server;

  @BeforeEach
  void setUp() throws Exception {
    tempDbPath = Files.createTempFile("saga-daemon-test-", ".db");
    definitionsDir = Files.createTempDirectory("saga-daemon-defs-");
    Files.writeString(definitionsDir.resolve(SAGA_NAME + ".json"), DEFINITION);
    Files.writeString(definitionsDir.resolve(COMPENSATING_SAGA + ".json"), COMPENSATING_DEF);
    Files.writeString(
        definitionsDir.resolve(COMPENSATION_FAILING_SAGA + ".json"), COMPENSATION_FAILING_DEF);

    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    props.setProperty(SagaServerConfig.PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, definitionsDir.toString());

    server = new SagaServer(SagaServerConfig.load(props)).start();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    Files.deleteIfExists(definitionsDir.resolve(SAGA_NAME + ".json"));
    Files.deleteIfExists(definitionsDir.resolve(COMPENSATING_SAGA + ".json"));
    Files.deleteIfExists(definitionsDir.resolve(COMPENSATION_FAILING_SAGA + ".json"));
    Files.deleteIfExists(definitionsDir);
    Files.deleteIfExists(tempDbPath);
  }

  @Test
  void startSync_completesAndIsQueryable() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");

    assertThat(post.statusCode()).isEqualTo(200);
    JsonNode body = MAPPER.readTree(post.body());
    assertThat(body.get("status").asText()).isEqualTo("COMPLETED");
    String sagaId = body.get("sagaId").asText();
    assertThat(sagaId).isNotBlank();

    HttpResponse<String> get = get("/sagas/" + sagaId);
    assertThat(get.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(get.body()).get("status").asText()).isEqualTo("COMPLETED");
  }

  @Test
  void startAsync_returns202AndEventuallyCompletes() throws Exception {
    HttpResponse<String> post =
        post("/sagas?async=true", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");

    assertThat(post.statusCode()).isEqualTo(202);
    String sagaId = MAPPER.readTree(post.body()).get("sagaId").asText();

    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
  }

  @Test
  void startWithClientSuppliedIdTwice_returns409WithExisting() throws Exception {
    String body = "{\"sagaName\":\"" + SAGA_NAME + "\"}";

    HttpResponse<String> first = put("/sagas/order-1", body);
    assertThat(first.statusCode()).isEqualTo(200);

    HttpResponse<String> second = put("/sagas/order-1", body);
    assertThat(second.statusCode()).isEqualTo(409);
    JsonNode conflict = MAPPER.readTree(second.body());
    assertThat(conflict.get("error").asText()).isEqualTo("SAGA_ALREADY_EXISTS");
    assertThat(conflict.get("sagaId").asText()).isEqualTo("order-1");
    assertThat(conflict.get("existing").get("sagaId").asText()).isEqualTo("order-1");
  }

  @Test
  void getUnknownSaga_returns404() throws Exception {
    HttpResponse<String> get = get("/sagas/does-not-exist");
    assertThat(get.statusCode()).isEqualTo(404);
    assertThat(MAPPER.readTree(get.body()).get("error").asText()).isEqualTo("SAGA_NOT_FOUND");
  }

  @Test
  void postWithoutSagaName_returns400() throws Exception {
    HttpResponse<String> post = post("/sagas", "{}");
    assertThat(post.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(post.body()).get("error").asText()).isEqualTo("BAD_REQUEST");
  }

  @Test
  void postWithMalformedBody_returns400() throws Exception {
    HttpResponse<String> post = post("/sagas", "not-json");
    assertThat(post.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(post.body()).get("error").asText()).isEqualTo("BAD_REQUEST");
  }

  @Test
  void postWithUnrecognizedAsyncValue_returns400() throws Exception {
    HttpResponse<String> post =
        post("/sagas?async=1", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    assertThat(post.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(post.body()).get("error").asText()).isEqualTo("BAD_REQUEST");
  }

  @Test
  void startSync_businessFailure_returns200WithCompensated() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPENSATING_SAGA + "\"}");

    // The saga ran to a terminal state (cleanly rolled back) → 200, with the outcome in the body.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(post.body()).get("status").asText()).isEqualTo("COMPENSATED");
  }

  @Test
  void startSync_compensationFailure_returns202WithCompensating() throws Exception {
    HttpResponse<String> post =
        post("/sagas", "{\"sagaName\":\"" + COMPENSATION_FAILING_SAGA + "\"}");

    // Compensation itself failed → saga is non-terminal (still resolving) → 202.
    assertThat(post.statusCode()).isEqualTo(202);
    assertThat(MAPPER.readTree(post.body()).get("status").asText()).isEqualTo("COMPENSATING");
  }

  private String pollUntilTerminal(String sagaId) throws Exception {
    for (int i = 0; i < 50; i++) {
      JsonNode body = MAPPER.readTree(get("/sagas/" + sagaId).body());
      String status = body.get("status").asText();
      if (!status.equals("RUNNING") && !status.equals("COMPENSATING")) {
        return status;
      }
      Thread.sleep(40);
    }
    throw new AssertionError("Saga " + sagaId + " did not reach a terminal status in time");
  }

  private HttpResponse<String> post(String path, String body) throws Exception {
    return http.send(
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(body))
            .build(),
        BodyHandlers.ofString());
  }

  private HttpResponse<String> put(String path, String body) throws Exception {
    return http.send(
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .PUT(BodyPublishers.ofString(body))
            .build(),
        BodyHandlers.ofString());
  }

  private HttpResponse<String> get(String path) throws Exception {
    return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), BodyHandlers.ofString());
  }

  private URI uri(String path) {
    return URI.create("http://localhost:" + server.port() + path);
  }
}
