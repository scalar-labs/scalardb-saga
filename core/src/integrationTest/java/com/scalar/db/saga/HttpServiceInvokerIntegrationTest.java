package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.ServiceInvokerFactory;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.invoker.HttpServiceInvoker;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration of the {@link HttpServiceInvoker}: sagas whose steps are {@code
 * ServiceStep}s dispatched through the invoker against a real in-process HTTP participant, driven
 * by the engine over a SQLite-backed store. Covers the SAGA happy path with output flowing between
 * steps, engine retry on a retryable HTTP failure, compensation of a completed service step when a
 * later step fails, and the TCC lifecycle (reserve→confirm on success; reserve→cancel when a later
 * reserve fails).
 */
class HttpServiceInvokerIntegrationTest {

  private Path tempDbPath;
  private Properties props;
  private HttpServer server;
  private String baseUrl;

  /** Bodies received by the {@code /notify} and {@code /reserve/cancel} endpoints. */
  private final CopyOnWriteArrayList<String> notified = new CopyOnWriteArrayList<>();

  private final CopyOnWriteArrayList<String> cancelled = new CopyOnWriteArrayList<>();
  private final AtomicInteger flakyCalls = new AtomicInteger();

  /** TCC-phase calls received by the {@code /tcc/*} endpoints. */
  private final CopyOnWriteArrayList<String> reserved = new CopyOnWriteArrayList<>();

  private final CopyOnWriteArrayList<String> confirmed = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> tccCancelled = new CopyOnWriteArrayList<>();

  @BeforeEach
  void setUp() throws Exception {
    tempDbPath = Files.createTempFile("saga-svc-test-", ".db");
    props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/users/u-1", ex -> respond(ex, 200, "{\"name\":\"alice\"}"));
    server.createContext(
        "/notify",
        ex -> {
          notified.add(readBody(ex));
          respond(ex, 200, "{}");
        });
    server.createContext(
        "/flaky",
        ex -> {
          if (flakyCalls.incrementAndGet() == 1) {
            respond(ex, 503, "{}"); // retryable
          } else {
            respond(ex, 200, "{\"ok\":true}");
          }
        });
    server.createContext("/reserve", ex -> respond(ex, 200, "{}"));
    server.createContext(
        "/reserve/cancel",
        ex -> {
          cancelled.add(readBody(ex));
          respond(ex, 200, "{}");
        });
    server.createContext("/charge", ex -> respond(ex, 422, "{}")); // non-retryable
    server.createContext(
        "/tcc/reserve",
        ex -> {
          reserved.add(readBody(ex));
          respond(ex, 200, "{}");
        });
    server.createContext(
        "/tcc/confirm",
        ex -> {
          confirmed.add(readBody(ex));
          respond(ex, 200, "{}");
        });
    server.createContext(
        "/tcc/cancel",
        ex -> {
          tccCancelled.add(readBody(ex));
          respond(ex, 200, "{}");
        });
    server.createContext("/tcc/reserve-fail", ex -> respond(ex, 422, "{}")); // non-retryable
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.stop(0);
    Files.deleteIfExists(tempDbPath);
  }

  @Test
  void start_serviceStepsSucceed_outputFlowsToNextStep() {
    // Arrange — fetch the user, then notify using the fetched name
    ServiceInvokerFactory invokerFactory =
        () ->
            HttpServiceInvoker.newBuilder(baseUrl)
                .operation("getUser")
                .execution(
                    (http, ctx) ->
                        StepResult.of(
                            "userName",
                            Objects.requireNonNull(
                                http.get("/users/u-1").send().jsonObject().get("name"))))
                .compensation((http, ctx) -> {}) // read-only: undo is a no-op
                .add()
                .operation("notify")
                .execution(
                    (http, ctx) -> {
                      http.post("/notify")
                          .jsonBody(Map.of("user", ctx.get("userName", String.class).orElseThrow()))
                          .send();
                      return StepResult.empty();
                    })
                .compensation((http, ctx) -> {})
                .add()
                .build();
    SagaDefinition def =
        SagaDefinition.newBuilder("user-notification-saga", SagaMode.SAGA)
            .serviceStep("fetch", "user-service", "getUser")
            .add()
            .serviceStep("notify", "user-service", "notify")
            .add()
            .build();

    try (SagaManager manager = buildManager("user-service", invokerFactory)) {
      manager.register(def);

      // Act
      String sagaId = manager.start("user-notification-saga", Map.of());

      // Assert — saga completed and step 1's output reached step 2's request
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(notified).hasSize(1);
      assertThat(notified.get(0)).contains("alice");
    }
  }

  @Test
  void start_serviceStepReturnsRetryableStatus_retriedThenCompletes() {
    // Arrange — the participant fails once (503) then succeeds
    ServiceInvokerFactory invokerFactory =
        () ->
            HttpServiceInvoker.newBuilder(baseUrl)
                .operation("flakyGet")
                .execution((http, ctx) -> StepResult.of(http.get("/flaky").send().jsonObject()))
                .compensation((http, ctx) -> {}) // read-only: undo is a no-op
                .add()
                .build();
    SagaDefinition def =
        SagaDefinition.newBuilder("flaky-saga", SagaMode.SAGA)
            .serviceStep("flaky", "svc", "flakyGet")
            .retryPolicy(
                RetryPolicy.newBuilder()
                    .maxAttempts(3)
                    .initialIntervalMillis(1)
                    .maxIntervalMillis(1)
                    .build())
            .add()
            .build();

    try (SagaManager manager = buildManager("svc", invokerFactory)) {
      manager.register(def);

      // Act
      String sagaId = manager.start("flaky-saga", Map.of());

      // Assert — engine retried the retryable 503, second attempt succeeded
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(flakyCalls.get()).isEqualTo(2);
    }
  }

  @Test
  void start_laterServiceStepFailsNonRetryable_priorServiceStepCompensated() {
    // Arrange — reserve succeeds, charge fails non-retryably (422); reserve must be compensated
    ServiceInvokerFactory invokerFactory =
        () ->
            HttpServiceInvoker.newBuilder(baseUrl)
                .operation("reserve")
                .execution(
                    (http, ctx) -> {
                      http.post("/reserve").jsonBody(Map.of()).send();
                      return StepResult.empty();
                    })
                .compensation((http, ctx) -> http.post("/reserve/cancel").jsonBody(Map.of()).send())
                .add()
                .operation("charge")
                .execution(
                    (http, ctx) -> {
                      http.post("/charge").jsonBody(Map.of()).send();
                      return StepResult.empty();
                    })
                .compensation((http, ctx) -> {}) // charge fails before committing: no undo
                .add()
                .build();
    SagaDefinition def =
        SagaDefinition.newBuilder("payment-saga", SagaMode.SAGA)
            .serviceStep("reserve", "svc", "reserve")
            .add()
            .serviceStep("charge", "svc", "charge")
            .add()
            .build();

    try (SagaManager manager = buildManager("svc", invokerFactory)) {
      manager.register(def);

      // Act
      String sagaId = manager.start("payment-saga", Map.of());

      // Assert — saga compensated and the prior service step's compensation ran
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(cancelled).hasSize(1);
    }
  }

  @Test
  void start_tccServiceStepsAllReserve_confirmsAndCompletes() {
    // Arrange — two TCC service steps; both reserve, then both confirm
    ServiceInvokerFactory invokerFactory =
        () ->
            HttpServiceInvoker.newBuilder(baseUrl)
                .tccOperation("reserveStock")
                .reservation(
                    (http, ctx) -> {
                      http.post("/tcc/reserve").jsonBody(Map.of("op", "reserveStock")).send();
                      return StepResult.empty();
                    })
                .confirmation(
                    (http, ctx) ->
                        http.post("/tcc/confirm").jsonBody(Map.of("op", "reserveStock")).send())
                .cancellation(
                    (http, ctx) ->
                        http.post("/tcc/cancel").jsonBody(Map.of("op", "reserveStock")).send())
                .add()
                .tccOperation("reserveCredit")
                .reservation(
                    (http, ctx) -> {
                      http.post("/tcc/reserve").jsonBody(Map.of("op", "reserveCredit")).send();
                      return StepResult.empty();
                    })
                .confirmation(
                    (http, ctx) ->
                        http.post("/tcc/confirm").jsonBody(Map.of("op", "reserveCredit")).send())
                .cancellation(
                    (http, ctx) ->
                        http.post("/tcc/cancel").jsonBody(Map.of("op", "reserveCredit")).send())
                .add()
                .build();
    SagaDefinition def =
        SagaDefinition.newBuilder("tcc-order-saga", SagaMode.TCC)
            .serviceStep("reserveStock", "order-service", "reserveStock")
            .add()
            .serviceStep("reserveCredit", "order-service", "reserveCredit")
            .add()
            .build();

    try (SagaManager manager = buildManager("order-service", invokerFactory)) {
      manager.register(def);

      // Act
      String sagaId = manager.start("tcc-order-saga", Map.of());

      // Assert — all reserves succeeded, so both steps confirmed and the saga completed
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(reserved).hasSize(2);
      assertThat(confirmed).hasSize(2);
      assertThat(tccCancelled).isEmpty();
    }
  }

  @Test
  void start_tccLaterReserveFails_priorReserveCancelled() {
    // Arrange — first step reserves; second step's reserve fails non-retryably (422)
    ServiceInvokerFactory invokerFactory =
        () ->
            HttpServiceInvoker.newBuilder(baseUrl)
                .tccOperation("reserveStock")
                .reservation(
                    (http, ctx) -> {
                      http.post("/tcc/reserve").jsonBody(Map.of("op", "reserveStock")).send();
                      return StepResult.empty();
                    })
                .confirmation(
                    (http, ctx) ->
                        http.post("/tcc/confirm").jsonBody(Map.of("op", "reserveStock")).send())
                .cancellation(
                    (http, ctx) ->
                        http.post("/tcc/cancel").jsonBody(Map.of("op", "reserveStock")).send())
                .add()
                .tccOperation("reserveCredit")
                .reservation(
                    (http, ctx) -> {
                      http.post("/tcc/reserve-fail").jsonBody(Map.of("op", "reserveCredit")).send();
                      return StepResult.empty();
                    })
                .confirmation(
                    (http, ctx) ->
                        http.post("/tcc/confirm").jsonBody(Map.of("op", "reserveCredit")).send())
                .cancellation(
                    (http, ctx) ->
                        http.post("/tcc/cancel").jsonBody(Map.of("op", "reserveCredit")).send())
                .add()
                .build();
    SagaDefinition def =
        SagaDefinition.newBuilder("tcc-order-saga", SagaMode.TCC)
            .serviceStep("reserveStock", "order-service", "reserveStock")
            .add()
            .serviceStep("reserveCredit", "order-service", "reserveCredit")
            .add()
            .build();

    try (SagaManager manager = buildManager("order-service", invokerFactory)) {
      manager.register(def);

      // Act
      String sagaId = manager.start("tcc-order-saga", Map.of());

      // Assert — the second reserve failed, so the first reserve was cancelled and nothing
      // confirmed
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(reserved).hasSize(1);
      assertThat(tccCancelled).hasSize(1);
      assertThat(confirmed).isEmpty();
    }
  }

  private SagaManager buildManager(String serviceName, ServiceInvokerFactory invokerFactory) {
    return SagaManager.newBuilder()
        .storeFactory(ScalarDbSagaStoreFactory.create(props))
        .serviceInvokerFactory(serviceName, invokerFactory)
        .build();
  }

  private static String readBody(HttpExchange exchange) throws IOException {
    return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
