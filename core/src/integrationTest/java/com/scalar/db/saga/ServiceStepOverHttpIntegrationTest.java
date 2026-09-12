package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.HttpCall;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.transport.HttpServiceConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end conformance of declarative steps (Layer 2b) against the participant HTTP protocol:
 * sagas whose steps are declarative {@code ServiceStep}s, registered via {@code httpEndpoint(...)},
 * driven by the engine over a SQLite-backed store against a real in-process participant. Covers the
 * SAGA happy path (a {@code GET} read step with path/query templating, output extraction, and
 * output flowing into a later step's request), correlation-header propagation, engine retry on a
 * retryable {@code 503}, compensation of a completed step when a later step fails non-retryably
 * ({@code 422}), the TCC lifecycle (reserve→confirm; reserve→cancel when a later reserve fails),
 * and configuration hot reload (an already-built plan re-resolves a swapped endpoint through the
 * orchestrator's registrar).
 */
class ServiceStepOverHttpIntegrationTest {

  private Path tempDbPath;
  private Properties props;
  private HttpServer server;
  private String baseUrl;

  private final AtomicReference<String> sagaIdHeader = new AtomicReference<>();
  private final AtomicReference<String> sagaStepHeader = new AtomicReference<>();
  private final CopyOnWriteArrayList<String> notified = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> cancelled = new CopyOnWriteArrayList<>();
  private final AtomicInteger flakyCalls = new AtomicInteger();
  private final AtomicReference<String> xmlContentType = new AtomicReference<>();
  private final AtomicReference<String> xmlBody = new AtomicReference<>();
  private final AtomicReference<String> xmlAuthHeader = new AtomicReference<>();
  private final CopyOnWriteArrayList<String> reserved = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> confirmed = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> tccCancelled = new CopyOnWriteArrayList<>();

  @BeforeEach
  void setUp() throws Exception {
    tempDbPath = Files.createTempFile("saga-declarative-test-", ".db");
    props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/users/u-1",
        ex -> {
          sagaIdHeader.set(ex.getRequestHeaders().getFirst("X-Saga-Id"));
          sagaStepHeader.set(ex.getRequestHeaders().getFirst("X-Saga-Step"));
          respond(ex, 200, "{\"name\":\"alice\"}");
        });
    server.createContext(
        "/notify",
        ex -> {
          notified.add(readBody(ex));
          respond(ex, 200, "{}");
        });
    server.createContext("/noop", ex -> respond(ex, 200, "{}"));
    server.createContext(
        "/xml-notify",
        ex -> {
          xmlContentType.set(ex.getRequestHeaders().getFirst("Content-Type"));
          xmlAuthHeader.set(ex.getRequestHeaders().getFirst("Authorization"));
          xmlBody.set(readBody(ex));
          respondText(ex, 200, "ACK-OK"); // non-JSON response body for $body capture
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
  void start_declarativeStepsSucceed_outputFlowsAndHeadersPropagate() {
    // Arrange — a GET read step (path templated, output extracted), then a POST using that output
    SagaDefinition def =
        SagaDefinition.newBuilder("user-notification-saga")
            .saga()
            .serviceStep("fetch", "user-service")
            .execution(
                HttpCall.newBuilder("/users/${userId}")
                    .method(HttpMethod.GET)
                    .output(Map.of("userName", "$.name"))
                    .build())
            .compensation(HttpCall.newBuilder("/noop").build())
            .add()
            .serviceStep("notify", "user-service")
            .execution(
                HttpCall.newBuilder("/notify").jsonBody(Map.of("user", "${userName}")).build())
            .compensation(HttpCall.newBuilder("/noop").build())
            .add()
            .build();

    try (DefaultSagaOrchestrator orchestrator = buildOrchestrator("user-service")) {
      orchestrator.register(def);

      // Act
      String sagaId = orchestrator.start("user-notification-saga", Map.of("userId", "u-1"));

      // Assert — saga completed; output flowed; correlation headers reached the participant
      assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(notified).hasSize(1);
      assertThat(notified.get(0)).contains("alice");
      assertThat(sagaIdHeader.get()).isEqualTo(sagaId);
      assertThat(sagaStepHeader.get()).isEqualTo("fetch");
    }
  }

  @Test
  void start_declarativeStepReturnsRetryableStatus_retriedThenCompletes() {
    // Arrange — the participant fails once (503) then succeeds
    SagaDefinition def =
        SagaDefinition.newBuilder("flaky-saga")
            .saga()
            .serviceStep("flaky", "svc")
            .execution(HttpCall.newBuilder("/flaky").output(Map.of("ok", "$.ok")).build())
            .compensation(HttpCall.newBuilder("/noop").build())
            .retryPolicy(
                RetryPolicy.newBuilder()
                    .maxAttempts(3)
                    .initialIntervalMillis(1)
                    .maxIntervalMillis(1)
                    .build())
            .add()
            .build();

    try (DefaultSagaOrchestrator orchestrator = buildOrchestrator("svc")) {
      orchestrator.register(def);

      // Act
      String sagaId = orchestrator.start("flaky-saga", Map.of());

      // Assert — engine retried the retryable 503; the second attempt succeeded
      assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(flakyCalls.get()).isEqualTo(2);
    }
  }

  @Test
  void start_laterDeclarativeStepFailsNonRetryable_priorStepCompensated() {
    // Arrange — reserve succeeds, charge fails non-retryably (422); reserve must be compensated
    SagaDefinition def =
        SagaDefinition.newBuilder("payment-saga")
            .saga()
            .serviceStep("reserve", "svc")
            .execution(HttpCall.newBuilder("/reserve").build())
            .compensation(HttpCall.newBuilder("/reserve/cancel").build())
            .add()
            .serviceStep("charge", "svc")
            .execution(HttpCall.newBuilder("/charge").build())
            .compensation(HttpCall.newBuilder("/noop").build())
            .add()
            .build();

    try (DefaultSagaOrchestrator orchestrator = buildOrchestrator("svc")) {
      orchestrator.register(def);

      // Act
      String sagaId = orchestrator.start("payment-saga", Map.of());

      // Assert — saga compensated and the prior step's compensation ran
      assertThat(orchestrator.getStateSnapshot(sagaId).getStatus())
          .isEqualTo(SagaStatus.COMPENSATED);
      assertThat(cancelled).hasSize(1);
    }
  }

  @Test
  void start_tccDeclarativeStepsAllReserve_confirmsAndCompletes() {
    // Arrange — two TCC declarative steps; both reserve, then both confirm
    SagaDefinition def =
        SagaDefinition.newBuilder("tcc-order-saga")
            .tcc()
            .serviceStep("reserveStock", "order-service")
            .reservation(tcc("/tcc/reserve", "reserveStock"))
            .confirmation(tcc("/tcc/confirm", "reserveStock"))
            .cancellation(tcc("/tcc/cancel", "reserveStock"))
            .add()
            .serviceStep("reserveCredit", "order-service")
            .reservation(tcc("/tcc/reserve", "reserveCredit"))
            .confirmation(tcc("/tcc/confirm", "reserveCredit"))
            .cancellation(tcc("/tcc/cancel", "reserveCredit"))
            .add()
            .build();

    try (DefaultSagaOrchestrator orchestrator = buildOrchestrator("order-service")) {
      orchestrator.register(def);

      // Act
      String sagaId = orchestrator.start("tcc-order-saga", Map.of());

      // Assert — all reserves succeeded, so both steps confirmed
      assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(reserved).hasSize(2);
      assertThat(confirmed).hasSize(2);
      assertThat(tccCancelled).isEmpty();
    }
  }

  @Test
  void start_tccDeclarativeLaterReserveFails_bothReservesCancelled() {
    // Arrange — first step reserves; second step's reserve fails non-retryably (422)
    SagaDefinition def =
        SagaDefinition.newBuilder("tcc-order-saga")
            .tcc()
            .serviceStep("reserveStock", "order-service")
            .reservation(tcc("/tcc/reserve", "reserveStock"))
            .confirmation(tcc("/tcc/confirm", "reserveStock"))
            .cancellation(tcc("/tcc/cancel", "reserveStock"))
            .add()
            .serviceStep("reserveCredit", "order-service")
            .reservation(tcc("/tcc/reserve-fail", "reserveCredit"))
            .confirmation(tcc("/tcc/confirm", "reserveCredit"))
            .cancellation(tcc("/tcc/cancel", "reserveCredit"))
            .add()
            .build();

    try (DefaultSagaOrchestrator orchestrator = buildOrchestrator("order-service")) {
      orchestrator.register(def);

      // Act
      String sagaId = orchestrator.start("tcc-order-saga", Map.of());

      // Assert — the second reserve returned 422 (it may have committed the reservation), so BOTH
      // reservations are cancelled (cancel from the failed step, not just the prior one)
      // and nothing is confirmed.
      assertThat(orchestrator.getStateSnapshot(sagaId).getStatus())
          .isEqualTo(SagaStatus.COMPENSATED);
      assertThat(reserved).hasSize(1);
      assertThat(tccCancelled).hasSize(2);
      assertThat(confirmed).isEmpty();
    }
  }

  @Test
  void start_stringBodyWithContentTypeAndBodyCaptureAndDefaultHeader_completes() {
    // Arrange — a step with a raw templated XML body, a content-type override, a $body raw capture,
    // and an endpoint default header (auth) flowing through. The captured raw body feeds a second
    // step's JSON request.
    SagaDefinition def =
        SagaDefinition.newBuilder("xml-saga")
            .saga()
            .serviceStep("send", "xml-service")
            .execution(
                HttpCall.newBuilder("/xml-notify")
                    .method(HttpMethod.POST)
                    .stringBody("<msg>${text}</msg>")
                    .contentType("application/xml")
                    .output(Map.of("ack", HttpCall.BODY_OUTPUT))
                    .build())
            .compensation(HttpCall.newBuilder("/noop").build())
            .add()
            .serviceStep("record", "xml-service")
            .execution(HttpCall.newBuilder("/notify").jsonBody(Map.of("ack", "${ack}")).build())
            .compensation(HttpCall.newBuilder("/noop").build())
            .add()
            .build();

    try (DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(ScalarDbSagaStoreFactory.create(props))
            .httpEndpoint("xml-service", baseUrl)
            .defaultHeader("Authorization", "Bearer secret")
            .add()
            .build()) {
      orchestrator.register(def);

      // Act
      String sagaId = orchestrator.start("xml-saga", Map.of("text", "hello"));

      // Assert — the override content type, templated raw body, default auth header, and raw $body
      // capture all worked end-to-end.
      assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(xmlContentType.get()).isEqualTo("application/xml");
      assertThat(xmlBody.get()).isEqualTo("<msg>hello</msg>");
      assertThat(xmlAuthHeader.get()).isEqualTo("Bearer secret");
      assertThat(notified).hasSize(1);
      assertThat(notified.get(0)).contains("ACK-OK");
    }
  }

  @Test
  void start_afterEndpointSwapViaRegistrar_cachedPlanResolvesReplacementEndpoint()
      throws Exception {
    // Arrange — the same participant behind two servers: the service "moves" from A to B
    AtomicInteger hitsA = new AtomicInteger();
    AtomicInteger hitsB = new AtomicInteger();
    HttpServer serverA = pingServer(hitsA);
    HttpServer serverB = pingServer(hitsB);
    try {
      SagaDefinition def =
          SagaDefinition.newBuilder("moving-saga")
              .saga()
              .serviceStep("ping", "moving-service")
              .execution(HttpCall.newBuilder("/ping").build())
              .compensation(HttpCall.newBuilder("/noop").build())
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          DefaultSagaOrchestrator.newBuilder()
              .storeFactory(ScalarDbSagaStoreFactory.create(props))
              .httpEndpoint("moving-service", baseUrlOf(serverA))
              .add()
              .build()) {
        // Registration builds and caches the plan against the original endpoint set
        orchestrator.register(def);
        String first = orchestrator.start("moving-saga", Map.of());
        assertThat(orchestrator.getStateSnapshot(first).getStatus())
            .isEqualTo(SagaStatus.COMPLETED);
        assertThat(hitsA.get()).isEqualTo(1);

        // Act — hot-swap the service to B through the orchestrator's public registrar seam
        orchestrator
            .httpEndpointRegistrar()
            .swapHttpEndpoints(
                Map.of(
                    "moving-service",
                    new HttpServiceConfig(baseUrlOf(serverB), List.of(), -1, null, Map.of())));

        // Assert — the SAME cached plan late-binds per call: the rerun lands on B, not A,
        // covering the engine/instantiator wiring behind httpEndpointRegistrar()
        String second = orchestrator.start("moving-saga", Map.of());
        assertThat(orchestrator.getStateSnapshot(second).getStatus())
            .isEqualTo(SagaStatus.COMPLETED);
        assertThat(hitsA.get()).isEqualTo(1);
        assertThat(hitsB.get()).isEqualTo(1);
      }
    } finally {
      serverA.stop(0);
      serverB.stop(0);
    }
  }

  private static HttpServer pingServer(AtomicInteger hits) throws IOException {
    HttpServer s = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    s.createContext(
        "/ping",
        ex -> {
          hits.incrementAndGet();
          respond(ex, 200, "{}");
        });
    s.createContext("/noop", ex -> respond(ex, 200, "{}"));
    s.start();
    return s;
  }

  private static String baseUrlOf(HttpServer server) {
    return "http://localhost:" + server.getAddress().getPort();
  }

  private static HttpCall tcc(String path, String op) {
    return HttpCall.newBuilder(path).jsonBody(Map.of("op", op)).build();
  }

  private DefaultSagaOrchestrator buildOrchestrator(String serviceName) {
    return DefaultSagaOrchestrator.newBuilder()
        .storeFactory(ScalarDbSagaStoreFactory.create(props))
        .httpEndpoint(serviceName, baseUrl)
        .add()
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

  private static void respondText(HttpExchange exchange, int status, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
