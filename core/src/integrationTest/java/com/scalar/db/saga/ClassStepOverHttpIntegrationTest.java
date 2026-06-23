package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.Named;
import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.testing.CrashingStoreDecorator;
import com.scalar.db.saga.testing.SimulatedCrashError;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of <b>code steps</b> that call a participant through the injected {@link
 * SagaHttpClient} (Layer 2a). The client is resolved by the DEFAULT reflective resolver from {@code
 * httpEndpoint(name, baseUrl)} and exercised against a real in-process participant over a
 * SQLite-backed store: the SAGA happy path with output flowing from one code step into the next and
 * correlation-header propagation, engine retry on a retryable {@code 503} surfaced through {@code
 * send()}, compensation of a completed code step when a later code step fails non-retryably ({@code
 * 422}), and crash-recovery that re-resolves the code {@link SagaHttpClient} steps.
 */
class ClassStepOverHttpIntegrationTest {

  private Path tempDbPath;
  private Properties props;
  private HttpServer server;
  private String baseUrl;

  private final AtomicReference<String> sagaIdHeader = new AtomicReference<>();
  private final AtomicReference<String> sagaStepHeader = new AtomicReference<>();
  private final CopyOnWriteArrayList<String> notified = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> cancelled = new CopyOnWriteArrayList<>();
  private final AtomicInteger flakyCalls = new AtomicInteger();

  @BeforeEach
  void setUp() throws Exception {
    tempDbPath = Files.createTempFile("saga-httpclient-test-", ".db");
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
          respond(ex, 200, "{\"ack\":\"done\"}");
        });
    server.createContext("/noop", ex -> respond(ex, 200, "{}"));
    server.createContext("/reserve", ex -> respond(ex, 200, "{}"));
    server.createContext(
        "/reserve/cancel",
        ex -> {
          cancelled.add(readBody(ex));
          respond(ex, 200, "{}");
        });
    server.createContext("/charge", ex -> respond(ex, 422, "{}")); // non-retryable
    server.createContext(
        "/flaky",
        ex -> {
          if (flakyCalls.incrementAndGet() == 1) {
            respond(ex, 503, "{}"); // retryable
          } else {
            respond(ex, 200, "{\"ok\":true}");
          }
        });
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.stop(0);
    Files.deleteIfExists(tempDbPath);
  }

  // ---------------------------------------------------------------------------
  // 1. Success + output flow
  // ---------------------------------------------------------------------------

  @Test
  void start_codeStepsCallSagaHttpClient_outputFlowsAndHeadersPropagate() {
    // Arrange — a GET code step extracts a value into StepResult, a second code step POSTs with it
    SagaDefinition def =
        SagaDefinition.newBuilder("user-notification-saga", SagaMode.SAGA)
            .step("fetch", FetchUserStep.class.getName())
            .add()
            .step("notify", NotifyStep.class.getName())
            .add()
            .build();

    try (SagaManager manager = buildManager("svc")) {
      manager.register(def);

      // Act
      String sagaId = manager.start("user-notification-saga", Map.of("userId", "u-1"));

      // Assert — saga completed; the fetched value flowed into the second step's POST; the
      // correlation headers reached the participant from the injected client
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(notified).hasSize(1);
      assertThat(notified.get(0)).contains("alice");
      assertThat(sagaIdHeader.get()).isEqualTo(sagaId);
      assertThat(sagaStepHeader.get()).isEqualTo("fetch");
    }
  }

  // ---------------------------------------------------------------------------
  // 2. Retry classification
  // ---------------------------------------------------------------------------

  @Test
  void start_sagaHttpClientReturnsRetryableStatus_retriedThenCompletes() {
    // Arrange — the participant fails once (503) then succeeds; send() throws → engine retries
    SagaDefinition def =
        SagaDefinition.newBuilder("flaky-saga", SagaMode.SAGA)
            .step("flaky", FlakyStep.class.getName())
            .retryPolicy(
                RetryPolicy.newBuilder()
                    .maxAttempts(3)
                    .initialIntervalMillis(1)
                    .maxIntervalMillis(1)
                    .build())
            .add()
            .build();

    try (SagaManager manager = buildManager("svc")) {
      manager.register(def);

      // Act
      String sagaId = manager.start("flaky-saga", Map.of());

      // Assert — send() threw a retryable StepExecutionException on the 503; the engine retried and
      // the second attempt succeeded
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(flakyCalls.get()).isEqualTo(2);
    }
  }

  // ---------------------------------------------------------------------------
  // 3. Compensation
  // ---------------------------------------------------------------------------

  @Test
  void start_laterCodeStepFailsNonRetryable_priorCodeStepCompensated() {
    // Arrange — reserve (code step) succeeds, charge (code step) fails non-retryably (422)
    SagaDefinition def =
        SagaDefinition.newBuilder("payment-saga", SagaMode.SAGA)
            .step("reserve", ReserveStep.class.getName())
            .add()
            .step("charge", ChargeStep.class.getName())
            .add()
            .build();

    try (SagaManager manager = buildManager("svc")) {
      manager.register(def);

      // Act
      String sagaId = manager.start("payment-saga", Map.of());

      // Assert — saga compensated; the prior code step's compensate() HTTP call ran
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(cancelled).hasSize(1);
    }
  }

  // ---------------------------------------------------------------------------
  // 4. Crash-recovery over HTTP-backed code steps
  // ---------------------------------------------------------------------------

  @Test
  void recover_crashAfterCodeStep_reResolvesHttpStepsAndCompletes() {
    // Arrange — two code steps (each using the injected SagaHttpClient)
    SagaDefinition def =
        SagaDefinition.newBuilder("recover-saga", SagaMode.SAGA)
            .step("fetch", FetchUserStep.class.getName())
            .add()
            .step("notify", NotifyStep.class.getName())
            .add()
            .build();

    String sagaId = "http-crash-saga-1";

    // First run — crash after the first (code) step completes
    try (SagaStore baseStore = ScalarDbSagaStoreFactory.create(props).createStore();
        SagaStore crashingStore = new CrashingStoreDecorator(baseStore, 0);
        SagaManager manager = buildManager(crashingStore, "svc")) {
      manager.register(def);

      try {
        manager.start(sagaId, "recover-saga", Map.of("userId", "u-1"));
      } catch (SimulatedCrashError expected) {
        // Expected crash after the code step's completion event was persisted
      }

      // The first code step ran; the second has not yet been reached
      assertThat(manager.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.RUNNING);
      assertThat(notified).isEmpty();
    }

    // Restart — fresh manager built with the SAME httpEndpoint; recovery must re-resolve both kinds
    try (SagaStore recoveryStore = ScalarDbSagaStoreFactory.create(props).createStore();
        SagaManager recovered = buildManager(recoveryStore, "svc")) {
      recovered.register(def);
      recoveryStore.markForRecovery(sagaId);
      recovered.recover();

      // Assert — recovery re-resolved both SagaHttpClient code steps and completed the saga; the
      // second code step ran exactly once (the first step's result was persisted before the crash,
      // so it is not re-executed)
      SagaStateSnapshot result = recovered.getStateSnapshot(sagaId);
      assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(notified).hasSize(1);
      assertThat(notified.get(0)).contains("alice");
    }
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private SagaManager buildManager(String serviceName) {
    return SagaManager.newBuilder()
        .storeFactory(ScalarDbSagaStoreFactory.create(props))
        .httpEndpoint(serviceName, baseUrl)
        .add()
        .build();
  }

  private SagaManager buildManager(SagaStore sagaStore, String serviceName) {
    return SagaManager.newBuilder()
        .storeFactory(() -> sagaStore)
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

  // ===========================================================================
  // Code-step fixtures (resolved by the default reflective resolver; the @Named SagaHttpClient is
  // injected from the registered httpEndpoint)
  // ===========================================================================

  /** GETs the user and extracts {@code name} into {@code userName}. */
  public static class FetchUserStep implements Step {
    private final SagaHttpClient http;

    public FetchUserStep(@Named("svc") SagaHttpClient http) {
      this.http = http;
    }

    @Override
    public String getName() {
      return "fetch";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      String userId = context.get("userId", String.class).orElseThrow();
      Map<String, Object> body = http.get("/users/" + userId).send().bodyJsonObject();
      return StepResult.of("userName", Objects.requireNonNull(body.get("name")));
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** POSTs to {@code /notify} using the {@code userName} produced by {@link FetchUserStep}. */
  public static class NotifyStep implements Step {
    private final SagaHttpClient http;

    public NotifyStep(@Named("svc") SagaHttpClient http) {
      this.http = http;
    }

    @Override
    public String getName() {
      return "notify";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      String userName = context.get("userName", String.class).orElseThrow();
      Map<String, Object> body =
          http.post("/notify").jsonBody(Map.of("user", userName)).send().bodyJsonObject();
      return StepResult.of("ack", Objects.requireNonNull(body.get("ack")));
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Calls {@code /flaky}: {@code send()} throws a retryable exception on the first 503. */
  public static class FlakyStep implements Step {
    private final SagaHttpClient http;

    public FlakyStep(@Named("svc") SagaHttpClient http) {
      this.http = http;
    }

    @Override
    public String getName() {
      return "flaky";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      Map<String, Object> body = http.get("/flaky").send().bodyJsonObject();
      return StepResult.of("ok", Objects.requireNonNull(body.get("ok")));
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Reserves via {@code /reserve}; compensates via {@code /reserve/cancel}. */
  public static class ReserveStep implements Step {
    private final SagaHttpClient http;

    public ReserveStep(@Named("svc") SagaHttpClient http) {
      this.http = http;
    }

    @Override
    public String getName() {
      return "reserve";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      http.post("/reserve").jsonBody(Map.of("op", "reserve")).send();
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {
      try {
        http.post("/reserve/cancel").jsonBody(Map.of("op", "cancel")).send();
      } catch (StepExecutionException e) {
        throw new StepCompensationException(e);
      }
    }
  }

  /** Calls {@code /charge}, which returns a non-retryable 422 → {@code send()} throws. */
  public static class ChargeStep implements Step {
    private final SagaHttpClient http;

    public ChargeStep(@Named("svc") SagaHttpClient http) {
      this.http = http;
    }

    @Override
    public String getName() {
      return "charge";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      http.post("/charge").jsonBody(Map.of("op", "charge")).send();
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }
}
