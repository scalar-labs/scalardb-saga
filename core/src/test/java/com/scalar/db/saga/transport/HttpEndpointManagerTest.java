package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.HttpCall;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HttpEndpointManager}: per-call resolution, the swap semantics (reuse on
 * unchanged topology, in-place header rotation, graceful retirement), the retired-list bound, and
 * the bounded drain at close.
 */
class HttpEndpointManagerTest {

  private static final Map<Phase, CallSpec> SAGA_PHASES =
      Map.of(
          Phase.EXECUTION, HttpCall.newBuilder("/do").build(),
          Phase.COMPENSATION, HttpCall.newBuilder("/undo").build());
  private static final Map<Phase, CallSpec> TCC_PHASES =
      Map.of(
          Phase.RESERVATION, HttpCall.newBuilder("/reserve").build(),
          Phase.CONFIRMATION, HttpCall.newBuilder("/confirm").build(),
          Phase.CANCELLATION, HttpCall.newBuilder("/cancel").build());
  private static final SagaContext CTX = new FakeSagaContext("saga-1", Map.of());

  private static HttpServiceConfig config(String baseUrl, Map<String, String> headers) {
    return new HttpServiceConfig(baseUrl, List.of(), -1, null, headers);
  }

  private static HttpEndpointManager managerOf(String name, HttpServiceConfig config) {
    return HttpEndpointManager.create(Map.of(name, config), null);
  }

  /**
   * A config whose endpoint cannot be built: a supplied redirect-following client combined with an
   * allowlist is rejected by {@code HttpEndpoint.create} (SSRF-via-redirect), after config
   * construction succeeds — the way to make an endpoint-set build fail partway.
   */
  private static HttpServiceConfig unbuildableConfig() {
    HttpClient redirecting =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();
    return new HttpServiceConfig(
        "http://bad:8080", List.of("allowed-host"), -1, redirecting, Map.of());
  }

  private static HttpServer startServer(com.sun.net.httpserver.HttpHandler handler)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", handler);
    server.start();
    return server;
  }

  private static String baseUrlOf(HttpServer server) {
    return "http://localhost:" + server.getAddress().getPort();
  }

  private static void respondJson(com.sun.net.httpserver.HttpExchange ex) throws IOException {
    byte[] bytes = "{}".getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/json");
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  // =========================================================================
  // Resolution
  // =========================================================================

  @Nested
  class Resolve {

    @Test
    void resolve_registeredService_returnsItsEndpointAdapter() throws Exception {
      // Arrange
      HttpEndpointManager manager = managerOf("account", config("http://account:8080", Map.of()));

      // Act
      TransportAdapter adapter = manager.resolve("account");

      // Assert — the adapter is the registered endpoint's own
      HttpEndpoint endpoint = java.util.Objects.requireNonNull(manager.endpointOrNull("account"));
      assertThat(adapter).isSameAs(endpoint.transportAdapter());
      manager.close();
    }

    @Test
    void sagaHttpClients_twoEndpointsRegistered_returnsOneClientPerEndpoint() {
      // Arrange
      HttpEndpointManager manager =
          HttpEndpointManager.create(
              Map.of(
                  "account", config("http://account:8080", Map.of()),
                  "payment", config("http://payment:8080", Map.of())),
              null);

      // Act
      Map<String, SagaHttpClient> clients = manager.sagaHttpClients();

      // Assert — one consistent snapshot: every registered name maps to a client
      assertThat(clients).containsOnlyKeys("account", "payment");
      assertThat(clients.values()).doesNotContainNull();
      manager.close();
    }

    @Test
    void resolve_unknownService_throwsRetryableKnownNotCommitted() {
      // Arrange
      HttpEndpointManager manager = managerOf("account", config("http://account:8080", Map.of()));

      // Act
      Throwable thrown = catchThrowable(() -> manager.resolve("payment"));

      // Assert — the miss is pre-send by construction and self-heals once configuration
      // propagates, so it must be retryable and known-not-committed
      assertThat(thrown).isInstanceOf(TransportException.class);
      assertThat(((TransportException) thrown).isRetryable()).isTrue();
      assertThat(((TransportException) thrown).knownNotCommitted()).isTrue();
      manager.close();
    }
  }

  // =========================================================================
  // Step construction (moved here from the per-endpoint factories)
  // =========================================================================

  @Nested
  class StepConstruction {

    private Map<Phase, CallSpec> asyncSagaPhases() {
      return Map.of(
          Phase.EXECUTION, HttpCall.newBuilder("/do").async(true).build(),
          Phase.COMPENSATION, HttpCall.newBuilder("/undo").build());
    }

    @Test
    void toStep_withSagaPhases_returnsNamedStep() {
      // Arrange
      HttpEndpointManager manager = managerOf("account", config("http://account:8080", Map.of()));

      // Act
      Step step = manager.toStep("debit", "account", SAGA_PHASES);

      // Assert
      assertThat(step.getName()).isEqualTo("debit");
      manager.close();
    }

    @Test
    void toTccStep_withTccPhases_returnsNamedTccStep() {
      // Arrange
      HttpEndpointManager manager = managerOf("booking", config("http://booking:8080", Map.of()));

      // Act
      TccStep step = manager.toTccStep("seat", "booking", TCC_PHASES);

      // Assert
      assertThat(step.getName()).isEqualTo("seat");
      manager.close();
    }

    @Test
    void toStep_asyncPhaseWithoutCallbackProvider_throwsSagaDefinitionException() {
      // An async step with no callback URL provider configured cannot be provisioned — fail fast
      // at plan build rather than parking a saga that could never be completed.
      HttpEndpointManager manager = managerOf("account", config("http://account:8080", Map.of()));

      assertThatThrownBy(() -> manager.toStep("debit", "account", asyncSagaPhases()))
          .isInstanceOfSatisfying(
              SagaDefinitionException.class,
              e -> {
                assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_STEP_DEFINITION);
                assertThat(e.getMetadata()).containsEntry("step_name", "debit");
              });
      manager.close();
    }

    @Test
    void toStep_asyncPhaseWithCallbackProvider_succeeds() {
      HttpEndpointManager manager =
          HttpEndpointManager.create(
              Map.of("account", config("http://account:8080", Map.of())),
              (sagaId, step) -> "http://cb/x");

      assertThatCode(() -> manager.toStep("debit", "account", asyncSagaPhases()))
          .doesNotThrowAnyException();
      manager.close();
    }
  }

  // =========================================================================
  // Creation
  // =========================================================================

  @Nested
  class Create {

    @Test
    void create_endpointCreationFailsPartway_throwsCreationError() {
      // The initial build goes through the swap path, so its rollback covers a config that fails
      // here too; from outside, the creation error propagates and no manager is returned.
      Map<String, HttpServiceConfig> configs = new LinkedHashMap<>();
      configs.put("good", config("http://good:8080", Map.of()));
      configs.put("bad", unbuildableConfig());

      assertThatThrownBy(() -> HttpEndpointManager.create(configs, null))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  // =========================================================================
  // Swap semantics
  // =========================================================================

  @Nested
  class Swap {

    @Test
    void swapHttpEndpoints_headersOnlyChange_reusesEndpointAndAppliesNewHeader() throws Exception {
      // Arrange — a server recording the Authorization header of each request
      AtomicReference<String> seenAuth = new AtomicReference<>();
      HttpServer server =
          startServer(
              ex -> {
                seenAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
                respondJson(ex);
              });
      try {
        String baseUrl = baseUrlOf(server);
        HttpEndpointManager manager =
            managerOf("svc", config(baseUrl, Map.of("Authorization", "Bearer old")));
        HttpEndpoint before = manager.endpointOrNull("svc");
        HttpCall call = HttpCall.newBuilder("/x").method(HttpMethod.GET).build();
        manager.resolve("svc").call(call, CTX, "s");
        assertThat(seenAuth.get()).isEqualTo("Bearer old");

        // Act — rotate the secret with no topology change
        manager.swapHttpEndpoints(
            Map.of("svc", config(baseUrl, Map.of("Authorization", "Bearer new"))));

        // Assert — the SAME endpoint instance serves the new header: rotation without churn, and
        // nothing was retired
        assertThat(manager.endpointOrNull("svc")).isSameAs(before);
        assertThat(manager.retiredCount()).isZero();
        manager.resolve("svc").call(call, CTX, "s");
        assertThat(seenAuth.get()).isEqualTo("Bearer new");
        manager.close();
      } finally {
        server.stop(0);
      }
    }

    @Test
    void swapHttpEndpoints_topologyChange_replacesEndpointAndRetiresOld() throws Exception {
      // Arrange
      HttpServer server = startServer(HttpEndpointManagerTest::respondJson);
      try {
        String baseUrl = baseUrlOf(server);
        HttpEndpointManager manager = managerOf("svc", config(baseUrl, Map.of()));
        HttpEndpoint before = manager.endpointOrNull("svc");
        TransportAdapter oldAdapter = manager.resolve("svc");

        // Act — change the base URL (topology)
        manager.swapHttpEndpoints(Map.of("svc", config(baseUrl + "/v2", Map.of())));

        // Assert — a new endpoint serves the name; the old one is retired and a call through the
        // stale adapter fails pre-send as retryable + known-not-committed
        assertThat(manager.endpointOrNull("svc")).isNotSameAs(before);
        HttpCall call = HttpCall.newBuilder("/x").method(HttpMethod.GET).build();
        Throwable thrown = catchThrowable(() -> oldAdapter.call(call, CTX, "s"));
        assertThat(thrown).isInstanceOf(TransportException.class);
        assertThat(((TransportException) thrown).isRetryable()).isTrue();
        assertThat(((TransportException) thrown).knownNotCommitted()).isTrue();
        // The replacement resolves and works
        assertThatCode(() -> manager.resolve("svc").call(call, CTX, "s"))
            .doesNotThrowAnyException();
        manager.close();
      } finally {
        server.stop(0);
      }
    }

    // The next three tests hold baseUrl fixed and change exactly one other topology component, so
    // each conjunct of HttpEndpoint.sameTopology is pinned as a swap trigger on its own. If one
    // were dropped, the swap would silently reuse the old endpoint and the new value would never
    // take effect — for allowedHosts that means a tightened SSRF allowlist being ignored.

    @Test
    void swapHttpEndpoints_allowedHostsOnlyChange_replacesEndpointAndRetiresOld() {
      // Arrange — allow-all to start
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));
      HttpEndpoint before = manager.endpointOrNull("svc");

      // Act — tighten the SSRF allowlist; everything else unchanged
      manager.swapHttpEndpoints(
          Map.of(
              "svc", new HttpServiceConfig("http://svc:8080", List.of("svc"), -1, null, Map.of())));

      // Assert
      assertThat(manager.endpointOrNull("svc")).isNotSameAs(before);
      assertThat(manager.retiredCount()).isEqualTo(1);
      manager.close();
    }

    @Test
    void swapHttpEndpoints_maxBodyBytesOnlyChange_replacesEndpointAndRetiresOld() {
      // Arrange — default body cap to start
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));
      HttpEndpoint before = manager.endpointOrNull("svc");

      // Act — change only the body cap
      manager.swapHttpEndpoints(
          Map.of("svc", new HttpServiceConfig("http://svc:8080", List.of(), 1024, null, Map.of())));

      // Assert
      assertThat(manager.endpointOrNull("svc")).isNotSameAs(before);
      assertThat(manager.retiredCount()).isEqualTo(1);
      manager.close();
    }

    @Test
    void swapHttpEndpoints_httpClientOnlyChange_replacesEndpointAndRetiresOld() throws Exception {
      // Arrange — a caller-supplied client
      HttpClient clientA = HttpClient.newBuilder().build();
      HttpClient clientB = HttpClient.newBuilder().build();
      HttpEndpointManager manager =
          HttpEndpointManager.create(
              Map.of(
                  "svc",
                  new HttpServiceConfig("http://svc:8080", List.of(), -1, clientA, Map.of())),
              null);
      HttpEndpoint before = manager.endpointOrNull("svc");
      TransportAdapter oldAdapter = manager.resolve("svc");

      // Act — an otherwise identical config carrying a different client instance; client equality
      // is identity, so this is a topology change
      manager.swapHttpEndpoints(
          Map.of(
              "svc", new HttpServiceConfig("http://svc:8080", List.of(), -1, clientB, Map.of())));

      // Assert — the old endpoint was retired: a call through its stale adapter fails pre-send.
      // The retired list is already empty, not still holding it: a caller-supplied client is
      // never shut down here, so the retiree counts as terminated and the same swap sweeps it.
      assertThat(manager.endpointOrNull("svc")).isNotSameAs(before);
      HttpCall call = HttpCall.newBuilder("/x").method(HttpMethod.GET).build();
      Throwable thrown = catchThrowable(() -> oldAdapter.call(call, CTX, "s"));
      assertThat(thrown).isInstanceOf(TransportException.class);
      assertThat(((TransportException) thrown).isRetryable()).isTrue();
      assertThat(manager.retiredCount()).isZero();
      manager.close();
    }

    @Test
    void swapHttpEndpoints_serviceRemoved_resolveMissesRetryably() {
      // Arrange
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));

      // Act
      manager.swapHttpEndpoints(Map.of());

      // Assert
      Throwable thrown = catchThrowable(() -> manager.resolve("svc"));
      assertThat(thrown).isInstanceOf(TransportException.class);
      assertThat(((TransportException) thrown).isRetryable()).isTrue();
      manager.close();
    }

    @Test
    void swapHttpEndpoints_serviceAdded_resolves() throws Exception {
      // Arrange
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));

      // Act
      manager.swapHttpEndpoints(
          Map.of(
              "svc", config("http://svc:8080", Map.of()),
              "other", config("http://other:8080", Map.of())));

      // Assert
      assertThat(manager.resolve("other")).isNotNull();
      assertThat(manager.retiredCount()).isZero();
      manager.close();
    }

    @Test
    void swapHttpEndpoints_retiredListSweptOnceClientsTerminate() throws Exception {
      // Arrange — a topology change retires the original endpoint
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));
      HttpEndpoint before = java.util.Objects.requireNonNull(manager.endpointOrNull("svc"));
      manager.swapHttpEndpoints(Map.of("svc", config("http://svc:8081", Map.of())));
      assertThat(manager.retiredCount()).isEqualTo(1);

      // Act — force the retiree's client to terminate (an idle client's graceful termination
      // latency is a JDK implementation detail and flakes on CI), then the next swap — even a
      // no-op one — sweeps the entry
      before.shutdownNow();
      assertThat(before.awaitTermination(Duration.ofSeconds(10))).isTrue();
      manager.swapHttpEndpoints(Map.of("svc", config("http://svc:8081", Map.of())));

      // Assert
      assertThat(manager.retiredCount()).isZero();
      manager.close();
    }

    @Test
    void swapHttpEndpoints_duringInFlightCall_callCompletesAgainstOldEndpoint() throws Exception {
      // Arrange — a handler that blocks until released, so a call is provably in flight while the
      // swap runs
      CountDownLatch requestArrived = new CountDownLatch(1);
      CountDownLatch releaseResponse = new CountDownLatch(1);
      HttpServer server =
          startServer(
              ex -> {
                requestArrived.countDown();
                try {
                  releaseResponse.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                respondJson(ex);
              });
      try {
        String baseUrl = baseUrlOf(server);
        HttpEndpointManager manager = managerOf("svc", config(baseUrl, Map.of()));
        TransportAdapter oldAdapter = manager.resolve("svc");
        HttpCall call = HttpCall.newBuilder("/x").method(HttpMethod.GET).build();
        CompletableFuture<Throwable> inFlight =
            CompletableFuture.supplyAsync(
                () -> catchThrowable(() -> oldAdapter.call(call, CTX, "s")));
        assertThat(requestArrived.await(10, TimeUnit.SECONDS)).isTrue();

        // Act — retire the endpoint while its call is mid-exchange, then let the server respond
        manager.swapHttpEndpoints(Map.of("svc", config(baseUrl + "/v2", Map.of())));
        releaseResponse.countDown();

        // Assert — graceful retirement: the in-flight exchange completed normally
        assertThat(inFlight.get(10, TimeUnit.SECONDS)).isNull();
        manager.close();
      } finally {
        server.stop(0);
      }
    }

    @Test
    void swapHttpEndpoints_endpointCreationFailsPartway_rollsBackCreatedAndKeepsCurrentSet()
        throws Exception {
      // Arrange — a live endpoint, and a candidate set whose second entry cannot be built after
      // the first already created a fresh endpoint (LinkedHashMap fixes the build order)
      HttpServer server = startServer(HttpEndpointManagerTest::respondJson);
      try {
        String baseUrl = baseUrlOf(server);
        HttpEndpointManager manager = managerOf("svc", config(baseUrl, Map.of()));
        HttpEndpoint before = manager.endpointOrNull("svc");
        Map<String, HttpServiceConfig> services = new LinkedHashMap<>();
        services.put("fresh", config("http://fresh:8080", Map.of()));
        services.put("bad", unbuildableConfig());

        // Act
        Throwable thrown = catchThrowable(() -> manager.swapHttpEndpoints(services));

        // Assert — the creation failure propagates; nothing was published (the current set keeps
        // serving, the attempt's entries are absent); the endpoint the attempt already built was
        // shut down and joined the retired list rather than leaking
        assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
        assertThat(manager.endpointOrNull("svc")).isSameAs(before);
        assertThat(manager.contains("fresh")).isFalse();
        HttpCall call = HttpCall.newBuilder("/x").method(HttpMethod.GET).build();
        assertThatCode(() -> manager.resolve("svc").call(call, CTX, "s"))
            .doesNotThrowAnyException();
        assertThat(manager.retiredCount()).isEqualTo(1);

        // A retry with the config fixed converges on the intended set
        manager.swapHttpEndpoints(
            Map.of(
                "svc", config(baseUrl, Map.of()),
                "fresh", config("http://fresh:8080", Map.of())));
        assertThat(manager.contains("fresh")).isTrue();
        manager.close();
      } finally {
        server.stop(0);
      }
    }

    @Test
    void swapHttpEndpoints_rotationAppliedByFailedSwap_reapplyingPreviousConfigRestoresHeader()
        throws Exception {
      // Arrange — a server recording the Authorization header, and an endpoint on the last known
      // good secret
      AtomicReference<String> seenAuth = new AtomicReference<>();
      HttpServer server =
          startServer(
              ex -> {
                seenAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
                respondJson(ex);
              });
      try {
        String baseUrl = baseUrlOf(server);
        HttpServiceConfig lastKnownGood = config(baseUrl, Map.of("Authorization", "Bearer old"));
        HttpEndpointManager manager = managerOf("svc", lastKnownGood);
        HttpCall call = HttpCall.newBuilder("/x").method(HttpMethod.GET).build();

        // A candidate set that rotates svc's secret and then fails on a later entry: the rotation
        // is already live on the reused endpoint when the failure hits (LinkedHashMap fixes the
        // build order)
        Map<String, HttpServiceConfig> failedCandidate = new LinkedHashMap<>();
        failedCandidate.put("svc", config(baseUrl, Map.of("Authorization", "Bearer rotated")));
        failedCandidate.put("bad", unbuildableConfig());
        assertThat(catchThrowable(() -> manager.swapHttpEndpoints(failedCandidate)))
            .isInstanceOf(IllegalArgumentException.class);
        manager.resolve("svc").call(call, CTX, "s");
        assertThat(seenAuth.get()).isEqualTo("Bearer rotated");

        // Act — the operator recovery the registrar contract promises: re-apply the last known
        // good configuration
        manager.swapHttpEndpoints(Map.of("svc", lastKnownGood));

        // Assert — the rollback restored the previous header; a guard comparing the candidate
        // against the recorded config would skip this rotation, because the record never saw the
        // failed swap's rotation and still holds the old value
        manager.resolve("svc").call(call, CTX, "s");
        assertThat(seenAuth.get()).isEqualTo("Bearer old");
        manager.close();
      } finally {
        server.stop(0);
      }
    }

    @Test
    @SuppressWarnings("NullAway") // deliberately passing null to exercise the runtime guard
    void swapHttpEndpoints_nullServicesGiven_throwsNullPointerException() {
      // Arrange
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));

      // Act & Assert — the public seam fails fast, before the swap lock and before any build
      assertThatThrownBy(() -> manager.swapHttpEndpoints(null))
          .isInstanceOf(NullPointerException.class);
      manager.close();
    }

    @Test
    @SuppressWarnings("NullAway") // deliberately passing null to exercise the runtime guard
    void swapHttpEndpoints_nullConfigValueGiven_throwsNullPointerException() {
      // Arrange
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));
      Map<String, HttpServiceConfig> services = new LinkedHashMap<>();
      services.put("svc", null);

      // Act & Assert — rejected up front: nothing was built, retired, or rotated
      assertThatThrownBy(() -> manager.swapHttpEndpoints(services))
          .isInstanceOf(NullPointerException.class);
      assertThat(manager.contains("svc")).isTrue();
      assertThat(manager.retiredCount()).isZero();
      manager.close();
    }

    @Test
    void swapHttpEndpoints_afterClose_throwsIllegalStateException() {
      // Arrange
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));
      manager.close();

      // Act & Assert
      assertThatThrownBy(() -> manager.swapHttpEndpoints(Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  // =========================================================================
  // Close
  // =========================================================================

  @Nested
  class Close {

    @Test
    void close_idleEndpoints_completesAndIsIdempotent() {
      // Arrange
      HttpEndpointManager manager =
          HttpEndpointManager.create(
              Map.of(
                  "a", config("http://a:8080", Map.of()),
                  "b", config("http://b:8080", Map.of())),
              null);

      // Act & Assert
      assertThatCode(
              () -> {
                manager.close();
                manager.close();
              })
          .doesNotThrowAnyException();
    }

    @Test
    void close_drainsRetiredEndpointsToo() throws Exception {
      // Arrange — leave one retiree behind, then close
      HttpEndpointManager manager = managerOf("svc", config("http://svc:8080", Map.of()));
      HttpEndpoint before = java.util.Objects.requireNonNull(manager.endpointOrNull("svc"));
      manager.swapHttpEndpoints(Map.of("svc", config("http://svc:8081", Map.of())));

      // Act
      manager.close();

      // Assert — both the retiree and the current endpoint have terminated
      assertThat(before.isTerminated()).isTrue();
      HttpEndpoint current = java.util.Objects.requireNonNull(manager.endpointOrNull("svc"));
      assertThat(current.isTerminated()).isTrue();
    }
  }
}
