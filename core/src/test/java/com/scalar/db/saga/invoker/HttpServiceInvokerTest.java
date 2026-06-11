package com.scalar.db.saga.invoker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link HttpServiceInvoker}/{@link HttpExchange} against a real in-process HTTP
 * participant.
 */
class HttpServiceInvokerTest {

  private static final ServiceCallContext CTX = new FakeServiceCallContext("saga-1", "debit");

  private static HttpServer server;
  private static String baseUrl;

  @BeforeAll
  static void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/echo",
        ex -> {
          String method = ex.getRequestMethod();
          String step = ex.getRequestHeaders().getFirst(HttpHeaders.SAGA_STEP);
          String id = ex.getRequestHeaders().getFirst(HttpHeaders.SAGA_ID);
          String auth = ex.getRequestHeaders().getFirst("Authorization");
          String query = ex.getRequestURI().getQuery();
          ex.getRequestBody().readAllBytes();
          respond(
              ex,
              200,
              "",
              String.format(
                  "{\"method\":\"%s\",\"step\":\"%s\",\"id\":\"%s\",\"auth\":\"%s\",\"query\":\"%s\"}",
                  method, step, id, auth, query));
        });
    server.createContext("/array", ex -> respond(ex, 200, "", "[1,2,3]"));
    server.createContext("/fail503", ex -> respond(ex, 503, "", "{}"));
    server.createContext("/fail422", ex -> respond(ex, 422, "", "{\"reason\":\"declined\"}"));
    server.createContext("/retryable409", ex -> respond(ex, 409, "true", "{}"));
    server.createContext("/nonRetryable503", ex -> respond(ex, 503, "false", "{}"));
    server.createContext("/big", ex -> respond(ex, 200, "", "x".repeat(5000)));
    server.createContext(
        "/bigChunked",
        ex -> {
          // Length 0 → chunked transfer encoding with NO Content-Length header, so the size
          // can only be discovered by reading the stream (exercises the bounded-read path).
          byte[] bytes = "x".repeat(5000).getBytes(StandardCharsets.UTF_8);
          ex.sendResponseHeaders(200, 0);
          try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
          }
        });
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterAll
  static void tearDown() {
    server.stop(0);
  }

  @Test
  void execute_success_propagatesCorrelationHeadersAndReturnsBody() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution(
                (http, ctx) ->
                    StepResult.of(
                        http.post("/echo").jsonBody(Map.of("k", "v")).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    StepResult result = invoker.execute("debit", CTX);

    // X-Saga-Step carries the step name (not the method); X-Saga-Id the saga id.
    assertThat(result.getOutput())
        .containsEntry("method", "POST")
        .containsEntry("step", "debit")
        .containsEntry("id", "saga-1");
  }

  @Test
  void execute_customHeaderAndQueryParam_arePropagated() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution(
                (http, ctx) ->
                    StepResult.of(
                        http.get("/echo")
                            .header("Authorization", "Bearer token-123")
                            .queryParam("q", "a b")
                            .send()
                            .jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    StepResult result = invoker.execute("debit", CTX);

    assertThat(result.getOutput())
        .containsEntry("method", "GET")
        .containsEntry("auth", "Bearer token-123")
        .containsEntry("query", "q=a+b");
  }

  @Test
  void execute_jsonArrayResponse_isDecodable() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("list")
            .execution((http, ctx) -> StepResult.of("items", http.get("/array").send().jsonArray()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    StepResult result = invoker.execute("list", CTX);

    assertThat(result.getOutput()).containsEntry("items", java.util.List.of(1, 2, 3));
  }

  @Test
  void execute_actionUsesGet_sendsGetRequestWithStepHeader() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("read")
            .execution((http, ctx) -> StepResult.of(http.get("/echo").send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    StepResult result = invoker.execute("read", CTX);

    assertThat(result.getOutput()).containsEntry("method", "GET").containsEntry("step", "debit");
  }

  @Test
  void execute_actionUsesPut_sendsPutRequest() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("upd")
            .execution(
                (http, ctx) ->
                    StepResult.of(http.put("/echo").jsonBody(Map.of("a", "b")).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    assertThat(invoker.execute("upd", CTX).getOutput()).containsEntry("method", "PUT");
  }

  @Test
  void execute_actionUsesPatch_sendsPatchRequest() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("upd")
            .execution(
                (http, ctx) ->
                    StepResult.of(
                        http.patch("/echo").jsonBody(Map.of("a", "b")).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    assertThat(invoker.execute("upd", CTX).getOutput()).containsEntry("method", "PATCH");
  }

  @Test
  void execute_actionUsesDelete_sendsDeleteRequest() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("del")
            .execution((http, ctx) -> StepResult.of(http.delete("/echo").send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    assertThat(invoker.execute("del", CTX).getOutput()).containsEntry("method", "DELETE");
  }

  @Test
  void execute_httpStatus503_throwsRetryable() {
    HttpServiceInvoker invoker = invokerPosting("debit", "/fail503");

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void execute_httpStatus422_throwsNonRetryable() {
    HttpServiceInvoker invoker = invokerPosting("debit", "/fail422");

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void execute_errorResponseBody_isReadableFromException() {
    // A lambda can catch the HttpCallException and read the error body off the attached response.
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution(
                (http, ctx) -> {
                  try {
                    return StepResult.of(
                        http.post("/fail422").jsonBody(Map.of()).send().jsonObject());
                  } catch (HttpCallException e) {
                    String reason = (String) e.response().orElseThrow().jsonObject().get("reason");
                    throw new HttpCallException("declined: " + reason, false);
                  }
                })
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(t).hasMessageContaining("declined");
  }

  @Test
  void execute_xSagaRetryableTrueOn409_overridesToRetryable() {
    HttpServiceInvoker invoker = invokerPosting("debit", "/retryable409");

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void execute_xSagaRetryableFalseOn503_overridesToNonRetryable() {
    HttpServiceInvoker invoker = invokerPosting("debit", "/nonRetryable503");

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void execute_noActionForMethod_throwsStepExecutionException() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("other")
            .execution(
                (http, ctx) ->
                    StepResult.of(http.post("/echo").jsonBody(Map.of()).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
  }

  @Test
  void execute_hostNotAllowed_throwsNonRetryable() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .allowedHosts("other.host")
            .operation("debit")
            .execution(
                (http, ctx) ->
                    StepResult.of(http.post("/echo").jsonBody(Map.of()).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void execute_responseExceedsBodyLimit_throwsNonRetryable() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .maxBodyBytes(100)
            .operation("debit")
            .execution(
                (http, ctx) ->
                    StepResult.of(http.post("/big").jsonBody(Map.of()).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void execute_chunkedResponseExceedsBodyLimit_throwsNonRetryable() {
    // No Content-Length header, so the limit must be enforced while streaming the body, not via
    // the Content-Length early-reject path.
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .maxBodyBytes(100)
            .operation("debit")
            .execution(
                (http, ctx) ->
                    StepResult.of(http.post("/bigChunked").jsonBody(Map.of()).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void execute_requestExceedsBodyLimit_throwsNonRetryable() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .maxBodyBytes(10)
            .operation("debit")
            .execution(
                (http, ctx) ->
                    StepResult.of(
                        http.post("/echo")
                            .jsonBody(Map.of("k", "x".repeat(100)))
                            .send()
                            .jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void execute_malformedUri_throwsNonRetryable() {
    // An illegal URI character (space) in the path makes URI.create throw; it must surface as a
    // non-retryable HttpCallException, not escape as a generic RuntimeException.
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution(
                (http, ctx) ->
                    StepResult.of(http.post("/bad path").jsonBody(Map.of()).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.execute("debit", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void compensate_success_doesNotThrow() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution((http, ctx) -> StepResult.empty())
            .compensation((http, ctx) -> http.post("/echo").jsonBody(Map.of()).send())
            .add()
            .build();

    invoker.compensate("debit", CTX);
  }

  @Test
  void compensate_httpFailure_throwsStepCompensationException() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution((http, ctx) -> StepResult.empty())
            .compensation((http, ctx) -> http.post("/fail503").jsonBody(Map.of()).send())
            .add()
            .build();

    assertThat(catchThrowable(() -> invoker.compensate("debit", CTX)))
        .isInstanceOf(StepCompensationException.class);
  }

  @Test
  void compensate_noCompensationForMethod_throwsStepCompensationException() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("other")
            .execution(
                (http, ctx) ->
                    StepResult.of(http.post("/echo").jsonBody(Map.of()).send().jsonObject()))
            .compensation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.compensate("debit", CTX));

    assertThat(t).isInstanceOf(StepCompensationException.class);
  }

  @Test
  void build_noOperations_throwsIllegalState() {
    HttpServiceInvoker.Builder builder = HttpServiceInvoker.newBuilder(baseUrl);

    assertThat(catchThrowable(builder::build)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void supportsExecuteAndCompensate_reflectRegistrations() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution((http, ctx) -> StepResult.empty())
            .compensation((http, ctx) -> {})
            .add()
            .build();

    assertThat(invoker.supportsExecute("debit")).isTrue();
    assertThat(invoker.supportsExecute("missing")).isFalse();
    assertThat(invoker.supportsCompensate("debit")).isTrue();
    assertThat(invoker.supportsCompensate("missing")).isFalse();
  }

  @Test
  void close_userSuppliedClient_isNotClosed() {
    HttpClient userClient = mock(HttpClient.class);
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .httpClient(userClient)
            .operation("debit")
            .execution((http, ctx) -> StepResult.empty())
            .compensation((http, ctx) -> {})
            .add()
            .build();

    invoker.close();

    // The caller owns a client they supplied — the invoker must not close it.
    verify(userClient, never()).close();
  }

  @Test
  void close_ownedClientInvoker_isIdempotent() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution((http, ctx) -> StepResult.empty())
            .compensation((http, ctx) -> {})
            .add()
            .build();

    // Owns its client → close() releases it; calling twice must not throw.
    invoker.close();
    invoker.close();
  }

  @Test
  void reserve_success_propagatesCorrelationHeadersAndReturnsBody() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation(
                (http, ctx) ->
                    StepResult.of(
                        http.post("/echo").jsonBody(Map.of("k", "v")).send().jsonObject()))
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    StepResult result = invoker.reserve("reserveStock", CTX);

    // X-Saga-Step carries the step name (not the operation); X-Saga-Id the saga id.
    assertThat(result.getOutput())
        .containsEntry("method", "POST")
        .containsEntry("step", "debit")
        .containsEntry("id", "saga-1");
  }

  @Test
  void confirm_success_doesNotThrow() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> http.post("/echo").jsonBody(Map.of()).send())
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    invoker.confirm("reserveStock", CTX);
  }

  @Test
  void cancel_success_doesNotThrow() throws Exception {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> http.post("/echo").jsonBody(Map.of()).send())
            .add()
            .build();

    invoker.cancel("reserveStock", CTX);
  }

  @Test
  void reserve_httpStatus503_throwsRetryable() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation(
                (http, ctx) ->
                    StepResult.of(http.post("/fail503").jsonBody(Map.of()).send().jsonObject()))
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.reserve("reserveStock", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void reserve_httpStatus422_throwsNonRetryable() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation(
                (http, ctx) ->
                    StepResult.of(http.post("/fail422").jsonBody(Map.of()).send().jsonObject()))
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.reserve("reserveStock", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void reserve_noReserveForOperation_throwsStepExecutionException() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("other")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.reserve("reserveStock", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
  }

  @Test
  void confirm_noConfirmForOperation_throwsStepExecutionException() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("other")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    assertThat(catchThrowable(() -> invoker.confirm("reserveStock", CTX)))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void cancel_noCancelForOperation_throwsStepCompensationException() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("other")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    assertThat(catchThrowable(() -> invoker.cancel("reserveStock", CTX)))
        .isInstanceOf(StepCompensationException.class);
  }

  @Test
  void confirm_httpStatus503_throwsRetryable() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> http.post("/fail503").jsonBody(Map.of()).send())
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.confirm("reserveStock", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void confirm_httpStatus422_throwsNonRetryable() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> http.post("/fail422").jsonBody(Map.of()).send())
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    Throwable t = catchThrowable(() -> invoker.confirm("reserveStock", CTX));

    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void cancel_httpFailure_throwsStepCompensationException() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> http.post("/fail503").jsonBody(Map.of()).send())
            .add()
            .build();

    assertThat(catchThrowable(() -> invoker.cancel("reserveStock", CTX)))
        .isInstanceOf(StepCompensationException.class);
  }

  @Test
  void supportsReserveConfirmCancel_reflectRegistrations() {
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    assertThat(invoker.supportsReserve("reserveStock")).isTrue();
    assertThat(invoker.supportsReserve("missing")).isFalse();
    assertThat(invoker.supportsConfirm("reserveStock")).isTrue();
    assertThat(invoker.supportsConfirm("missing")).isFalse();
    assertThat(invoker.supportsCancel("reserveStock")).isTrue();
    assertThat(invoker.supportsCancel("missing")).isFalse();
  }

  @Test
  void build_onlyTccOperation_succeeds() {
    // A TCC-only invoker (no SAGA execute/compensate) is valid — reserve satisfies the
    // at-least-one-operation invariant.
    HttpServiceInvoker invoker =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {})
            .cancellation((http, ctx) -> {})
            .add()
            .build();

    assertThat(invoker.supportsReserve("reserveStock")).isTrue();
    assertThat(invoker.supportsExecute("reserveStock")).isFalse();
  }

  @Test
  void operationAdd_missingCompensation_throwsIllegalState() {
    HttpServiceInvoker.OperationBuilder op =
        HttpServiceInvoker.newBuilder(baseUrl)
            .operation("debit")
            .execution((http, ctx) -> StepResult.empty());

    assertThat(catchThrowable(op::add)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void tccOperationAdd_missingCancellation_throwsIllegalState() {
    HttpServiceInvoker.TccOperationBuilder op =
        HttpServiceInvoker.newBuilder(baseUrl)
            .tccOperation("reserveStock")
            .reservation((http, ctx) -> StepResult.empty())
            .confirmation((http, ctx) -> {});

    assertThat(catchThrowable(op::add)).isInstanceOf(IllegalStateException.class);
  }

  private HttpServiceInvoker invokerPosting(String operation, String path) {
    return HttpServiceInvoker.newBuilder(baseUrl)
        .operation(operation)
        .execution(
            (http, ctx) -> StepResult.of(http.post(path).jsonBody(Map.of()).send().jsonObject()))
        .compensation((http, ctx) -> {})
        .add()
        .build();
  }

  private static void respond(HttpExchange ex, int status, String retryableHeader, String body)
      throws IOException {
    if (!retryableHeader.isEmpty()) {
      ex.getResponseHeaders().add(HttpHeaders.SAGA_RETRYABLE, retryableHeader);
    }
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  /** Minimal {@link ServiceCallContext} for these tests. */
  private static final class FakeServiceCallContext implements ServiceCallContext {
    private final String sagaId;
    private final String stepName;

    FakeServiceCallContext(String sagaId, String stepName) {
      this.sagaId = sagaId;
      this.stepName = stepName;
    }

    @Override
    public String getStepName() {
      return stepName;
    }

    @Override
    public String getSagaId() {
      return sagaId;
    }

    @Override
    public <T> Optional<T> get(String key, Class<T> type) {
      return Optional.empty();
    }
  }
}
