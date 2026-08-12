package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.server.security.SagaAuthUnavailableException;
import com.scalar.db.saga.server.security.SagaAuthenticationException;
import com.scalar.db.saga.server.security.SagaAuthorizationException;
import com.scalar.db.saga.server.security.SagaRole;
import io.javalin.Javalin;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.NotFoundResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Locks in {@link ErrorMapper}'s exception-resolution behavior against a real Javalin dispatch.
 *
 * <p>The catch-all {@code Exception} handler must <b>not</b> swallow Javalin's built-in {@link
 * io.javalin.http.HttpResponseException}s (e.g. {@link BadRequestResponse}). In Javalin 6.x {@code
 * ExceptionMapper} pre-registers a handler for {@code HttpResponseException} and resolves the
 * nearest handler up the class hierarchy, so {@code HttpResponseException} (1 hop) wins over {@code
 * Exception} (3 hops) and the intended status is preserved. A Javalin upgrade that changed that
 * resolution order — silently mapping client errors to {@code 500} — would fail this test.
 *
 * <p>Binds an ephemeral port (no ScalarDB, no {@code SagaServer}); it exercises only {@code
 * ErrorMapper} on a bare app.
 */
class ErrorMapperTest {

  private final HttpClient http = HttpClient.newHttpClient();
  private Javalin app;

  // What the /throw-dispatch route throws; set per golden-table row. Initialized to a placeholder
  // so the field is never null.
  private RuntimeException toThrow = new IllegalStateException("no case set");

  @BeforeEach
  void setUp() {
    app = Javalin.create();
    ErrorMapper.register(app);
    app.get(
        "/bad-request",
        ctx -> {
          throw new BadRequestResponse();
        });
    app.get(
        "/boom",
        ctx -> {
          throw new RuntimeException("internal detail");
        });
    app.get(
        "/auth-unavailable",
        ctx -> {
          throw new SagaAuthUnavailableException("jwks unreachable", new RuntimeException());
        });
    app.get(
        "/persist-transient",
        ctx -> {
          throw SagaPersistenceException.storeUnavailable(
              new RuntimeException("db down on secret_table"));
        });
    app.get(
        "/persist-permanent",
        ctx -> {
          throw SagaPersistenceException.serializationFailed(
              new RuntimeException("bad json for secret_table"));
        });
    app.get(
        "/not-found-typed",
        ctx -> {
          throw new SagaNotFoundException("s-404");
        });
    app.get(
        "/throw-dispatch",
        ctx -> {
          throw toThrow;
        });
    app.start(0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void httpResponseException_keepsItsStatus_notSwallowedByCatchAll() throws Exception {
    HttpResponse<String> response = get("/bad-request");
    assertThat(response.statusCode()).isEqualTo(400);
  }

  @Test
  void uncaughtException_mapsToGeneric500_withoutLeakingMessage() throws Exception {
    HttpResponse<String> response = get("/boom");
    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body())
        .contains(SagaErrorCode.INTERNAL_ERROR.code())
        .doesNotContain("internal detail");
  }

  @Test
  void authProviderUnavailable_mapsTo503_withoutLeakingMessage() throws Exception {
    HttpResponse<String> response = get("/auth-unavailable");
    assertThat(response.statusCode()).isEqualTo(503);
    assertThat(response.body())
        .contains(SagaErrorCode.SERVICE_UNAVAILABLE.code())
        .doesNotContain("jwks unreachable");
  }

  @Test
  void retryablePersistenceError_mapsTo503_withoutLeakingMessage() throws Exception {
    HttpResponse<String> response = get("/persist-transient");
    assertThat(response.statusCode()).isEqualTo(503);
    assertThat(response.body())
        .contains(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE.code())
        .doesNotContain("secret_table");
  }

  @Test
  void permanentPersistenceError_mapsTo500_withoutLeakingMessage() throws Exception {
    // A permanent persistence failure must not be a retryable 503 — the client would retry it
    // futilely. It maps to a generic 500 instead, still without leaking the internal message.
    HttpResponse<String> response = get("/persist-permanent");
    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body())
        .contains(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED.code())
        .doesNotContain("secret_table");
  }

  @Test
  void unknownRoute_mapsTo404_withStructuredBody() throws Exception {
    // Javalin reports an unroutable request as an internal NotFoundResponse; the registered
    // handler for that exact type must compose the structured body, or this would be the one REST
    // response without an errorCode. ENDPOINT_NOT_FOUND, not INVALID_REQUEST: the sub-range is a
    // wire contract, so the 404 status must carry a not-found-family (102xx) code.
    HttpResponse<String> response = get("/no-such-route");
    assertThat(response.statusCode()).isEqualTo(404);
    assertThat(response.body())
        .contains(SagaErrorCode.ENDPOINT_NOT_FOUND.code())
        .contains("/no-such-route");
  }

  @Test
  void unknownRoute_veryLongPathGiven_capsTheEchoedDetail() throws Exception {
    // The unmatched-route echo is the one response an anonymous caller can shape without matching
    // any route (auth and rate limiting never run for it), so its length must be bounded by the
    // handler, not only by Jetty's request-line limit.
    HttpResponse<String> response = get("/no-such-route/" + "a".repeat(300));
    assertThat(response.statusCode()).isEqualTo(404);
    assertThat(response.body())
        .contains(SagaErrorCode.ENDPOINT_NOT_FOUND.code())
        .doesNotContain("a".repeat(250));
  }

  @Test
  void handlerProduced404_keepsItsTypedBody_notAffectedByNotFoundRouteMapping() throws Exception {
    // A typed not-found from a handler must keep its own code; the NotFoundResponse entry covers
    // only Javalin's unmatched-route signal, not every 404.
    HttpResponse<String> response = get("/not-found-typed");
    assertThat(response.statusCode()).isEqualTo(404);
    assertThat(response.body())
        .contains(SagaErrorCode.SAGA_NOT_FOUND.code())
        .doesNotContain(SagaErrorCode.ENDPOINT_NOT_FOUND.code());
  }

  /** One dispatch entry per row: the thrown exception, the HTTP status, and the body's code. */
  private record Arm(RuntimeException thrown, int status, SagaErrorCode code) {}

  /**
   * The golden table: every per-type handler and both branches where one branches (persistence
   * retryable vs not, definition bad-request vs conflict), mirroring {@code GrpcErrorMapperTest}'s
   * table so the two mappers cannot drift apart silently. A new handler without a row here — or a
   * row without a handler — is a conscious edit to this list, not a silent gap.
   */
  private static List<Arm> allArms() {
    SagaStateSnapshot existing =
        new SagaStateSnapshot(
            "s-1",
            "transfer",
            SagaStatus.RUNNING,
            "owner-1",
            "v1",
            Instant.ofEpochSecond(1_700_000_000L),
            Instant.ofEpochSecond(1_700_000_000L));
    return List.of(
        new Arm(new IllegalArgumentException("bad"), 400, SagaErrorCode.INVALID_ARGUMENT),
        new Arm(new SagaInvalidRequestException("x"), 400, SagaErrorCode.INVALID_REQUEST),
        new Arm(new SagaIllegalArgumentException("x"), 400, SagaErrorCode.INVALID_ARGUMENT),
        new Arm(
            SagaDefinitionException.definitionInvalid("transfer", "dup step"),
            400,
            SagaErrorCode.INVALID_DEFINITION),
        new Arm(
            SagaDefinitionException.declarativeStepInvalid("debit", "missing 'path'"),
            400,
            SagaErrorCode.INVALID_STEP_DEFINITION),
        new Arm(
            SagaDefinitionException.definitionMalformed("json", new RuntimeException()),
            400,
            SagaErrorCode.MALFORMED_DEFINITION),
        new Arm(
            SagaDefinitionException.sourceUnreadable("x.json"),
            400,
            SagaErrorCode.UNREADABLE_DEFINITION_SOURCE),
        new Arm(
            SagaDefinitionException.stepClassInvalid("com.example.C", "not a Step"),
            400,
            SagaErrorCode.INVALID_STEP_CLASS),
        new Arm(
            SagaDefinitionException.stepClassNotSupportedOnServer("transfer", "debit"),
            400,
            SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER),
        new Arm(
            SagaDefinitionException.httpEndpointLookupFailed("none registered"),
            400,
            SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED),
        new Arm(
            SagaDefinitionException.versionContentConflict("transfer", "v2"),
            409,
            SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT),
        new Arm(new SagaNotFoundException("s-1"), 404, SagaErrorCode.SAGA_NOT_FOUND),
        new Arm(
            SagaDefinitionNotFoundException.byName("transfer"),
            404,
            SagaErrorCode.SAGA_DEFINITION_NOT_FOUND),
        new Arm(
            SagaDefinitionNotFoundException.byNameAndVersion("transfer", "v2"),
            404,
            SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND),
        // Javalin's unmatched-route signal; its handler must answer with a not-found-family code,
        // and this row is what puts the pairing under the sub-range guard below.
        new Arm(new NotFoundResponse(), 404, SagaErrorCode.ENDPOINT_NOT_FOUND),
        new Arm(new CallbackAuthException("bad token"), 401, SagaErrorCode.UNAUTHENTICATED),
        new Arm(
            new SagaAuthenticationException("missing credential"),
            401,
            SagaErrorCode.UNAUTHENTICATED),
        new Arm(
            new SagaAuthorizationException("caller-1", SagaRole.ADMIN),
            403,
            SagaErrorCode.PERMISSION_DENIED),
        new Arm(
            new RateLimitExceededException("over limit", 1_000L),
            429,
            SagaErrorCode.RATE_LIMIT_EXCEEDED),
        new Arm(
            new SagaAuthUnavailableException("jwks unreachable", new RuntimeException()),
            503,
            SagaErrorCode.SERVICE_UNAVAILABLE),
        new Arm(
            new SagaAlreadyExistsException("s-1", existing),
            409,
            SagaErrorCode.SAGA_ALREADY_EXISTS),
        new Arm(
            new SagaConcurrentModificationException("s-1"),
            409,
            SagaErrorCode.SAGA_CONCURRENT_MODIFICATION),
        new Arm(
            SagaStatePreconditionException.wrongState("s-1", "RUNNING", "recover"),
            422,
            SagaErrorCode.SAGA_WRONG_STATE),
        new Arm(SagaStatePreconditionException.parked("s-1"), 422, SagaErrorCode.SAGA_PARKED),
        new Arm(
            SagaPersistenceException.storeUnavailable(new RuntimeException("db down")),
            503,
            SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE),
        new Arm(
            SagaPersistenceException.serializationFailed(new RuntimeException("bad json")),
            500,
            SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED),
        new Arm(
            SagaPersistenceException.deserializationFailed(new RuntimeException("schema drift")),
            500,
            SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED),
        // The category fallback for an unmapped SagaRuntimeException subclass.
        new Arm(
            new SagaRuntimeException(SagaErrorCode.RATE_LIMIT_EXCEEDED, ErrorMetadata.of()),
            503,
            SagaErrorCode.RATE_LIMIT_EXCEEDED),
        // The non-Saga catch-all.
        new Arm(new IllegalStateException("boom"), 500, SagaErrorCode.INTERNAL_ERROR));
  }

  @Test
  void register_everyArm_mapsToItsStatusAndBodyCode() throws Exception {
    for (Arm arm : allArms()) {
      toThrow = arm.thrown();

      HttpResponse<String> response = get("/throw-dispatch");

      assertThat(response.statusCode())
          .as("status of %s", arm.thrown().getClass().getSimpleName())
          .isEqualTo(arm.status());
      assertThat(response.body())
          .as("body code of %s", arm.thrown().getClass().getSimpleName())
          .contains(arm.code().code());
    }
  }

  @Test
  void register_userErrorCodes_statusMatchesTheSubRange() {
    // The class javadoc of SagaErrorCode pins USER_ERROR sub-ranges by client-facing consequence
    // (100xx bad request, 101xx auth, 102xx not-found, 103xx conflict, 104xx precondition), and
    // the docs generator files codes by it. This guards the sub-range digits against what this
    // mapper actually answers, so a code cannot sit in the conflict range while the daemon says
    // 400 — which is exactly how SAGA_DEFINITION_VERSION_CONTENT_CONFLICT shipped until its handler
    // branched on the code.
    Map<String, List<Integer>> statusesBySubRange =
        Map.of(
            "100", List.of(400),
            "101", List.of(401, 403),
            "102", List.of(404),
            "103", List.of(409),
            "104", List.of(422));
    for (Arm arm : allArms()) {
      if (arm.code().category() != SagaErrorCode.Category.USER_ERROR) {
        continue;
      }
      String subRange = arm.code().code().substring("DB-SAGA-".length(), "DB-SAGA-".length() + 3);

      assertThat(arm.status())
          .as("%s is numbered in sub-range %sxx", arm.code().name(), subRange)
          .isIn(statusesBySubRange.get(subRange));
    }
  }

  @Test
  void allArms_coverEveryServerProducibleCode() {
    // Codes deliberately absent from the dispatch table; every entry says why. A new enum
    // constant that lands in neither place fails here, so a forgotten mapper handler is a build
    // failure instead of a silent category-fallback response contradicting its own sub-range.
    EnumSet<SagaErrorCode> excluded =
        EnumSet.of(
            // Produced only by the client SDK; this server never emits them.
            SagaErrorCode.SERVER_UNREACHABLE,
            SagaErrorCode.REQUEST_TIMEOUT,
            SagaErrorCode.REQUEST_ABORTED,
            SagaErrorCode.UNRECOGNIZED_SERVER_ERROR,
            SagaErrorCode.UNRECOGNIZED_RETRYABLE_SERVER_ERROR,
            // Reserved, produced nowhere yet (see SagaErrorCode).
            SagaErrorCode.STEP_TIMEOUT,
            SagaErrorCode.STEP_USER_FAILURE,
            // Recorded into saga state as a timeline event, never a top-level error response.
            SagaErrorCode.COMPENSATION_FAILED);

    Set<SagaErrorCode> covered =
        allArms().stream()
            .map(Arm::code)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(SagaErrorCode.class)));

    assertThat(covered)
        .as("every SagaErrorCode needs a dispatch-table row or a commented exclusion above")
        .isEqualTo(EnumSet.complementOf(excluded));
  }

  private HttpResponse<String> get(String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path)).GET().build(),
        BodyHandlers.ofString());
  }
}
