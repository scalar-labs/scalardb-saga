package com.scalar.db.saga.server.grpc;

import static com.scalar.db.saga.server.grpc.ErrorInfos.errorInfo;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.rpc.ErrorInfo;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Unit-tests {@link GrpcErrorMapper}: the per-type gRPC status table, and the generic {@link
 * ErrorInfo} body carrying the {@link SagaErrorCode#code()} as {@code reason} and the exception's
 * schema metadata as {@code metadata}. Leak-sensitive statuses ({@code INTERNAL}, {@code
 * UNAVAILABLE}) surface only a fixed daemon-owned description.
 */
class GrpcErrorMapperTest {

  @Test
  void toStatusRuntimeException_illegalStateGiven_mapsToInternalWithInternalErrorCode() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new IllegalStateException("server invariant"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    // The internal message is never surfaced; the enum's fixed message is.
    assertThat(e.getStatus().getDescription())
        .contains(SagaErrorCode.INTERNAL_ERROR.code())
        .doesNotContain("server invariant");
    assertThat(errorInfo(e).getReason()).isEqualTo(SagaErrorCode.INTERNAL_ERROR.code());
  }

  @Test
  void toStatusRuntimeException_illegalArgumentGiven_mapsToArgumentInvalid() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new IllegalArgumentException("bad client value"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    // Engine's wording is not echoed; a fixed daemon-owned detail is.
    assertThat(e.getStatus().getDescription())
        .contains(SagaErrorCode.INVALID_ARGUMENT.code())
        .doesNotContain("bad client value");
    ErrorInfo info = errorInfo(e);
    // INVALID_ARGUMENT, not INVALID_REQUEST: the request message was well-formed; a value inside
    // it was rejected. INVALID_REQUEST is reserved for the message itself failing validation.
    assertThat(info.getReason()).isEqualTo(SagaErrorCode.INVALID_ARGUMENT.code());
    assertThat(info.getMetadataMap()).containsEntry("detail", "invalid request parameter");
  }

  @Test
  void toStatusRuntimeException_sagaNotFoundGiven_mapsToNotFoundWithCodeReason() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new SagaNotFoundException("s-1"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    ErrorInfo info = errorInfo(e);
    assertThat(info.getReason()).isEqualTo(SagaErrorCode.SAGA_NOT_FOUND.code());
    assertThat(info.getMetadataMap()).containsEntry("saga_id", "s-1");
  }

  @Test
  void toStatusRuntimeException_definitionNotFoundNameOnlyGiven_carriesSagaNameMetadata() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(SagaDefinitionNotFoundException.byName("orders"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    ErrorInfo info = errorInfo(e);
    assertThat(info.getReason()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_NOT_FOUND.code());
    assertThat(info.getMetadataMap())
        .containsEntry("saga_name", "orders")
        .doesNotContainKey("version");
  }

  @Test
  void toStatusRuntimeException_definitionNotFoundWithVersionGiven_carriesNameAndVersion() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(
            SagaDefinitionNotFoundException.byNameAndVersion("orders", "v2"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    ErrorInfo info = errorInfo(e);
    assertThat(info.getReason()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND.code());
    assertThat(info.getMetadataMap())
        .containsEntry("saga_name", "orders")
        .containsEntry("version", "v2");
  }

  /** One dispatch arm per row: the thrown exception, the gRPC status, and the ErrorInfo reason. */
  private record Arm(Throwable thrown, Status.Code status, SagaErrorCode code) {}

  /**
   * The golden table: every arm of the dispatch switch, both branches where an arm branches
   * (persistence retryable vs not, definition bad-request vs conflict). A new arm without a row
   * here — or a row without an arm — is a conscious edit to this list, not a silent gap.
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
        new Arm(
            new IllegalArgumentException("bad"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.INVALID_ARGUMENT),
        new Arm(
            new SagaInvalidRequestException("x"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.INVALID_REQUEST),
        new Arm(
            new SagaIllegalArgumentException("x"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.INVALID_ARGUMENT),
        new Arm(
            SagaDefinitionException.definitionInvalid("transfer", "dup step"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.INVALID_DEFINITION),
        new Arm(
            SagaDefinitionException.declarativeStepInvalid("debit", "missing 'path'"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.INVALID_STEP_DEFINITION),
        new Arm(
            SagaDefinitionException.definitionMalformed("json", new RuntimeException()),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.MALFORMED_DEFINITION),
        new Arm(
            SagaDefinitionException.sourceUnreadable("x.json"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.UNREADABLE_DEFINITION_SOURCE),
        new Arm(
            SagaDefinitionException.stepClassInvalid("com.example.C", "not a Step"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.INVALID_STEP_CLASS),
        new Arm(
            SagaDefinitionException.stepClassNotSupportedOnServer("transfer", "debit"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER),
        new Arm(
            SagaDefinitionException.httpEndpointLookupFailed("none registered"),
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED),
        new Arm(
            SagaDefinitionException.versionContentConflict("transfer", "v2"),
            Status.Code.ALREADY_EXISTS,
            SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT),
        new Arm(
            new SagaNotFoundException("s-1"), Status.Code.NOT_FOUND, SagaErrorCode.SAGA_NOT_FOUND),
        new Arm(
            SagaDefinitionNotFoundException.byName("transfer"),
            Status.Code.NOT_FOUND,
            SagaErrorCode.SAGA_DEFINITION_NOT_FOUND),
        new Arm(
            SagaDefinitionNotFoundException.byNameAndVersion("transfer", "v2"),
            Status.Code.NOT_FOUND,
            SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND),
        new Arm(
            new SagaAlreadyExistsException("s-1", existing),
            Status.Code.ALREADY_EXISTS,
            SagaErrorCode.SAGA_ALREADY_EXISTS),
        new Arm(
            new SagaConcurrentModificationException("s-1"),
            Status.Code.ABORTED,
            SagaErrorCode.SAGA_CONCURRENT_MODIFICATION),
        new Arm(
            SagaStatePreconditionException.wrongState("s-1", "RUNNING", "recover"),
            Status.Code.FAILED_PRECONDITION,
            SagaErrorCode.SAGA_WRONG_STATE),
        new Arm(
            SagaStatePreconditionException.parked("s-1"),
            Status.Code.FAILED_PRECONDITION,
            SagaErrorCode.SAGA_PARKED),
        new Arm(
            SagaPersistenceException.storeUnavailable(new RuntimeException("db down")),
            Status.Code.UNAVAILABLE,
            SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE),
        new Arm(
            SagaPersistenceException.serializationFailed(new RuntimeException("bad json")),
            Status.Code.INTERNAL,
            SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED),
        new Arm(
            SagaPersistenceException.deserializationFailed(new RuntimeException("schema drift")),
            Status.Code.INTERNAL,
            SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED),
        // The category fallback for an unmapped SagaRuntimeException subclass.
        new Arm(
            new SagaRuntimeException(SagaErrorCode.RATE_LIMIT_EXCEEDED, ErrorMetadata.of()),
            Status.Code.UNAVAILABLE,
            SagaErrorCode.RATE_LIMIT_EXCEEDED),
        // The non-Saga catch-all.
        new Arm(
            new IllegalStateException("boom"), Status.Code.INTERNAL, SagaErrorCode.INTERNAL_ERROR));
  }

  @Test
  void toStatusRuntimeException_everyArm_mapsToItsStatusAndReason() {
    for (Arm arm : allArms()) {
      StatusRuntimeException e = GrpcErrorMapper.toStatusRuntimeException(arm.thrown());

      assertThat(e.getStatus().getCode())
          .as("status of %s", arm.thrown().getClass().getSimpleName())
          .isEqualTo(arm.status());
      assertThat(errorInfo(e).getReason())
          .as("reason of %s", arm.thrown().getClass().getSimpleName())
          .isEqualTo(arm.code().code());
    }
  }

  @Test
  void toStatusRuntimeException_userErrorCodes_statusMatchesTheSubRange() {
    // The class javadoc of SagaErrorCode pins USER_ERROR sub-ranges by client-facing consequence
    // (100xx bad request, 101xx auth, 102xx not-found, 103xx conflict, 104xx precondition), and
    // the docs generator files codes by it. SagaErrorCodeTest guards digit 1 (the category); this
    // guards the next two against what this mapper actually answers, so a code cannot sit in the
    // conflict range while the daemon says INVALID_ARGUMENT — which is exactly how
    // SAGA_DEFINITION_VERSION_CONTENT_CONFLICT shipped 400 until the guarded dispatch arm was
    // added.
    Map<String, List<Status.Code>> statusesBySubRange =
        Map.of(
            "100", List.of(Status.Code.INVALID_ARGUMENT),
            "101", List.of(Status.Code.UNAUTHENTICATED, Status.Code.PERMISSION_DENIED),
            "102", List.of(Status.Code.NOT_FOUND),
            "103", List.of(Status.Code.ALREADY_EXISTS),
            "104", List.of(Status.Code.FAILED_PRECONDITION));
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
  void allArms_coverEveryMapperProducibleCode() {
    // Codes deliberately absent from the dispatch table; every entry says why. A new enum
    // constant that lands in neither place fails here, so a forgotten mapper arm is a build
    // failure instead of a silent category-fallback response contradicting its own sub-range.
    EnumSet<SagaErrorCode> excluded =
        EnumSet.of(
            // Emitted on gRPC by the interceptors through GrpcErrorMapper.close, never by this
            // dispatch; the interceptor tests pin their status and reason.
            SagaErrorCode.UNAUTHENTICATED,
            SagaErrorCode.PERMISSION_DENIED,
            SagaErrorCode.SERVICE_UNAVAILABLE,
            // REST-only: the unmatched-route 404 body.
            SagaErrorCode.ENDPOINT_NOT_FOUND,
            // Produced only by the client SDK; this server never emits them.
            SagaErrorCode.SERVER_UNREACHABLE,
            SagaErrorCode.REQUEST_TIMEOUT,
            SagaErrorCode.REQUEST_ABORTED,
            SagaErrorCode.UNRECOGNIZED_SERVER_ERROR,
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

  @Test
  void toStatusRuntimeException_preconditionGiven_carriesCodeAsReasonNotDescription() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(
            SagaStatePreconditionException.wrongState("s-1", "RUNNING", "recover"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    ErrorInfo info = errorInfo(e);
    assertThat(info.getReason()).isEqualTo(SagaErrorCode.SAGA_WRONG_STATE.code());
    assertThat(info.getMetadataMap())
        .containsEntry("saga_id", "s-1")
        .containsEntry("current_state", "RUNNING")
        .containsEntry("requested_operation", "recover");
  }
}
