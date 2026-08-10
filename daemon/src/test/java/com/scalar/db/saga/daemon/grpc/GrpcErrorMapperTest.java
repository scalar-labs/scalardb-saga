package com.scalar.db.saga.daemon.grpc;

import static com.scalar.db.saga.daemon.grpc.ErrorInfos.errorInfo;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
        GrpcErrorMapper.toStatusRuntimeException(new SagaDefinitionNotFoundException("orders"));

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
            new SagaDefinitionNotFoundException("orders", "v2"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    ErrorInfo info = errorInfo(e);
    assertThat(info.getReason()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND.code());
    assertThat(info.getMetadataMap())
        .containsEntry("saga_name", "orders")
        .containsEntry("version", "v2");
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
