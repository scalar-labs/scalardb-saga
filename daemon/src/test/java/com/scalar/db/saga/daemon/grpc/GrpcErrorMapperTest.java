package com.scalar.db.saga.daemon.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

/**
 * Unit-tests {@link GrpcErrorMapper}: the server-fault vs client-fault distinction — the exception
 * types whose gRPC code differs — and the machine-readable {@link ErrorInfo} reason it attaches
 * where a caller must switch on the cause. An {@link IllegalStateException} (a server-side
 * invariant violation, e.g. an api/proto enum skew on the response path) must map to {@code
 * INTERNAL}, not the client-facing {@code INVALID_ARGUMENT} that {@link IllegalArgumentException}
 * (an engine-rejected client value) maps to. Either way only a fixed, daemon-owned description is
 * surfaced.
 */
class GrpcErrorMapperTest {

  @Test
  void toStatusRuntimeException_illegalStateGiven_mapsToInternal() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new IllegalStateException("server invariant"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    // The internal message is never surfaced; a fixed daemon-owned description is.
    assertThat(e.getStatus().getDescription()).isEqualTo("Internal server error");
  }

  @Test
  void toStatusRuntimeException_illegalArgumentGiven_mapsToInvalidArgument() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new IllegalArgumentException("bad client value"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(e.getStatus().getDescription()).isEqualTo("Invalid request parameter");
  }

  @Test
  void toStatusRuntimeException_sagaNotFoundGiven_mapsToNotFoundWithReason() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new SagaNotFoundException("s-1"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(errorInfo(e).getReason()).isEqualTo("SAGA_NOT_FOUND");
  }

  @Test
  void toStatusRuntimeException_definitionNotFoundNameOnlyGiven_carriesNameMetadata() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new SagaDefinitionNotFoundException("orders"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    ErrorInfo info = errorInfo(e);
    assertThat(info.getReason()).isEqualTo("SAGA_DEFINITION_NOT_FOUND");
    assertThat(info.getMetadataMap())
        .containsEntry("sagaName", "orders")
        .doesNotContainKey("version");
  }

  @Test
  void toStatusRuntimeException_definitionNotFoundWithVersionGiven_carriesNameAndVersion() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(
            new SagaDefinitionNotFoundException("orders", "v2"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    ErrorInfo info = errorInfo(e);
    assertThat(info.getReason()).isEqualTo("SAGA_DEFINITION_NOT_FOUND");
    assertThat(info.getMetadataMap())
        .containsEntry("sagaName", "orders")
        .containsEntry("version", "v2");
  }

  @Test
  void toStatusRuntimeException_preconditionGiven_carriesCodeAsReasonNotDescription() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(
            new SagaStatePreconditionException(
                "s-1", SagaStatePreconditionException.Code.SAGA_WRONG_STATE, "wrong state"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    // The code moved off the description onto the reason; the description is a daemon-owned
    // literal.
    assertThat(e.getStatus().getDescription())
        .isEqualTo("The saga is not in a state that allows this operation");
    assertThat(errorInfo(e).getReason()).isEqualTo("SAGA_WRONG_STATE");
  }

  /** Extracts the {@link ErrorInfo} detail, failing the test if the status carries none. */
  private static ErrorInfo errorInfo(StatusRuntimeException e) {
    com.google.rpc.Status status = StatusProto.fromThrowable(e);
    if (status == null) {
      throw new AssertionError("status carried no google.rpc.Status details");
    }
    for (Any detail : status.getDetailsList()) {
      if (detail.is(ErrorInfo.class)) {
        try {
          return detail.unpack(ErrorInfo.class);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
          throw new AssertionError("malformed ErrorInfo detail", malformed);
        }
      }
    }
    throw new AssertionError("status carried no ErrorInfo detail");
  }
}
