package com.scalar.db.saga.daemon.grpc;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.daemon.api.InvalidRequestException;
import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPermissionDeniedException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps engine/daemon exceptions to gRPC {@link Status} codes, the gRPC analogue of {@link
 * com.scalar.db.saga.daemon.api.ErrorMapper} (REST). <b>Keep in sync with {@code ErrorMapper}</b> —
 * both translate the same exception hierarchy through the same {@link SagaErrorCode} vocabulary.
 *
 * <p><b>{@link SagaErrorCode} is the sole wire-facing source of truth.</b> Every response — engine
 * exceptions, daemon-only exceptions, catch-all — is composed from a code + its schema-validated
 * metadata via one path: {@code Status.description = code.buildMessage(metadata)}; {@code
 * ErrorInfo.reason = code.code()}; {@code ErrorInfo.metadata = metadata}. The mapper carries no
 * hand-authored code tokens or message strings.
 *
 * <p>Per-type handlers exist only to (a) pick the {@link Status.Code} per exception type and (b)
 * log at the right severity for server-side faults; the body composition is uniform.
 */
final class GrpcErrorMapper {

  private static final Logger logger = LoggerFactory.getLogger(GrpcErrorMapper.class);

  /** The {@link ErrorInfo} domain scoping every reason. */
  private static final String ERROR_DOMAIN = "saga.scalar.com";

  private GrpcErrorMapper() {}

  static StatusRuntimeException toStatusRuntimeException(Throwable t) {
    if (t instanceof InvalidRequestException) {
      // Daemon-authored specifics ride in `detail`; the code is the wire discriminator.
      return status(
          Status.Code.INVALID_ARGUMENT,
          SagaErrorCode.INVALID_REQUEST,
          ErrorMetadata.of("detail", nullToEmpty(t.getMessage())));
    }
    if (t instanceof IllegalArgumentException) {
      // A client value the engine rejected — a fixed daemon-owned detail (no engine wording).
      return status(
          Status.Code.INVALID_ARGUMENT,
          SagaErrorCode.INVALID_REQUEST,
          ErrorMetadata.of("detail", "invalid request parameter"));
    }
    if (t instanceof SagaRuntimeException e) {
      Status.Code code = statusForType(e);
      if (code == Status.Code.INTERNAL || code == Status.Code.UNAVAILABLE) {
        logger.error("Server-side error handling gRPC call", e);
      }
      return status(code, e.getErrorCode(), e.getMetadata());
    }
    // Catch-all: never leak an unmapped exception; log it and surface the enum's generic INTERNAL.
    logger.error("Unhandled error handling gRPC call", t);
    return status(Status.Code.INTERNAL, SagaErrorCode.INTERNAL_ERROR, ErrorMetadata.of());
  }

  /**
   * The per-type gRPC status. Types not listed fall back to the {@link SagaErrorCode.Category}
   * mapping so a newly-added subclass with a code still lands in a sensible bucket.
   */
  private static Status.Code statusForType(SagaRuntimeException e) {
    if (e instanceof SagaNotFoundException || e instanceof SagaDefinitionNotFoundException) {
      return Status.Code.NOT_FOUND;
    }
    if (e instanceof SagaAlreadyExistsException) {
      return Status.Code.ALREADY_EXISTS;
    }
    if (e instanceof SagaConcurrentModificationException) {
      return Status.Code.ABORTED;
    }
    if (e instanceof SagaStatePreconditionException) {
      return Status.Code.FAILED_PRECONDITION;
    }
    if (e instanceof SagaDefinitionException) {
      return Status.Code.INVALID_ARGUMENT;
    }
    if (e instanceof SagaTimeoutException) {
      return Status.Code.DEADLINE_EXCEEDED;
    }
    if (e instanceof SagaUnauthenticatedException) {
      return Status.Code.UNAUTHENTICATED;
    }
    if (e instanceof SagaPermissionDeniedException) {
      return Status.Code.PERMISSION_DENIED;
    }
    if (e instanceof SagaUnavailableException) {
      return Status.Code.UNAVAILABLE;
    }
    if (e instanceof SagaPersistenceException pe) {
      return pe.isRetryable() ? Status.Code.UNAVAILABLE : Status.Code.INTERNAL;
    }
    return switch (e.getErrorCode().category()) {
      case USER_ERROR -> Status.Code.INVALID_ARGUMENT;
      case RETRYABLE_SERVER_ERROR -> Status.Code.UNAVAILABLE;
      case NON_RETRYABLE_SERVER_ERROR, CLIENT_ERROR -> Status.Code.INTERNAL;
    };
  }

  /**
   * The one body-composition path: {@code Status.description = code.buildMessage(metadata)}; {@code
   * ErrorInfo.reason = code.code()}; {@code ErrorInfo.metadata = metadata}.
   */
  private static StatusRuntimeException status(
      Status.Code statusCode, SagaErrorCode code, Map<String, String> metadata) {
    ErrorInfo info =
        ErrorInfo.newBuilder()
            .setReason(code.code())
            .setDomain(ERROR_DOMAIN)
            .putAllMetadata(metadata)
            .build();
    com.google.rpc.Status status =
        com.google.rpc.Status.newBuilder()
            .setCode(statusCode.value())
            .setMessage(code.buildMessage(metadata))
            .addDetails(Any.pack(info))
            .build();
    return StatusProto.toStatusRuntimeException(status);
  }

  private static String nullToEmpty(@org.jspecify.annotations.Nullable String s) {
    return s == null ? "" : s;
  }
}
