package com.scalar.db.saga.daemon.grpc;

import com.scalar.db.saga.daemon.api.InvalidRequestException;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps engine/daemon exceptions to gRPC {@link Status} codes, the gRPC analogue of {@link
 * com.scalar.db.saga.daemon.api.ErrorMapper} (REST). <b>Keep in sync with {@code ErrorMapper}</b> —
 * both translate the same exception hierarchy, so a change to one almost always needs the other.
 *
 * <p><b>Leak discipline.</b> Only daemon-owned messages ever reach a caller. For {@code INTERNAL},
 * {@code UNAVAILABLE}, and the engine-rejected {@code INVALID_ARGUMENT} case, the {@code Status}
 * description is a fixed literal — never the exception's message, never {@code
 * Status.fromThrowable}, never an outbound {@code withCause}. The real exception is logged
 * server-side. {@link InvalidRequestException}'s message is daemon-authored and the one exception
 * message safe to surface (it mirrors the REST {@code 400} body).
 */
final class GrpcErrorMapper {

  private static final Logger logger = LoggerFactory.getLogger(GrpcErrorMapper.class);

  private GrpcErrorMapper() {}

  static StatusRuntimeException toStatusRuntimeException(Throwable t) {
    if (t instanceof SagaNotFoundException e) {
      return status(Status.Code.NOT_FOUND, "No saga found with id '" + e.getSagaId() + "'");
    }
    if (t instanceof SagaDefinitionNotFoundException e) {
      return status(Status.Code.NOT_FOUND, definitionNotFoundMessage(e));
    }
    if (t instanceof InvalidRequestException) {
      // Daemon-authored message — the one exception message safe to surface (mirrors REST 400).
      return status(Status.Code.INVALID_ARGUMENT, t.getMessage());
    }
    if (t instanceof IllegalArgumentException) {
      // A client value the engine rejected; do not echo the engine's wording (mirrors ErrorMapper).
      return status(Status.Code.INVALID_ARGUMENT, "Invalid request parameter");
    }
    if (t instanceof SagaAlreadyExistsException e) {
      // No existing-snapshot detail: the client re-fetches via GetSaga if it needs it.
      return status(
          Status.Code.ALREADY_EXISTS, "A saga already exists with id '" + e.getSagaId() + "'");
    }
    if (t instanceof SagaPersistenceException e) {
      // A transient store failure is retryable (UNAVAILABLE); a permanent one (e.g. a serialization
      // or parse error) is not — surface it as INTERNAL so the client does not retry it futilely.
      if (e.isRetryable()) {
        logger.error("Transient persistence error handling gRPC call", t);
        return status(Status.Code.UNAVAILABLE, "Service temporarily unavailable");
      }
      logger.error("Permanent persistence error handling gRPC call", t);
      return status(Status.Code.INTERNAL, "Internal server error");
    }
    // Catch-all: never leak an unmapped exception's message; log it and return a generic INTERNAL.
    logger.error("Unhandled error handling gRPC call", t);
    return status(Status.Code.INTERNAL, "Internal server error");
  }

  /**
   * The only {@link Status} factory: the description is an explicit daemon-owned message, never
   * derived from a throwable. There is deliberately no throwable-accepting overload, so no caller
   * can accidentally leak an internal message or cause.
   */
  private static StatusRuntimeException status(Status.Code code, @Nullable String fixedMessage) {
    return code.toStatus()
        .withDescription(fixedMessage == null ? "" : fixedMessage)
        .asRuntimeException();
  }

  private static String definitionNotFoundMessage(SagaDefinitionNotFoundException e) {
    String message = "No saga definition registered with name '" + e.getSagaName() + "'";
    String version = e.getVersion();
    return version == null ? message : message + " (version '" + version + "')";
  }
}
