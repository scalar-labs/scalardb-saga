package com.scalar.db.saga.daemon.grpc;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.daemon.api.InvalidRequestException;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps engine/daemon exceptions to gRPC {@link Status} codes, the gRPC analogue of {@link
 * com.scalar.db.saga.daemon.api.ErrorMapper} (REST). <b>Keep in sync with {@code ErrorMapper}</b> —
 * both translate the same exception hierarchy, so a change to one almost always needs the other.
 *
 * <p><b>Machine-readable cause.</b> Where a caller must switch on the reason — the two distinct
 * {@code NOT_FOUND} shapes (missing saga vs unregistered definition) and the wrong-state {@code
 * FAILED_PRECONDITION} — the reason travels as a {@link ErrorInfo} detail (the standard gRPC rich
 * error model), not parsed out of the description. Its {@code reason} token equals the REST {@link
 * com.scalar.db.saga.daemon.api.ErrorMapper} error code, so the two mappers stay in lockstep.
 *
 * <p><b>Leak discipline.</b> Only daemon-owned values ever reach a caller. For {@code INTERNAL},
 * {@code UNAVAILABLE}, and the engine-rejected {@code INVALID_ARGUMENT} case, the {@code Status}
 * description is a fixed literal — never the exception's message, never {@code
 * Status.fromThrowable}, never an outbound {@code withCause}. The real exception is logged
 * server-side. {@link InvalidRequestException}'s message is daemon-authored and the one exception
 * message safe to surface (it mirrors the REST {@code 400} body). An {@link ErrorInfo} detail
 * carries only a daemon-owned reason and explicit identifying metadata (a saga name or version),
 * never the exception's message or cause.
 */
final class GrpcErrorMapper {

  private static final Logger logger = LoggerFactory.getLogger(GrpcErrorMapper.class);

  /** The {@link ErrorInfo} domain scoping every reason token below. */
  private static final String ERROR_DOMAIN = "saga.scalar.com";

  private GrpcErrorMapper() {}

  static StatusRuntimeException toStatusRuntimeException(Throwable t) {
    if (t instanceof SagaNotFoundException e) {
      // NOT_FOUND, but the caller must distinguish this from an unregistered definition below, so
      // the reason rides an ErrorInfo detail (mirrors the REST SAGA_NOT_FOUND error code).
      return statusWithReason(
          Status.Code.NOT_FOUND,
          "No saga found with id '" + e.getSagaId() + "'",
          "SAGA_NOT_FOUND",
          Map.of());
    }
    if (t instanceof SagaDefinitionNotFoundException e) {
      // Same code as a missing saga, distinct reason: the saga exists but its definition is not
      // registered, so the caller must re-register it. The name and version travel as metadata so
      // the client can reconstruct the exception it holds only a saga id for.
      Map<String, String> metadata =
          e.getVersion() == null
              ? Map.of("sagaName", e.getSagaName())
              : Map.of("sagaName", e.getSagaName(), "version", e.getVersion());
      return statusWithReason(
          Status.Code.NOT_FOUND,
          definitionNotFoundMessage(e),
          "SAGA_DEFINITION_NOT_FOUND",
          metadata);
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
    if (t instanceof SagaStatePreconditionException e) {
      // An admin mutation on a saga in the wrong state — a precondition failure, not transient, so
      // FAILED_PRECONDITION (the gRPC analogue of REST 422). The machine-readable code is the
      // contract and rides an ErrorInfo detail (its name matches the REST error code); the
      // description is a daemon-owned literal and the exception's message is not surfaced.
      return statusWithReason(
          Status.Code.FAILED_PRECONDITION,
          "The saga is not in a state that allows this operation",
          e.getCode().name(),
          Map.of());
    }
    if (t instanceof SagaConcurrentModificationException) {
      // A lost compare-and-set — a transient race, so ABORTED (the gRPC analogue of REST 409); a
      // retry may now succeed against the new state.
      return status(Status.Code.ABORTED, "The saga was concurrently modified; retry");
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

  /**
   * Builds a status carrying a machine-readable {@link ErrorInfo} detail so the client can switch
   * on {@code reason} rather than parse the description. Like {@link #status}, the description is
   * an explicit daemon-owned literal; the detail adds only the daemon-owned reason and the given
   * identifying metadata — never a throwable's message or cause.
   */
  private static StatusRuntimeException statusWithReason(
      Status.Code code, String description, String reason, Map<String, String> metadata) {
    ErrorInfo info =
        ErrorInfo.newBuilder()
            .setReason(reason)
            .setDomain(ERROR_DOMAIN)
            .putAllMetadata(metadata)
            .build();
    com.google.rpc.Status status =
        com.google.rpc.Status.newBuilder()
            .setCode(code.value())
            .setMessage(description)
            .addDetails(Any.pack(info))
            .build();
    return StatusProto.toStatusRuntimeException(status);
  }

  private static String definitionNotFoundMessage(SagaDefinitionNotFoundException e) {
    String message = "No saga definition registered with name '" + e.getSagaName() + "'";
    String version = e.getVersion();
    return version == null ? message : message + " (version '" + version + "')";
  }
}
