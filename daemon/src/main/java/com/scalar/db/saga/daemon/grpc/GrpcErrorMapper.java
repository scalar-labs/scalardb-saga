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
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
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
 * <p><b>{@link SagaErrorCode} is the sole wire-facing source of truth.</b> Every response is
 * composed from a code + its schema-validated metadata via one path: {@code Status.description =
 * code.buildMessage(metadata)}; {@code ErrorInfo.reason = code.code()}; {@code ErrorInfo.metadata =
 * metadata}. Log messages derive from the same {@code e.getMessage()} so log and wire never drift.
 *
 * <p><b>Per-type explicit dispatch.</b> Every wire-facing exception has its own {@code instanceof}
 * branch; the generic {@link SagaRuntimeException} branch catches any future subclass so the
 * fallback route is safe. Auth-family exceptions ({@code SagaAuthenticationException}, {@code
 * SagaAuthorizationException}, {@code SagaAuthUnavailableException}) reach the gRPC layer only
 * through {@link SagaSecurityInterceptor} — they don't flow through this mapper.
 */
final class GrpcErrorMapper {

  private static final Logger logger = LoggerFactory.getLogger(GrpcErrorMapper.class);

  /** The {@link ErrorInfo} domain scoping every reason. */
  private static final String ERROR_DOMAIN = "saga.scalar.com";

  private GrpcErrorMapper() {}

  static StatusRuntimeException toStatusRuntimeException(Throwable t) {
    return switch (t) {
      // A client-supplied value the engine rejected surfaces as IllegalArgumentException — a stdlib
      // exception. Wrap it in InvalidRequestException with a fixed daemon-owned detail (do not
      // echo the engine's wording) so it flows through the same code path as every other case.
      case IllegalArgumentException iae ->
          respond(
              Status.Code.INVALID_ARGUMENT,
              new InvalidRequestException("invalid request parameter"));

      // ── Not found ──────────────────────────────────────────────────
      case SagaNotFoundException e -> respond(Status.Code.NOT_FOUND, e);
      case SagaDefinitionNotFoundException e -> respond(Status.Code.NOT_FOUND, e);

      // ── Conflict ───────────────────────────────────────────────────
      case SagaAlreadyExistsException e -> respond(Status.Code.ALREADY_EXISTS, e);
      case SagaConcurrentModificationException e -> respond(Status.Code.ABORTED, e);

      // ── Precondition ───────────────────────────────────────────────
      case SagaStatePreconditionException e -> respond(Status.Code.FAILED_PRECONDITION, e);

      // ── Bad request ────────────────────────────────────────────────
      case SagaDefinitionException e -> respond(Status.Code.INVALID_ARGUMENT, e);
      case InvalidRequestException e -> respond(Status.Code.INVALID_ARGUMENT, e);

      // ── Server errors ──────────────────────────────────────────────
      case SagaPersistenceException pe -> {
        Status.Code code = pe.isRetryable() ? Status.Code.UNAVAILABLE : Status.Code.INTERNAL;
        logger.error("{} handling gRPC call", pe.getMessage(), pe);
        yield respond(code, pe);
      }

      // ── Fallback for any unmapped SagaRuntimeException subclass ────
      case SagaRuntimeException e -> {
        Status.Code code = statusForCategory(e.getErrorCode().category());
        if (code == Status.Code.INTERNAL || code == Status.Code.UNAVAILABLE) {
          logger.error("{} handling gRPC call", e.getMessage(), e);
        }
        yield respond(code, e);
      }

      // ── Non-Saga catch-all ─────────────────────────────────────────
      default -> {
        logger.error("Unhandled error handling gRPC call", t);
        yield status(Status.Code.INTERNAL, SagaErrorCode.INTERNAL_ERROR, ErrorMetadata.of());
      }
    };
  }

  /** Builds a status carrying an ErrorInfo detail for a {@link SagaRuntimeException}. */
  private static StatusRuntimeException respond(Status.Code statusCode, SagaRuntimeException e) {
    return status(statusCode, e.getErrorCode(), e.getMetadata());
  }

  /**
   * Category-based gRPC status — used only by the {@link SagaRuntimeException} fallback for an
   * unmapped subclass, so a newly-added exception still lands in a sensible bucket.
   */
  private static Status.Code statusForCategory(SagaErrorCode.Category c) {
    return switch (c) {
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
}
