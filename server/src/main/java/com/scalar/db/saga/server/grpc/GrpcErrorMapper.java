package com.scalar.db.saga.server.grpc;

import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.rpc.ErrorInfo;
import com.google.rpc.RetryInfo;
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
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.EnumMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps engine/daemon exceptions to gRPC {@link Status} codes, the gRPC analogue of {@link
 * com.scalar.db.saga.server.api.ErrorMapper} (REST). <b>Keep in sync with {@code ErrorMapper}</b> —
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
 * through {@link SagaSecurityInterceptor} — they don't flow through this mapper's exception
 * dispatch; the interceptors compose their refusals through {@link #close} instead, so those
 * responses still carry an {@link ErrorInfo} from the same vocabulary.
 */
final class GrpcErrorMapper {

  private static final Logger logger = LoggerFactory.getLogger(GrpcErrorMapper.class);

  private GrpcErrorMapper() {}

  static StatusRuntimeException toStatusRuntimeException(Throwable t) {
    return switch (t) {
      // A client-supplied value the engine rejected surfaces as a stdlib IllegalArgumentException
      // from the engine sites not yet migrated to SagaIllegalArgumentException. Wrap it in the
      // latter with a fixed daemon-owned detail (do not echo the engine's wording) so it flows
      // through the same code path as every other case. INVALID_ARGUMENT, not INVALID_REQUEST: the
      // request message was well-formed; a value inside it was rejected.
      case IllegalArgumentException iae -> {
        // The engine's wording and cause are replaced on the wire, so log them: a server-side bug
        // surfacing as IllegalArgumentException (NumberFormatException, say) would otherwise be
        // reported to the caller as their fault with no evidence left anywhere. WARN, visible at
        // the production default: every hit is either a misattributed server bug or an unmigrated
        // caller-input site — the branch's shrink-to-zero to-do list.
        logger.warn("Replacing a bare IllegalArgumentException for the wire", iae);
        yield respond(
            Status.Code.INVALID_ARGUMENT,
            new SagaIllegalArgumentException("invalid request parameter"));
      }

      // ── Not found ──────────────────────────────────────────────────
      case SagaNotFoundException e -> respond(Status.Code.NOT_FOUND, e);
      case SagaDefinitionNotFoundException e -> respond(Status.Code.NOT_FOUND, e);

      // ── Conflict ───────────────────────────────────────────────────
      case SagaAlreadyExistsException e -> respond(Status.Code.ALREADY_EXISTS, e);
      case SagaConcurrentModificationException e -> respond(Status.Code.ABORTED, e);

      // ── Precondition ───────────────────────────────────────────────
      case SagaStatePreconditionException e -> respond(Status.Code.FAILED_PRECONDITION, e);

      // ── Bad request ────────────────────────────────────────────────
      // SAGA_DEFINITION_VERSION_CONTENT_CONFLICT is the one definition code that is not a bad
      // request: it is numbered in the conflict (103xx) sub-range, and the sub-range is a wire
      // contract, so the status must say 409 where the code says conflict. The guard dispatches it
      // ahead of the type arm; the other six definition codes are genuinely bad requests.
      case SagaDefinitionException e
          when e.getErrorCode() == SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT ->
          respond(Status.Code.ALREADY_EXISTS, e);
      case SagaDefinitionException e -> respond(Status.Code.INVALID_ARGUMENT, e);
      case SagaInvalidRequestException e -> respond(Status.Code.INVALID_ARGUMENT, e);
      case SagaIllegalArgumentException e -> respond(Status.Code.INVALID_ARGUMENT, e);

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
    // The exception constructor already built the wire message; reuse it rather than rebuilding.
    return status(statusCode, e.getErrorCode(), e.getMetadata(), e.getMessage());
  }

  /**
   * The one pairing table for interceptor refusals: which transport status carries each code the
   * interceptors emit, with the response payload precomputed — the pair is invariant per code, so a
   * refusal storm constructs no throwaway exception and serializes nothing per request.
   */
  private static final Map<SagaErrorCode, Refusal> REFUSALS = buildRefusals();

  /** The standard gRPC trailer carrying a serialized {@code google.rpc.Status}. */
  private static final Metadata.Key<byte[]> STATUS_DETAILS_BIN =
      Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER);

  private static Map<SagaErrorCode, Refusal> buildRefusals() {
    EnumMap<SagaErrorCode, Refusal> refusals = new EnumMap<>(SagaErrorCode.class);
    refusals.put(
        SagaErrorCode.UNAUTHENTICATED,
        refusal(Status.Code.UNAUTHENTICATED, SagaErrorCode.UNAUTHENTICATED));
    refusals.put(
        SagaErrorCode.PERMISSION_DENIED,
        refusal(Status.Code.PERMISSION_DENIED, SagaErrorCode.PERMISSION_DENIED));
    refusals.put(
        SagaErrorCode.SERVICE_UNAVAILABLE,
        refusal(Status.Code.UNAVAILABLE, SagaErrorCode.SERVICE_UNAVAILABLE));
    refusals.put(
        SagaErrorCode.INTERNAL_ERROR, refusal(Status.Code.INTERNAL, SagaErrorCode.INTERNAL_ERROR));
    refusals.put(
        SagaErrorCode.RATE_LIMIT_EXCEEDED,
        refusal(Status.Code.RESOURCE_EXHAUSTED, SagaErrorCode.RATE_LIMIT_EXCEEDED));
    return refusals;
  }

  private static Refusal refusal(Status.Code statusCode, SagaErrorCode code) {
    ErrorInfo info =
        ErrorInfo.newBuilder().setReason(code.code()).setDomain(SagaErrorCode.WIRE_DOMAIN).build();
    String message = code.buildMessage(ErrorMetadata.of());
    com.google.rpc.Status proto =
        com.google.rpc.Status.newBuilder()
            .setCode(statusCode.value())
            .setMessage(message)
            .addDetails(Any.pack(info))
            .build();
    return new Refusal(statusCode.toStatus().withDescription(message), proto);
  }

  /**
   * Closes a call an interceptor refuses before it reaches a service handler. The transport status
   * derives from the code through {@link #REFUSALS}, so the pairing decision exists in one place; a
   * code with no entry is an interceptor wiring bug and throws. Without this path, a refusal would
   * be the one server response with no {@link ErrorInfo}, and the client SDK would fall back to
   * transport-status dispatch — or misreport the refusal as an unrecognized server error.
   */
  static void close(ServerCall<?, ?> call, SagaErrorCode code) {
    Refusal refusal = refusalFor(code);
    call.close(refusal.status, refusal.trailers());
  }

  /**
   * As {@link #close(ServerCall, SagaErrorCode)}, appending a standard {@code RetryInfo} detail
   * carrying the advisory wait — for refusals whose reset time the server knows (the rate limiter's
   * window). Rebuilt per call since the delay varies; the cached base payload is reused. The REST
   * transport's analogue is the 429's Retry-After header.
   */
  static void close(ServerCall<?, ?> call, SagaErrorCode code, long retryAfterMillis) {
    Refusal refusal = refusalFor(code);
    RetryInfo retryInfo =
        RetryInfo.newBuilder()
            .setRetryDelay(
                Duration.newBuilder()
                    .setSeconds(retryAfterMillis / 1000)
                    .setNanos((int) ((retryAfterMillis % 1000) * 1_000_000L))
                    .build())
            .build();
    com.google.rpc.Status proto = refusal.proto.toBuilder().addDetails(Any.pack(retryInfo)).build();
    Metadata trailers = new Metadata();
    trailers.put(STATUS_DETAILS_BIN, proto.toByteArray());
    call.close(refusal.status, trailers);
  }

  private static Refusal refusalFor(SagaErrorCode code) {
    Refusal refusal = REFUSALS.get(code);
    if (refusal == null) {
      throw new IllegalArgumentException("no interceptor refusal payload for " + code);
    }
    return refusal;
  }

  /** A precomputed refusal: the transport status, the response proto, and its serialized form. */
  private static final class Refusal {
    private final Status status;
    private final com.google.rpc.Status proto;
    private final byte[] bytes;

    private Refusal(Status status, com.google.rpc.Status proto) {
      this.status = status;
      this.proto = proto;
      this.bytes = proto.toByteArray();
    }

    /**
     * Fresh {@link Metadata} per call (gRPC may take ownership of trailers); the payload bytes are
     * shared, which is safe — nothing mutates a put array.
     */
    private Metadata trailers() {
      Metadata trailers = new Metadata();
      trailers.put(STATUS_DETAILS_BIN, bytes);
      return trailers;
    }
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
    return status(statusCode, code, metadata, code.buildMessage(metadata));
  }

  private static StatusRuntimeException status(
      Status.Code statusCode, SagaErrorCode code, Map<String, String> metadata, String message) {
    ErrorInfo info =
        ErrorInfo.newBuilder()
            .setReason(code.code())
            .setDomain(SagaErrorCode.WIRE_DOMAIN)
            .putAllMetadata(metadata)
            .build();
    com.google.rpc.Status status =
        com.google.rpc.Status.newBuilder()
            .setCode(statusCode.value())
            .setMessage(message)
            .addDetails(Any.pack(info))
            .build();
    return StatusProto.toStatusRuntimeException(status);
  }
}
