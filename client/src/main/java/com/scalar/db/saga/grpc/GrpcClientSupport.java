package com.scalar.db.saga.grpc;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.ExceptionRegistry;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaPermissionDeniedException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import org.jspecify.annotations.Nullable;

/**
 * Transport mechanics shared by the two gRPC clients ({@link GrpcSagaOrchestratorClient} and {@link
 * GrpcSagaAdminClient}) — channel construction, graceful shutdown, and the common status-to-api
 * exception mapping. Both clients are otherwise self-contained; this holds only the pieces that
 * were byte-for-byte identical, so a change (or a fix) lands in one place instead of drifting
 * between two copies.
 */
final class GrpcClientSupport {

  private static final long CLOSE_TIMEOUT_SECONDS = 5L;

  private GrpcClientSupport() {}

  /**
   * Opens a channel to {@code target}. TLS uses the JRE's transport security and fails fast if ALPN
   * is unavailable (on Java 8, that means pre-8u252 without a bundled {@code tcnative}); plaintext
   * is appropriate for an isolated in-cluster network.
   */
  static ManagedChannel openChannel(String target, boolean useTls) {
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
    if (useTls) {
      if (!alpnAvailable()) {
        throw new IllegalStateException(
            "TLS requested but ALPN is unavailable on this JRE. On Java 8, use 8u252+ or add "
                + "netty-tcnative-boringssl-static; otherwise use plaintext (in-cluster).");
      }
      channelBuilder.useTransportSecurity();
    } else {
      channelBuilder.usePlaintext();
    }
    return channelBuilder.build();
  }

  /**
   * Gracefully shuts down an owned channel: initiate shutdown, wait up to {@value
   * #CLOSE_TIMEOUT_SECONDS}s for in-flight calls to drain, then force. Restores the interrupt flag
   * if interrupted while waiting. The caller is responsible for the once-only guard.
   */
  static void shutdown(ManagedChannel channel) {
    channel.shutdown();
    try {
      if (!channel.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    } catch (InterruptedException e) {
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Maps a wire failure to the api exception: the daemon's {@link ErrorInfo} when it sent one,
   * otherwise transport-status dispatch. This is the whole mapping for a call with no saga-scoped
   * context to add; the context-specific mappers call {@link #reconstruct} and {@link
   * #mapTransport} separately so they can slot their own handling between the two.
   */
  static RuntimeException mapWire(StatusRuntimeException e) {
    SagaRuntimeException reconstructed = reconstruct(e);
    return reconstructed != null ? reconstructed : mapTransport(e);
  }

  /**
   * Rebuilds the typed exception the daemon's {@link ErrorInfo} describes, inverting what {@code
   * GrpcErrorMapper} put on the wire, or returns {@code null} when the response carried no {@code
   * ErrorInfo} — an older daemon, an intermediary that stripped it, or a transport failure that
   * never reached the daemon's mapper — <b>and</b> when it carried one this client cannot resolve:
   * an unknown code (a newer daemon, during a rolling upgrade) or metadata that no longer fits the
   * code's schema. Returning {@code null} for those routes the failure to {@link #mapTransport},
   * which classifies by the status family the daemon did set correctly; reporting them as {@code
   * UNRECOGNIZED_SERVER_ERROR} here would flip a retryable {@code UNAVAILABLE} into {@code
   * CLIENT_ERROR} and stop a caller's retries. A genuine {@code DB-SAGA-49999} the server itself
   * sent still round-trips — that code has a registered reconstructor. One rescue before the {@code
   * null}: a reason whose frozen category digit says {@code 2xxxx} (retryable) returns the {@code
   * UNRECOGNIZED_RETRYABLE_SERVER_ERROR} sentinel instead, so the retry signal survives a version
   * skew even under a status the transport dispatch has no arm for.
   *
   * <p>Prefer this over {@link #mapTransport} wherever both could apply: the gRPC status is a
   * coarse family ({@code INVALID_ARGUMENT} carries both a rejected argument and seven definition
   * codes; {@code NOT_FOUND} carries a missing saga and a missing definition), while the {@code
   * ErrorInfo} reason names the exact code. The gRPC status is attached as the cause so its
   * description and trailers — the unresolved reason included — stay available for debugging.
   */
  static @Nullable SagaRuntimeException reconstruct(StatusRuntimeException e) {
    ErrorInfo info = errorInfo(e);
    if (info == null) {
      return null;
    }
    SagaRuntimeException reconstructed =
        ExceptionRegistry.tryReconstruct(info.getReason(), info.getMetadataMap()).orElse(null);
    if (reconstructed == null) {
      reconstructed = retryableUnknown(info.getReason());
    }
    if (reconstructed == null) {
      return null;
    }
    if (reconstructed.getCause() == null) {
      // The registry builds every exception cause-free, so this is the one chance to attach the
      // gRPC status. Guarded rather than unconditional: initCause throws once a cause is set, and a
      // future reconstructor may pass one.
      reconstructed.initCause(e);
    }
    return reconstructed;
  }

  /**
   * The unknown-but-retryable fallback: the category digit of a well-formed {@code DB-SAGA-*}
   * reason is a frozen wire contract, so even a code this client cannot resolve still tells it the
   * one thing retries key on. Only the retryable category is rescued here — the transport dispatch
   * classifies every other family acceptably, but a retryable code riding a status with no
   * transport arm (ABORTED, FAILED_PRECONDITION) would land in the CLIENT_ERROR catch-all and stop
   * a caller's retries. The real code stays readable in {@code server_value}.
   */
  private static @Nullable SagaRuntimeException retryableUnknown(String reason) {
    if (SagaErrorCode.Category.fromWireCode(reason).orElse(null)
        != SagaErrorCode.Category.RETRYABLE_SERVER_ERROR) {
      return null;
    }
    return new SagaRuntimeException(
        SagaErrorCode.UNRECOGNIZED_RETRYABLE_SERVER_ERROR,
        ErrorMetadata.of("server_value", reason));
  }

  /**
   * The first {@link ErrorInfo} detail on {@code e} whose domain is {@link
   * SagaErrorCode#WIRE_DOMAIN}, or {@code null} if it carries none. In {@code google.rpc.ErrorInfo}
   * the domain is what scopes the reason, and any hop in the request path may attach its own {@code
   * ErrorInfo} — a mesh sidecar or gateway generating the failure, for example — so an entry with a
   * foreign domain (or one that fails to parse) is skipped rather than read as a saga code, and the
   * scan continues in case the daemon's own entry follows it.
   */
  private static @Nullable ErrorInfo errorInfo(StatusRuntimeException e) {
    com.google.rpc.Status status = StatusProto.fromThrowable(e);
    if (status == null) {
      return null;
    }
    for (Any detail : status.getDetailsList()) {
      if (!detail.is(ErrorInfo.class)) {
        continue;
      }
      ErrorInfo info;
      try {
        info = detail.unpack(ErrorInfo.class);
      } catch (InvalidProtocolBufferException malformed) {
        continue;
      }
      if (SagaErrorCode.WIRE_DOMAIN.equals(info.getDomain())) {
        return info;
      }
    }
    return null;
  }

  /**
   * Maps a gRPC {@link Status} to the api exception, for a response that carried no {@link
   * ErrorInfo} to reconstruct from. {@code NOT_FOUND} is deliberately absent — each client's
   * context-specific mapper handles it upstream (a missing saga vs a missing definition), so it
   * never reaches this catch-all. Every status but {@code INVALID_ARGUMENT} takes its message from
   * its {@link SagaErrorCode} and carries the gRPC status, description included, as the cause.
   * {@code INVALID_ARGUMENT} instead passes the daemon's description through as the {@link
   * SagaIllegalArgumentException} detail (falling back to a fixed one when the daemon sent none),
   * because that text is the validation detail the caller needs.
   */
  static RuntimeException mapTransport(StatusRuntimeException e) {
    Status status = e.getStatus();
    String description = status.getDescription();
    switch (status.getCode()) {
      case INVALID_ARGUMENT:
        return new SagaIllegalArgumentException(
            description == null ? "Invalid request" : description, e);
      case DEADLINE_EXCEEDED:
        // The gRPC status (with its description) is preserved as the cause; the fixed message
        // comes from SagaErrorCode.REQUEST_TIMEOUT.
        return new SagaTimeoutException(e);
      case CANCELLED:
        // Caller-initiated, not a server error. The blocking stub reports an interrupt of the
        // calling thread as CANCELLED ("Thread interrupted"; gRPC has already restored the
        // interrupt flag), as it does an in-flight call killed by a concurrent shutdownNow. Letting
        // these fall through to the catch-all would claim a version skew and tell the caller to
        // upgrade the SDK. This matches the retry loops, which already surface an interrupt during
        // backoff as REQUEST_ABORTED.
        return new SagaRuntimeException(SagaErrorCode.REQUEST_ABORTED, ErrorMetadata.of(), e);
      case UNAVAILABLE:
        return new SagaUnavailableException(e);
      case RESOURCE_EXHAUSTED:
        // The daemon's rate limiter refuses over-limit calls at the interceptor layer, and an
        // older daemon does so without an ErrorInfo. Letting that fall through to the catch-all
        // would turn "back off and retry" into "upgrade the SDK". gRPC also uses
        // RESOURCE_EXHAUSTED for an oversized message; the original description stays readable on
        // the cause.
        return new SagaRuntimeException(SagaErrorCode.RATE_LIMIT_EXCEEDED, ErrorMetadata.of(), e);
      case PERMISSION_DENIED:
        return new SagaPermissionDeniedException(e);
      case UNAUTHENTICATED:
        return new SagaUnauthenticatedException(e);
      case INTERNAL:
      case UNKNOWN:
        // A server fault reported without an ErrorInfo — an older daemon's security interceptor,
        // or an intermediary. UNKNOWN is its sibling: what the gRPC runtime emits when a failure
        // escapes the daemon's handlers entirely (an Error, or a fault in interceptor code).
        // INTERNAL_ERROR tells the caller to escalate and not retry, which beats the catch-all's
        // claim of a version skew.
        return new SagaRuntimeException(SagaErrorCode.INTERNAL_ERROR, ErrorMetadata.of(), e);
      default:
        return new SagaRuntimeException(
            SagaErrorCode.UNRECOGNIZED_SERVER_ERROR,
            ErrorMetadata.of("server_value", status.getCode().name()),
            e);
    }
  }

  private static boolean alpnAvailable() {
    // SSLEngine.getApplicationProtocol exists from Java 9 and was backported to 8u252. Conservative
    // — a bundled tcnative may provide ALPN where the JDK does not — but it gives a clear, early
    // failure for the common pre-8u252 case.
    try {
      SSLEngine.class.getMethod("getApplicationProtocol");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
