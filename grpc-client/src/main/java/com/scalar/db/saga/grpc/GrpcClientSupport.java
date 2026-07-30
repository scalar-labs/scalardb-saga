package com.scalar.db.saga.grpc;

import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaPermissionDeniedException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;

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
   * Maps a gRPC {@link Status} to the api exception for the statuses that carry no saga-scoped
   * context. {@code NOT_FOUND} is deliberately absent — each client's context-specific mapper
   * handles it upstream (a missing saga vs a missing definition), so it never reaches this
   * catch-all. {@code rpcLabel} ({@code "Admin"} / {@code "Saga"}) flavors the human-readable
   * fallback messages; the daemon's own description, when present, is preferred over them.
   */
  static RuntimeException mapCommon(StatusRuntimeException e, String rpcLabel) {
    Status status = e.getStatus();
    String description = status.getDescription();
    switch (status.getCode()) {
      case INVALID_ARGUMENT:
        return new IllegalArgumentException(description == null ? "Invalid request" : description);
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
      case PERMISSION_DENIED:
        return new SagaPermissionDeniedException(e);
      case UNAUTHENTICATED:
        return new SagaUnauthenticatedException(e);
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
