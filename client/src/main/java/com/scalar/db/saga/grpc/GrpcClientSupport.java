package com.scalar.db.saga.grpc;

import com.scalar.db.saga.exception.SagaPermissionDeniedException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import java.io.IOException;
import java.nio.file.Path;
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
   *
   * <p>A non-null {@code trustCaCertPath} narrows this channel's trust to that CA (replacing the
   * JVM's default trust store) — the private-CA case, e.g. a cert-manager- or Vault-issued server
   * certificate. The file is read eagerly, so a bad path fails here naming the file rather than at
   * the first RPC as an opaque {@code UNAVAILABLE}. A non-null {@code overrideAuthority} validates
   * the server certificate against that name instead of the dialed address — for dialing by IP or
   * through a port-forward while the certificate names the service's DNS name.
   */
  static ManagedChannel openChannel(
      String target,
      boolean useTls,
      @Nullable Path trustCaCertPath,
      @Nullable String overrideAuthority) {
    // Enforced here, on the shared seam, rather than in each builder: a trust setting says the
    // caller expects encryption, and silently ignoring it on a plaintext channel would be worse
    // than refusing.
    if (trustCaCertPath != null && !useTls) {
      throw new IllegalStateException(
          "trustCaCertificate(...) is set but the channel is plaintext. Call"
              + " useTransportSecurity() to enable TLS, or drop the trust setting.");
    }
    ManagedChannelBuilder<?> channelBuilder;
    if (useTls) {
      if (!alpnAvailable()) {
        throw new IllegalStateException(
            "TLS requested but ALPN is unavailable on this JRE. On Java 8, use 8u252+ or add "
                + "netty-tcnative-boringssl-static; otherwise use plaintext (in-cluster).");
      }
      channelBuilder =
          trustCaCertPath == null
              ? ManagedChannelBuilder.forTarget(target).useTransportSecurity()
              : Grpc.newChannelBuilder(target, tlsCredentials(trustCaCertPath));
    } else {
      channelBuilder = ManagedChannelBuilder.forTarget(target).usePlaintext();
    }
    if (overrideAuthority != null) {
      channelBuilder.overrideAuthority(overrideAuthority);
    }
    return channelBuilder.build();
  }

  /**
   * Builds TLS channel credentials trusting only the CA at {@code trustCaCertPath}. {@code
   * TlsChannelCredentials} reads the file at build time, which is what gives the builders their
   * fail-at-build() behavior; the {@code IOException} is rewrapped so the failure names the path.
   */
  private static ChannelCredentials tlsCredentials(Path trustCaCertPath) {
    try {
      return TlsChannelCredentials.newBuilder().trustManager(trustCaCertPath.toFile()).build();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to read the trust CA certificate at '"
              + trustCaCertPath
              + "'. The file must be a readable PEM certificate collection.",
          e);
    }
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
        return new SagaTimeoutException(
            description == null ? rpcLabel + " RPC deadline exceeded" : description, e);
      case UNAVAILABLE:
        return new SagaUnavailableException(
            description == null ? rpcLabel + " service temporarily unavailable" : description, e);
      case PERMISSION_DENIED:
        return new SagaPermissionDeniedException(
            description == null ? "Permission denied" : description, e);
      case UNAUTHENTICATED:
        return new SagaUnauthenticatedException(
            description == null ? "Authentication required" : description, e);
      default:
        return new SagaRuntimeException(
            rpcLabel
                + " RPC failed ("
                + status.getCode()
                + ")"
                + (description == null ? "" : ": " + description),
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
