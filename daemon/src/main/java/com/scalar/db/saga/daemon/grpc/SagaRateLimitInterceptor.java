package com.scalar.db.saga.daemon.grpc;

import com.scalar.db.saga.daemon.api.RateLimiter;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.jspecify.annotations.Nullable;

/**
 * The gRPC parallel of {@link com.scalar.db.saga.daemon.api.RateLimitHandler}: rate-limits
 * saga-start calls per authenticated principal — the transport-symmetric half of the daemon's
 * per-caller DoS control. It shares one {@link RateLimiter} with the REST handler, so a caller's
 * saga-start budget is global to the caller rather than counted separately per port.
 *
 * <p>Runs after {@link SagaSecurityInterceptor} (which resolves the caller and puts the {@link
 * SagaIdentity} on the gRPC {@link io.grpc.Context}); only state-changing methods are limited, and
 * the read/poll methods ({@code GetSaga}/{@code AwaitSaga}) pass through. An over-limit call is
 * closed with {@code RESOURCE_EXHAUSTED} — the gRPC analogue of HTTP {@code 429}.
 */
public final class SagaRateLimitInterceptor implements ServerInterceptor {

  private final RateLimiter limiter;

  public SagaRateLimitInterceptor(RateLimiter limiter) {
    this.limiter = limiter;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    if (isRateLimited(call.getMethodDescriptor().getBareMethodName())) {
      // Set by SagaSecurityInterceptor, which must run first (see the interceptor ordering in
      // SagaServer). Null only if this runs without upstream auth; then there is no budget to key
      // on, so the call is left to proceed rather than blocked by a rate limit it cannot attribute.
      SagaIdentity identity = SagaSecurityInterceptor.IDENTITY.get();
      if (identity != null
          && !limiter.tryAcquire(identity.principal(), System.currentTimeMillis())) {
        call.close(
            Status.RESOURCE_EXHAUSTED.withDescription("Saga-start rate limit exceeded"),
            new Metadata());
        return new ServerCall.Listener<>() {};
      }
    }
    return next.startCall(call, headers);
  }

  /**
   * Whether a gRPC method is rate-limited: the read/poll methods ({@code GetSaga}/{@code
   * AwaitSaga}) are not; everything else is — starting a saga, any future state-changing method,
   * and an unclassifiable/{@code null} method (treated strictly, as {@link
   * SagaSecurityInterceptor#requiredRoleFor} treats an unknown method as a write). Mirrors the REST
   * handler, which limits writes and leaves reads alone. Package-private for unit testing.
   */
  static boolean isRateLimited(@Nullable String bareMethodName) {
    return !"GetSaga".equals(bareMethodName) && !"AwaitSaga".equals(bareMethodName);
  }
}
