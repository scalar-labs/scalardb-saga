package com.scalar.db.saga.daemon.grpc;

import com.scalar.db.saga.daemon.api.RateLimiter;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaOperation;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * The gRPC parallel of {@link com.scalar.db.saga.daemon.api.RateLimitHandler}: rate-limits calls
 * per authenticated principal — the transport-symmetric half of the daemon's per-caller DoS
 * control. It shares one {@link RateLimiter} with the REST handler, so a caller's budget is global
 * to the caller rather than counted separately per port.
 *
 * <p>Runs after {@link SagaSecurityInterceptor} (which resolves the caller and puts the {@link
 * SagaIdentity} on the gRPC {@link io.grpc.Context}). Whether a call is limited comes from its
 * {@link SagaOperation#rateLimited()}, the same policy the REST handler reads, so the two
 * transports cannot drift. An over-limit call is closed with {@code RESOURCE_EXHAUSTED} — the gRPC
 * analogue of HTTP {@code 429}.
 */
public final class SagaRateLimitInterceptor implements ServerInterceptor {

  private final RateLimiter limiter;

  public SagaRateLimitInterceptor(RateLimiter limiter) {
    this.limiter = limiter;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    SagaOperation operation =
        GrpcOperations.fromBareMethodName(call.getMethodDescriptor().getBareMethodName());
    // An unmapped method is refused by SagaSecurityInterceptor, which runs first (see the
    // interceptor ordering in SagaServer), so it cannot reach here. Treat it strictly regardless:
    // a method with no policy is not one to wave through unlimited.
    if (operation == null || operation.rateLimited()) {
      // Set by SagaSecurityInterceptor. Null only if this runs without upstream auth; then there is
      // no budget to key on, so the call is left to proceed rather than blocked by a rate limit it
      // cannot attribute.
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
}
