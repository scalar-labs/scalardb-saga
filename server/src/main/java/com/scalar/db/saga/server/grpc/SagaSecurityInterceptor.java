package com.scalar.db.saga.server.grpc;

import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.server.security.SagaAuthRequest;
import com.scalar.db.saga.server.security.SagaAuthUnavailableException;
import com.scalar.db.saga.server.security.SagaAuthenticationException;
import com.scalar.db.saga.server.security.SagaIdentity;
import com.scalar.db.saga.server.security.SagaOperation;
import com.scalar.db.saga.server.security.SagaRole;
import com.scalar.db.saga.server.security.SagaSecurityProvider;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.SocketAddress;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gRPC enforcement point, the transport-parallel of {@link
 * com.scalar.db.saga.server.security.SagaSecurityHandler} (REST): authenticates each call through
 * the same {@link SagaSecurityProvider} and checks the caller holds the role the call's {@link
 * SagaOperation} requires (RBAC). The method is mapped to its operation by {@link GrpcOperations},
 * so both transports enforce one shared policy rather than two per-transport encodings of it.
 *
 * <p>Applied to every privileged gRPC service — the saga service and the admin service, each
 * wrapped in {@code SagaServer} via {@code ServerInterceptors} — so a new service carrying
 * privileged RPCs must be wrapped here too; the standard health service is deliberately left
 * unintercepted so K8s-native gRPC probes need no credential. Credentials are read from the call's
 * request {@link Metadata} — every ASCII header is passed to the provider, so it reads whichever
 * one carries its credential ({@code authorization} for JWT, a configured header for API keys),
 * exactly as the REST handler does. An authentication failure closes the call with {@code
 * UNAUTHENTICATED}; a role shortfall with {@code PERMISSION_DENIED} — the gRPC analogues of {@code
 * 401}/{@code 403}. A provider that is unavailable (e.g. an unreachable JWKS endpoint) closes the
 * call with {@code UNAVAILABLE} — a retryable outage, not a bad credential. Every refusal is
 * composed through {@link GrpcErrorMapper#close}, so it carries the matching {@link SagaErrorCode}
 * in an {@code ErrorInfo} detail like every other daemon response. The resolved identity is
 * attached to the gRPC {@link Context} under {@link #IDENTITY} for downstream audit.
 */
public final class SagaSecurityInterceptor implements ServerInterceptor {

  private static final Logger logger = LoggerFactory.getLogger(SagaSecurityInterceptor.class);

  /** Context key holding the authenticated identity for the duration of the call. */
  public static final Context.Key<SagaIdentity> IDENTITY = Context.key("saga.identity");

  private final SagaSecurityProvider provider;

  public SagaSecurityInterceptor(SagaSecurityProvider provider) {
    this.provider = provider;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    SagaOperation operation =
        GrpcOperations.fromBareMethodName(call.getMethodDescriptor().getBareMethodName());
    if (operation == null) {
      // The method carries no policy — an RPC was registered without being added to GrpcOperations.
      // Refuse rather than default to a role: INTERNAL, because this is a server bug and not a bad
      // credential, mirroring the REST path's 500 on an untagged route.
      logger.error(
          "gRPC method '{}' has no mapped SagaOperation; refusing the call",
          call.getMethodDescriptor().getFullMethodName());
      return deny(call, Status.Code.INTERNAL, SagaErrorCode.INTERNAL_ERROR);
    }
    SagaRole required = operation.requiredRole();
    if (required == null) {
      // An auth-exempt operation. No gRPC method maps to one today — the health service is left
      // unintercepted entirely rather than exempted here — but the two transports resolve exemption
      // from the same place, so honour it symmetrically.
      return next.startCall(call, headers);
    }
    SagaIdentity identity;
    try {
      identity = provider.authenticate(toAuthRequest(call, headers));
    } catch (SagaAuthenticationException e) {
      return deny(call, Status.Code.UNAUTHENTICATED, SagaErrorCode.UNAUTHENTICATED);
    } catch (SagaAuthUnavailableException e) {
      // The provider could not verify the credential because it is unavailable (e.g. the JWKS
      // endpoint is unreachable) — a transient upstream outage, not a bad credential. Map to
      // UNAVAILABLE so the caller can retry, mirroring the REST path's 503.
      logger.warn("Authentication provider unavailable for a gRPC call", e);
      return deny(call, Status.Code.UNAVAILABLE, SagaErrorCode.SERVICE_UNAVAILABLE);
    } catch (RuntimeException e) {
      // An unexpected provider failure (not a rejected credential) — a bug or an unwrapped
      // transient error. Fail closed and log it server-side; map to INTERNAL rather than
      // UNAUTHENTICATED so it is not mistaken for a bad credential (the REST path's ErrorMapper
      // maps an unhandled error to 500 the same way). No detail leaks to the client.
      logger.error("Unexpected error authenticating a gRPC call", e);
      return deny(call, Status.Code.INTERNAL, SagaErrorCode.INTERNAL_ERROR);
    }
    if (!identity.hasRole(required)) {
      return deny(call, Status.Code.PERMISSION_DENIED, SagaErrorCode.PERMISSION_DENIED);
    }
    Context context = Context.current().withValue(IDENTITY, identity);
    return Contexts.interceptCall(context, call, headers, next);
  }

  private static <ReqT, RespT> ServerCall.Listener<ReqT> deny(
      ServerCall<ReqT, RespT> call, Status.Code statusCode, SagaErrorCode errorCode) {
    GrpcErrorMapper.close(call, statusCode, errorCode);
    return new ServerCall.Listener<ReqT>() {};
  }

  private static SagaAuthRequest toAuthRequest(ServerCall<?, ?> call, Metadata headers) {
    SocketAddress remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    return SagaAuthRequest.fromHeaderLookup(
        call.getMethodDescriptor().getFullMethodName(),
        remote == null ? null : remote.toString(),
        // Read only the header the provider asks for, rather than copying every header into a map.
        // gRPC metadata keys are lower-case, so normalize the requested name; a binary ("-bin")
        // header is not a text credential and simply will not be found.
        name ->
            headers.get(
                Metadata.Key.of(name.toLowerCase(Locale.ROOT), Metadata.ASCII_STRING_MARSHALLER)));
  }
}
