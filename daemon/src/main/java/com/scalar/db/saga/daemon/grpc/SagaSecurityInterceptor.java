package com.scalar.db.saga.daemon.grpc;

import com.scalar.db.saga.daemon.security.SagaAuthRequest;
import com.scalar.db.saga.daemon.security.SagaAuthenticationException;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The gRPC enforcement point, the transport-parallel of {@link
 * com.scalar.db.saga.daemon.security.SecurityHandler} (REST): authenticates each call through the
 * same {@link SagaSecurityProvider} and checks the caller holds the role the method requires
 * (RBAC).
 *
 * <p>Applied only to the saga service (via {@code ServerInterceptors.intercept}); the standard
 * health service is left unintercepted so K8s-native gRPC probes need no credential. Credentials
 * are read from the call's request {@link Metadata} — every ASCII header is passed to the provider,
 * so it reads whichever one carries its credential ({@code authorization} for JWT, a configured
 * header for API keys), exactly as the REST handler does. An authentication failure closes the call
 * with {@code UNAUTHENTICATED}; a role shortfall with {@code PERMISSION_DENIED} — the gRPC
 * analogues of {@code 401}/{@code 403}. The resolved identity is attached to the gRPC {@link
 * Context} under {@link #IDENTITY} for downstream audit.
 */
public final class SagaSecurityInterceptor implements ServerInterceptor {

  /** Context key holding the authenticated identity for the duration of the call. */
  public static final Context.Key<SagaIdentity> IDENTITY = Context.key("saga.identity");

  private final SagaSecurityProvider provider;

  public SagaSecurityInterceptor(SagaSecurityProvider provider) {
    this.provider = provider;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    SagaIdentity identity;
    try {
      identity = provider.authenticate(toAuthRequest(call, headers));
    } catch (SagaAuthenticationException e) {
      return deny(call, Status.UNAUTHENTICATED.withDescription("Authentication required"));
    }
    SagaRole required = requiredRoleFor(call.getMethodDescriptor().getBareMethodName());
    if (!identity.hasRole(required)) {
      return deny(call, Status.PERMISSION_DENIED.withDescription("Insufficient permissions"));
    }
    Context context = Context.current().withValue(IDENTITY, identity);
    return Contexts.interceptCall(context, call, headers, next);
  }

  private static <ReqT, RespT> ServerCall.Listener<ReqT> deny(
      ServerCall<ReqT, RespT> call, Status status) {
    call.close(status, new Metadata());
    return new ServerCall.Listener<ReqT>() {};
  }

  /**
   * Resolves the minimum role a gRPC method requires: the read/poll methods need {@link
   * SagaRole#READ}; everything else (starting a saga, and any future state-changing method) needs
   * {@link SagaRole#WRITE}.
   *
   * <p><b>Keep in sync</b> with the REST mapping in {@code SecurityHandler.requiredRoleFor}: the
   * two transports encode the same operation&rarr;role policy in different vocabularies (gRPC
   * method name vs HTTP verb), so an operation added to one must be mirrored in the other. When
   * ADMIN-gated operations land, replace both switches with a shared operation&rarr;role policy so
   * the decision lives in one place.
   *
   * <p>Package-private for unit testing.
   */
  static SagaRole requiredRoleFor(@Nullable String bareMethodName) {
    if (bareMethodName == null) {
      return SagaRole.WRITE;
    }
    return switch (bareMethodName) {
      case "GetSaga", "AwaitSaga" -> SagaRole.READ;
      default -> SagaRole.WRITE;
    };
  }

  private static SagaAuthRequest toAuthRequest(ServerCall<?, ?> call, Metadata headers) {
    Map<String, String> asciiHeaders = new HashMap<>();
    for (String key : headers.keys()) {
      if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        continue; // binary metadata cannot be read as an ASCII string
      }
      String value = headers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
      if (value != null) {
        asciiHeaders.put(key, value);
      }
    }
    SocketAddress remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    return SagaAuthRequest.fromHeaders(
        call.getMethodDescriptor().getFullMethodName(),
        remote == null ? null : remote.toString(),
        asciiHeaders);
  }
}
