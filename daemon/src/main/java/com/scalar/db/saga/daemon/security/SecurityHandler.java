package com.scalar.db.saga.daemon.security;

import io.javalin.Javalin;
import io.javalin.http.Context;
import java.util.Objects;

/**
 * The RBAC enforcement point for the REST transport: a Javalin {@code before}-handler that runs
 * ahead of every route, authenticating the caller through the configured {@link
 * SagaSecurityProvider} and checking the caller holds the role the endpoint requires.
 *
 * <p>Flow per request:
 *
 * <ol>
 *   <li>If the path is on the {@link AuthExemptions} allowlist (health probe, HMAC-authed async
 *       callback), skip auth entirely — those routes carry their own (or no) auth.
 *   <li>Authenticate via the provider; a failure surfaces as {@link SagaAuthenticationException}
 *       ({@code 401}).
 *   <li>Resolve the endpoint's required {@link SagaRole} from the HTTP method and check {@link
 *       SagaIdentity#hasRole}; a shortfall surfaces as {@link SagaAuthorizationException} ({@code
 *       403}).
 *   <li>Store the resolved {@link SagaIdentity} on the request under {@link #IDENTITY_ATTRIBUTE} so
 *       downstream handlers (e.g. a future Admin audit log) read the operator as a caller-supplied
 *       value rather than re-authenticating.
 * </ol>
 *
 * <p>The {@link SagaAuthenticationException}/{@link SagaAuthorizationException} thrown here are
 * mapped to {@code 401}/{@code 403} by {@code ErrorMapper}, keeping response rendering in one
 * place.
 */
public final class SecurityHandler {

  /** Request attribute key under which the authenticated {@link SagaIdentity} is stored. */
  public static final String IDENTITY_ATTRIBUTE = "saga.identity";

  private final SagaSecurityProvider provider;
  private final AuthExemptions exemptions;

  private SecurityHandler(SagaSecurityProvider provider, AuthExemptions exemptions) {
    this.provider = provider;
    this.exemptions = exemptions;
  }

  /**
   * Registers the RBAC before-handler on the given app.
   *
   * @param app the Javalin app
   * @param provider the security provider that authenticates requests
   * @param exemptions the paths that bypass authentication (health, async callback)
   */
  public static void register(
      Javalin app, SagaSecurityProvider provider, AuthExemptions exemptions) {
    Objects.requireNonNull(app, "app must not be null");
    Objects.requireNonNull(provider, "provider must not be null");
    Objects.requireNonNull(exemptions, "exemptions must not be null");
    SecurityHandler handler = new SecurityHandler(provider, exemptions);
    app.before(handler::handle);
  }

  private void handle(Context ctx) {
    if (exemptions.isExempt(ctx.path())) {
      return;
    }
    SagaAuthRequest request =
        SagaAuthRequest.fromHeaderLookup(ctx.method() + " " + ctx.path(), ctx.ip(), ctx::header);
    SagaIdentity identity = provider.authenticate(request);
    SagaRole required = requiredRoleFor(ctx.method().name());
    if (!identity.hasRole(required)) {
      throw new SagaAuthorizationException(identity.principal(), required);
    }
    ctx.attribute(IDENTITY_ATTRIBUTE, identity);
  }

  /**
   * Resolves the minimum {@link SagaRole} an endpoint requires from its HTTP method: read methods
   * ({@code GET}/{@code HEAD}) require {@link SagaRole#READ}; state-changing methods require {@link
   * SagaRole#WRITE}. Admin-only operations (cancel, list, and future admin endpoints) will require
   * {@link SagaRole#ADMIN} via an explicit per-route override when those routes land; there are
   * none today, so a method-based default covers the current surface.
   *
   * <p><b>Keep in sync</b> with the gRPC mapping in {@code
   * SagaSecurityInterceptor.requiredRoleFor}: the two transports encode the same
   * operation&rarr;role policy in different vocabularies (HTTP verb vs gRPC method name), so an
   * operation added to one must be mirrored in the other. When ADMIN-gated (per-route) operations
   * land, replace both verb/method switches with a shared operation&rarr;role policy so the
   * decision lives in one place.
   *
   * <p>Package-private for unit testing the mapping without a running server.
   */
  static SagaRole requiredRoleFor(String method) {
    return switch (method) {
      case "GET", "HEAD" -> SagaRole.READ;
      // POST/PUT/PATCH/DELETE and any other method are treated as state-changing (write). This is
      // the safe default: an unknown method requires at least write, never less.
      default -> SagaRole.WRITE;
    };
  }
}
