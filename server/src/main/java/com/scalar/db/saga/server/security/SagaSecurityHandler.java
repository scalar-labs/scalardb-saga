package com.scalar.db.saga.server.security;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.Header;
import io.javalin.http.HttpStatus;
import java.util.Objects;

/**
 * The RBAC enforcement point for the REST transport: a Javalin {@code beforeMatched}-handler that
 * runs ahead of every matched route, authenticating the caller through the configured {@link
 * SagaSecurityProvider} and checking the caller holds the role the route's {@link SagaOperation}
 * requires.
 *
 * <p>Flow per request:
 *
 * <ol>
 *   <li>Resolve the matched route's {@link SagaOperation} from its declared roles; an untagged
 *       route is refused rather than served (see {@link SagaOperation#fromRouteRoles}).
 *   <li>If the operation is exempt (health probe, HMAC-authed async callback), skip auth entirely —
 *       those routes carry their own (or no) auth.
 *   <li>Authenticate via the provider; a failure surfaces as {@link SagaAuthenticationException}
 *       ({@code 401}).
 *   <li>Check {@link SagaIdentity#hasRole} against the operation's required role; a shortfall
 *       surfaces as {@link SagaAuthorizationException} ({@code 403}).
 *   <li>Store the resolved {@link SagaIdentity} on the request under {@link #IDENTITY_ATTRIBUTE} so
 *       downstream handlers (the rate limiter, the Admin API's audit) read the operator as a
 *       resolved value rather than re-authenticating.
 * </ol>
 *
 * <p>This runs on {@code beforeMatched}, not {@code before}, because the policy is per route rather
 * than per HTTP verb, and {@code Context.routeRoles()} is only populated once a route has matched.
 * Two consequences are deliberate:
 *
 * <ul>
 *   <li>An unmatched path no longer authenticates — it 404s where it previously 401'd. Route
 *       existence becomes probeable without a credential; the route set is public API, not a
 *       secret.
 *   <li>Every handler that depends on {@link #IDENTITY_ATTRIBUTE} must also run on {@code
 *       beforeMatched}. Javalin runs all {@code before} handlers ahead of any {@code beforeMatched}
 *       one, so a dependent left on {@code before} would silently observe no identity — see {@link
 *       com.scalar.db.saga.server.api.RateLimitHandler}.
 * </ul>
 *
 * <p>A {@code HEAD} to a route that only registers {@code GET} is answered here with {@code 405
 * Method Not Allowed} ({@code Allow: GET}). Javalin has no HEAD-as-GET toggle, so it routes such a
 * request through {@code beforeMatched} with the empty resource-handler role set — the same signal
 * an untagged route gives. The two are told apart by the verb: an empty role set on {@code HEAD}
 * means "GET route exists, no HEAD handler" ({@code beforeMatched} runs for HEAD only when a GET
 * handler is registered at the path), so it is a method mismatch, not a missing policy; an empty
 * set on any other verb is still a programming error and falls through to {@link
 * SagaOperation#fromRouteRoles}'s fail-closed rejection. A route that wants to serve HEAD (the
 * liveness probe) registers its own {@code HEAD} handler, which carries the route's roles and so
 * never reaches this branch.
 *
 * <p>The {@link SagaAuthenticationException}/{@link SagaAuthorizationException} thrown here are
 * mapped to {@code 401}/{@code 403} by {@code ErrorMapper}, keeping response rendering in one
 * place. The {@code 405} above is the one response rendered inline rather than through {@code
 * ErrorMapper}: it is a method-dispatch decision made at this chokepoint, not an exception
 * surfacing from business logic, and a HEAD response carries no body to render anyway.
 */
public final class SagaSecurityHandler {

  /** Request attribute key under which the authenticated {@link SagaIdentity} is stored. */
  public static final String IDENTITY_ATTRIBUTE = "saga.identity";

  private final SagaSecurityProvider provider;

  private SagaSecurityHandler(SagaSecurityProvider provider) {
    this.provider = provider;
  }

  /**
   * Registers the RBAC handler on the given app.
   *
   * @param app the Javalin app
   * @param provider the security provider that authenticates requests
   */
  public static void register(Javalin app, SagaSecurityProvider provider) {
    Objects.requireNonNull(app, "app must not be null");
    Objects.requireNonNull(provider, "provider must not be null");
    SagaSecurityHandler handler = new SagaSecurityHandler(provider);
    app.beforeMatched(handler::handle);
  }

  private void handle(Context ctx) {
    if (ctx.method() == HandlerType.HEAD && ctx.routeRoles().isEmpty()) {
      // HEAD to a GET-only route: Javalin routes it here with the empty resource-handler role set.
      // The route exists (beforeMatched runs for HEAD only when a GET handler is registered at the
      // path), we just do not serve HEAD on it — answer 405 rather than letting fromRouteRoles
      // reject it as an untagged route (500).
      ctx.status(HttpStatus.METHOD_NOT_ALLOWED).header(Header.ALLOW, "GET");
      ctx.skipRemainingHandlers();
      return;
    }
    SagaOperation operation = SagaOperation.fromRouteRoles(ctx.routeRoles());
    SagaRole required = operation.requiredRole();
    if (required == null) {
      // An exempt operation: the liveness probe carries no credential by design, and the async
      // callback authenticates with its own per-step HMAC token. Authenticating here would reject a
      // participant's callback with 401 before its HMAC check ran, breaking async completion.
      return;
    }
    SagaAuthRequest request =
        SagaAuthRequest.fromHeaderLookup(ctx.method() + " " + ctx.path(), ctx.ip(), ctx::header);
    SagaIdentity identity = provider.authenticate(request);
    if (!identity.hasRole(required)) {
      throw new SagaAuthorizationException(identity.principal(), required);
    }
    ctx.attribute(IDENTITY_ATTRIBUTE, identity);
  }
}
