package com.scalar.db.saga.daemon.security;

import io.javalin.security.RouteRole;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * The daemon's catalogue of operations, and the single source of truth for the access policy each
 * one carries: the minimum {@link SagaRole} a caller must hold, and whether the operation is
 * rate-limited.
 *
 * <p>Both transports resolve their policy from this enum rather than encoding it themselves. REST
 * tags each route with its operation — this enum is a Javalin {@link RouteRole}, so the tag travels
 * as the route's declared roles and is read back through {@code Context.routeRoles()}. gRPC maps
 * each method name onto it. An operation therefore cannot be exposed on one transport under a
 * policy the other does not share, which is what the previous verb-keyed and method-keyed switches
 * could not guarantee.
 *
 * <p>The policy is <b>per operation, not per HTTP verb</b>. Verb-keying cannot express the admin
 * surface at all: every admin mutation is a {@code POST}, so a verb-keyed rule would grant them all
 * to any {@code saga:write} caller.
 *
 * <p>An operation with a {@code null} {@linkplain #requiredRole() required role} is <b>exempt from
 * authentication</b>: {@link #HEALTH} carries no credential by design, and {@link #CALLBACK}
 * authenticates with its own per-step HMAC token rather than a caller credential. Exemption is
 * modelled here rather than as a separate path allowlist so that "this route needs no auth" and
 * "this route was never given a policy" stay distinguishable — they require opposite outcomes, and
 * {@link #fromRouteRoles} denies the latter.
 */
public enum SagaOperation implements RouteRole {

  /** The liveness probe. Unauthenticated by design, so K8s-native probes need no credential. */
  HEALTH(null, false),

  /**
   * A participant's async step-completion callback. Authenticated by its own per-step HMAC token,
   * and never rate-limited — a callback must not be throttled by a user's saga-start budget.
   */
  CALLBACK(null, false),

  /** Starting a saga, in either the create ({@code POST}) or versioned ({@code PUT}) form. */
  START_SAGA(SagaRole.WRITE, true),

  /** Reading a saga's state snapshot. */
  GET_SAGA(SagaRole.READ, false),

  /**
   * Polling a saga until it reaches a terminal state. gRPC only; REST exposes no await endpoint.
   */
  AWAIT_SAGA(SagaRole.READ, false);

  private final @Nullable SagaRole requiredRole;
  private final boolean rateLimited;

  SagaOperation(@Nullable SagaRole requiredRole, boolean rateLimited) {
    this.requiredRole = requiredRole;
    this.rateLimited = rateLimited;
  }

  /**
   * Returns the minimum role a caller must hold to invoke this operation, or {@code null} if the
   * operation is exempt from authentication entirely (see the class javadoc). Callers must handle
   * the {@code null} case explicitly rather than defaulting it to a role.
   */
  public @Nullable SagaRole requiredRole() {
    return requiredRole;
  }

  /** Returns whether invoking this operation consumes the caller's rate-limit budget. */
  public boolean rateLimited() {
    return rateLimited;
  }

  /**
   * Resolves the operation a matched route declares, from the roles Javalin attached to it.
   *
   * <p><b>This deliberately inverts the documented Javalin idiom, and must not be "corrected" to
   * match it.</b> Javalin's migration guide suggests returning early when {@code routeRoles()} is
   * empty, which serves any untagged route without a check. Here an untagged (or ambiguously
   * tagged) route is a <b>programming error</b> — a route was registered with no policy — so it is
   * rejected instead. Failing closed matters because every admin mutation is a {@code POST}: the
   * old verb-keyed default would have served one to any {@code saga:write} caller.
   *
   * <p>Rejecting is also stronger than defaulting to {@link SagaRole#ADMIN}, which only fails
   * closed when a real provider is configured — {@link NoopSecurityProvider} grants every role,
   * ADMIN included, so an ADMIN default would serve an untagged route to an anonymous caller.
   *
   * <p>An untagged route makes the endpoint unreachable rather than open, which is the correct
   * direction to fail; {@code TransportPolicyParityTest} turns it into a build failure so it cannot
   * reach production.
   *
   * @param routeRoles the matched route's declared roles, from {@code Context.routeRoles()}
   * @return the single {@link SagaOperation} the route declares
   * @throws IllegalStateException if the route declares no operation, or more than one
   */
  public static SagaOperation fromRouteRoles(Set<RouteRole> routeRoles) {
    SagaOperation resolved = null;
    for (RouteRole role : routeRoles) {
      if (role instanceof SagaOperation operation) {
        if (resolved != null) {
          throw new IllegalStateException(
              "Route declares more than one SagaOperation ("
                  + resolved
                  + ", "
                  + operation
                  + "); its policy is ambiguous, so the request is refused");
        }
        resolved = operation;
      }
    }
    if (resolved == null) {
      throw new IllegalStateException(
          "Route declares no SagaOperation, so no access policy applies to it; refusing the request"
              + " rather than serving it unchecked");
    }
    return resolved;
  }
}
