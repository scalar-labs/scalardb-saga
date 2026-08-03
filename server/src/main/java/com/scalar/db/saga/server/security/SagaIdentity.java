package com.scalar.db.saga.server.security;

import java.util.Objects;
import java.util.Set;

/**
 * The authenticated caller a {@link SagaSecurityProvider} resolves a request to: a stable {@code
 * principal} (for audit) plus the set of {@link SagaRole}s the caller holds (for RBAC).
 *
 * <p>Immutable. The RBAC before-handler stores the resolved identity on the request so downstream
 * consumers — notably a future Admin API audit log — read the operator identity as a <b>caller-
 * supplied parameter</b> rather than reaching back into the {@link SagaSecurityProvider}. That
 * keeps the same core usable both in daemon mode (SPI) and embedded in a framework (e.g. Quarkus
 * MP-JWT).
 */
public final class SagaIdentity {

  private final String principal;
  private final Set<SagaRole> roles;

  private SagaIdentity(String principal, Set<SagaRole> roles) {
    this.principal = principal;
    // Copy inside the constructor so the field holds a provably-fresh immutable set: roles() can
    // then return it directly, and neither the stored reference nor the getter exposes caller
    // state.
    this.roles = Set.copyOf(roles);
  }

  /**
   * Creates an identity.
   *
   * @param principal a stable, non-blank identifier for the caller (e.g. a JWT {@code sub}, or an
   *     API key's configured principal name) — used for audit
   * @param roles the roles the caller holds; may be empty (an authenticated caller with no role
   *     passes authentication but is denied every role-gated endpoint)
   * @return the identity
   */
  public static SagaIdentity of(String principal, Set<SagaRole> roles) {
    Objects.requireNonNull(principal, "principal must not be null");
    Objects.requireNonNull(roles, "roles must not be null");
    if (principal.isBlank()) {
      throw new IllegalArgumentException("principal must not be blank");
    }
    return new SagaIdentity(principal, roles);
  }

  /** Returns the caller's stable principal identifier (for audit). */
  public String principal() {
    return principal;
  }

  /** Returns the immutable set of roles the caller holds. */
  public Set<SagaRole> roles() {
    return roles;
  }

  /**
   * Returns whether this caller satisfies {@code required}, honoring the role hierarchy — {@code
   * true} if any held role {@linkplain SagaRole#implies implies} {@code required}.
   */
  public boolean hasRole(SagaRole required) {
    Objects.requireNonNull(required, "required must not be null");
    for (SagaRole held : roles) {
      if (held.implies(required)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SagaIdentity that)) {
      return false;
    }
    return principal.equals(that.principal) && roles.equals(that.roles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, roles);
  }

  @Override
  public String toString() {
    return "SagaIdentity{principal='" + principal + "', roles=" + roles + '}';
  }
}
