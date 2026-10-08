package com.scalar.db.saga.server.security;

import java.util.Optional;

/**
 * A role granted to an authenticated caller, gating access to the daemon's endpoints (RBAC).
 *
 * <p>The three roles are <b>hierarchical</b>: {@link #ADMIN} subsumes {@link #WRITE}, which
 * subsumes {@link #READ}. A caller holding a higher role therefore satisfies any endpoint that
 * requires a lower one — see {@link #implies(SagaRole)}. Each endpoint declares the
 * <em>minimum</em> role it requires:
 *
 * <ul>
 *   <li>{@link #READ} ({@code saga:read}) — read saga state ({@code GET /sagas/{id}}).
 *   <li>{@link #WRITE} ({@code saga:write}) — start/drive sagas ({@code POST}/{@code PUT /sagas}).
 *   <li>{@link #ADMIN} ({@code saga:admin}) — operator actions (list, recover, force-complete,
 *       reset, and future admin endpoints).
 * </ul>
 *
 * <p>A {@link SagaSecurityProvider} maps its own credential claims (JWT scopes/roles, an API key's
 * configured role set) onto this enum, so the RBAC check is provider-agnostic. The wire name — the
 * {@code saga:read}/{@code saga:write}/{@code saga:admin} string a provider matches against — is
 * {@link #wireName()}.
 */
public enum SagaRole {
  /** Reads saga state by id; wire name {@code saga:read}. */
  READ("saga:read", 0),

  /** Starts sagas, and everything {@link #READ} grants; wire name {@code saga:write}. */
  WRITE("saga:write", 1),

  /**
   * Operator actions (list, recover, force-complete, reset), and everything {@link #WRITE} grants;
   * wire name {@code saga:admin}.
   */
  ADMIN("saga:admin", 2);

  private final String wireName;
  private final int privilege;

  SagaRole(String wireName, int privilege) {
    this.wireName = wireName;
    this.privilege = privilege;
  }

  /**
   * Returns the {@code saga:<action>} wire name a provider matches a credential's claims against
   * (e.g. {@code "saga:write"}).
   *
   * @return the wire name
   */
  public String wireName() {
    return wireName;
  }

  /**
   * Returns the role whose {@link #wireName()} equals {@code wireName} (e.g. {@code "saga:write"} →
   * {@link #WRITE}), or empty if none matches. Providers use this to map a credential's claim
   * values (JWT scopes, an API key's configured roles) onto the RBAC roles.
   *
   * @param wireName the claim value to match, exactly as the credential carries it
   * @return the matching role, or empty if the value names none
   */
  public static Optional<SagaRole> fromWireName(String wireName) {
    for (SagaRole role : values()) {
      if (role.wireName.equals(wireName)) {
        return Optional.of(role);
      }
    }
    return Optional.empty();
  }

  /**
   * Returns whether holding this role satisfies a requirement for {@code required}, honoring the
   * {@link #ADMIN} &gt; {@link #WRITE} &gt; {@link #READ} hierarchy. For example {@code
   * ADMIN.implies(READ)} is {@code true}; {@code READ.implies(WRITE)} is {@code false}. A role
   * always implies itself.
   *
   * @param required the role an operation requires
   * @return {@code true} if this role is {@code required} or ranks above it
   */
  public boolean implies(SagaRole required) {
    // Higher privilege grants everything at or below it (READ=0 < WRITE=1 < ADMIN=2). An explicit
    // privilege field (not ordinal) keeps the ordering independent of declaration order.
    return this.privilege >= required.privilege;
  }
}
