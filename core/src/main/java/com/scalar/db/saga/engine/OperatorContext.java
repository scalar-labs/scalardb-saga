package com.scalar.db.saga.engine;

/**
 * Supplies the identity of the operator performing an admin mutation, so {@link
 * DefaultSagaAdminService} can stamp it on the audit record. The identity is <b>injected by the
 * server</b>, never passed in by the caller — a caller cannot forge who they are in the audit.
 *
 * <p>Embedded mode supplies a fixed principal (there is no authenticated user); the daemon wires
 * this to the authenticated request identity. The returned value must be non-blank — the admin
 * service refuses to write an anonymous audit record.
 */
@FunctionalInterface
public interface OperatorContext {

  /** Returns the current operator's identity (non-blank). */
  String currentOperator();
}
