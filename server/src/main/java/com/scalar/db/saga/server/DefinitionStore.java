package com.scalar.db.saga.server;

import com.scalar.db.saga.definition.SagaDefinition;
import org.jspecify.annotations.Nullable;

/**
 * The definition store as a reload pass needs it: register a definition, and ask which definition
 * of a name actually serves new starts.
 *
 * <p>Both halves are necessary because registering does not always change what serves. Registered
 * content is immutable and the store is append-only, so re-writing an older version's file
 * registers nothing and the newer version keeps winning — a pass that only registered would go on
 * describing a version nobody runs.
 */
interface DefinitionStore {

  /** Registers {@code definition}, or does nothing if that exact version is already stored. */
  void register(SagaDefinition definition);

  /**
   * The definition of {@code sagaName} a name-only start would run, or {@code null} when nothing is
   * registered under that name.
   *
   * <p>The whole definition rather than its version, because a pass that finds the files
   * disagreeing with the store has to go on validating something, and the only safe thing to
   * validate is what serves.
   */
  @Nullable SagaDefinition latest(String sagaName);

  /**
   * Whether {@code version} of {@code sagaName} is already stored. With {@link #latest} it
   * separates an ordinary upgrade — a new version about to become the latest — from a rollback to a
   * version that is already there and will therefore register nothing.
   */
  boolean isRegistered(String sagaName, String version);
}
