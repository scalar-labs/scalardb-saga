package com.scalar.db.saga.server;

import com.scalar.db.saga.definition.SagaDefinition;
import org.jspecify.annotations.Nullable;

/**
 * The definition store as {@code --validate-config} has it: there is none.
 *
 * <p>Validation reads the store to find what a name is <i>actually serving</i>, so that a file
 * naming an already-registered version is validated against the version that runs rather than the
 * one the file names. Offline there is no such thing, and answering "nothing is registered" is the
 * honest reading: every definition is judged on the version its own file names, which is all a set
 * of files can be judged on by itself.
 *
 * <p>What that costs is real and is why the report enumerates it — an un-bumped change, a rollback,
 * and a version conflicting with stored content are all invisible without the store. It is a
 * narrower answer than the daemon's, never a different one.
 */
final class NoDefinitionStore implements DefinitionStore {

  /**
   * Never reached: validation stops before a pass applies anything. It throws rather than doing
   * nothing so that a change which accidentally registers during validation fails loudly here,
   * instead of silently writing to a store the caller said it did not have.
   */
  @Override
  public void register(SagaDefinition definition) {
    throw new AssertionError("A configuration validation must not register a definition");
  }

  @Override
  public @Nullable SagaDefinition latest(String sagaName) {
    return null;
  }

  @Override
  public boolean isRegistered(String sagaName, String version) {
    return false;
  }
}
