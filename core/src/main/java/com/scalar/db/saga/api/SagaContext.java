package com.scalar.db.saga.api;

import java.util.Optional;

/**
 * Shared data context passed to {@link Step} and {@link TccStep} implementations.
 *
 * <p>Provides read-only access to the saga's key-value data map. Steps return output via {@link
 * StepResult}, which the engine merges into the context for subsequent steps. Engine-internal
 * tracking (event sequencing, state transitions, failure tracking) is in the {@code
 * ExecutionContext} implementation.
 */
public interface SagaContext {

  /** Returns the unique identifier of this saga instance. */
  String getSagaId();

  /**
   * Retrieves a value from the saga data map, casting it to the specified type.
   *
   * <p>Note: after crash recovery, numeric types may differ from what was originally stored (e.g.,
   * an {@code Integer} may be deserialized as a {@code Long}). Implementations should coerce
   * numeric types when the requested type differs from the stored type.
   *
   * @param key the data key
   * @param type the expected value type
   * @param <T> the value type
   * @return an {@link Optional} containing the value, or empty if not present
   * @throws ClassCastException if the stored value cannot be converted to the requested type
   */
  <T> Optional<T> get(String key, Class<T> type);
}
