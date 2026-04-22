package com.scalar.db.saga.api;

import org.jspecify.annotations.Nullable;

/**
 * Shared data context passed to {@link Step} and {@link TccStep} implementations.
 *
 * <p>Provides read/write access to the saga's key-value data map. Engine-internal tracking (event
 * sequencing, state transitions, failure tracking) is in the {@code ExecutionContext}
 * implementation.
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
   * @return the value, or {@code null} if not present
   * @throws ClassCastException if the stored value cannot be converted to the requested type
   */
  <T> @Nullable T get(String key, Class<T> type);

  /**
   * Stores a value in the saga data map, available to subsequent steps.
   *
   * <p>Only the following types are allowed to ensure reliable JSON serialization across crash
   * recovery boundaries:
   *
   * <ul>
   *   <li>Primitives and their wrappers: {@code Boolean}, {@code Integer}, {@code Long}, {@code
   *       Double}, {@code Float}, {@code Short}, {@code Byte}
   *   <li>{@code String}
   *   <li>{@code List} and {@code Map} containing only the above types (nested collections are
   *       allowed)
   * </ul>
   *
   * @param key the data key
   * @param value the value to store
   * @throws IllegalArgumentException if the value type is not one of the allowed types listed above
   */
  void put(String key, Object value);
}
