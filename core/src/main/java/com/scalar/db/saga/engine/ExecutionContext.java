package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.StepResult;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import net.jcip.annotations.NotThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Engine-internal implementation of {@link SagaContext}.
 *
 * <p>Adds tracking fields for event sequencing, state transitions, and failure tracking that are
 * not visible to {@link com.scalar.db.saga.api.Step} implementations.
 *
 * <p><b>Thread safety:</b> This class is not thread-safe. It relies on thread confinement — one
 * instance per saga execution, with steps executing sequentially on a single virtual thread. If
 * parallel step execution is added in the future, the concurrency model must be redesigned (e.g.,
 * isolated data maps per branch with merge at join points).
 *
 * <p><b>Allowed value types:</b> Only primitives, strings, {@link BigDecimal}, and collections/maps
 * of these types are allowed. This restriction ensures reliable JSON serialization and
 * deserialization across crash recovery boundaries.
 */
@NotThreadSafe
class ExecutionContext implements SagaContext {

  private static final Set<Class<?>> ALLOWED_TYPES =
      Set.of(
          String.class,
          Integer.class,
          Long.class,
          Double.class,
          Float.class,
          Boolean.class,
          BigDecimal.class);

  private final String sagaId;
  private final Map<String, Object> data;
  private int nextEventSequence;
  private SagaStateSnapshot currentState;
  private final Set<Integer> failedStepIndices = new HashSet<>();

  ExecutionContext(String sagaId, Map<String, Object> input, SagaStateSnapshot currentState) {
    this.sagaId = Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(input, "input must not be null");
    input.values().forEach(this::validateType);
    this.data = new HashMap<>(input);
    this.currentState = Objects.requireNonNull(currentState, "currentState must not be null");
  }

  // --- SagaContext interface (user-facing) ---

  @Override
  public String getSagaId() {
    return sagaId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> @Nullable T get(String key, Class<T> type) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Object value = data.get(key);
    if (value == null) {
      return null;
    }
    if (type.isInstance(value)) {
      return (T) value;
    }
    // Numeric coercion: after crash recovery, numeric types may differ from what was stored
    if (value instanceof Number number) {
      Object coerced = coerceNumber(number, type);
      if (coerced != null) {
        return (T) coerced;
      }
    }
    throw new ClassCastException(
        "Cannot convert " + value.getClass().getName() + " to " + type.getName());
  }

  @Override
  public void put(String key, Object value) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(value, "value must not be null");
    validateType(value);
    data.put(key, value);
  }

  // --- Engine-internal (not accessible from Step implementations) ---

  int nextSequence() {
    return nextEventSequence;
  }

  void advanceSequence() {
    nextEventSequence++;
  }

  void setNextEventSequence(int seq) {
    this.nextEventSequence = seq;
  }

  SagaStateSnapshot getCurrentState() {
    return currentState;
  }

  void setCurrentState(SagaStateSnapshot state) {
    this.currentState = Objects.requireNonNull(state, "state must not be null");
  }

  void markStepFailed(int stepIndex) {
    failedStepIndices.add(stepIndex);
  }

  boolean hasFailureEvent(int stepIndex) {
    return failedStepIndices.contains(stepIndex);
  }

  void merge(StepResult result) {
    Objects.requireNonNull(result, "result must not be null");
    result.getOutput().values().forEach(this::validateType);
    data.putAll(result.getOutput());
  }

  Map<String, Object> getData() {
    return Map.copyOf(data);
  }

  private void validateType(Object value) {
    if (ALLOWED_TYPES.contains(value.getClass())) {
      return;
    }
    if (value instanceof List<?> list) {
      for (Object element : list) {
        if (element == null) {
          throw new IllegalArgumentException("SagaContext does not allow null elements in lists");
        }
        validateType(element);
      }
      return;
    }
    if (value instanceof Map<?, ?> map) {
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!(entry.getKey() instanceof String)) {
          throw new IllegalArgumentException(
              "SagaContext only accepts String keys in maps. Got: "
                  + (entry.getKey() == null ? "null" : entry.getKey().getClass().getName()));
        }
        if (entry.getValue() == null) {
          throw new IllegalArgumentException("SagaContext does not allow null values in maps");
        }
        validateType(entry.getValue());
      }
      return;
    }
    throw new IllegalArgumentException(
        "SagaContext only accepts primitives, strings, BigDecimal, and collections thereof. Got: "
            + value.getClass().getName());
  }

  // Handles type drift after crash recovery: JSON may deserialize Integer as Long (or vice versa)
  // and Float as Double. Floating-point → integral conversions are rejected to prevent silent
  // truncation (e.g., 99.95 → 99). Range checks guard the narrowing Long→Integer path.
  private static @Nullable Object coerceNumber(Number number, Class<?> targetType) {
    if (targetType == Integer.class || targetType == int.class) {
      if (number instanceof Double || number instanceof Float) {
        throw new ClassCastException(
            "Cannot convert " + number.getClass().getName() + " to Integer: lossy conversion");
      }
      long longValue = number.longValue();
      if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
        throw new ClassCastException(
            "Cannot convert " + number + " to Integer: value out of range");
      }
      return number.intValue();
    }
    if (targetType == Long.class || targetType == long.class) {
      if (number instanceof Double || number instanceof Float) {
        throw new ClassCastException(
            "Cannot convert " + number.getClass().getName() + " to Long: lossy conversion");
      }
      return number.longValue();
    }
    if (targetType == Double.class || targetType == double.class) {
      return number.doubleValue();
    }
    if (targetType == Float.class || targetType == float.class) {
      double doubleValue = number.doubleValue();
      if (doubleValue < -Float.MAX_VALUE || doubleValue > Float.MAX_VALUE) {
        throw new ClassCastException("Cannot convert " + number + " to Float: value out of range");
      }
      return number.floatValue();
    }
    if (targetType == BigDecimal.class) {
      return new BigDecimal(number.toString());
    }
    return null;
  }
}
