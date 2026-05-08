package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.StepResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ExecutionContextTest {

  private static final Instant NOW = Instant.now();
  private static final SagaStateSnapshot DEFAULT_STATE =
      new SagaStateSnapshot(
          "saga-1", "test-saga", SagaStatus.RUNNING, "owner-1", 0, "v1", NOW, NOW);

  private ExecutionContext createContext() {
    return new ExecutionContext("saga-1", new HashMap<>(), DEFAULT_STATE);
  }

  private ExecutionContext createContext(Map<String, Object> input) {
    return new ExecutionContext("saga-1", input, DEFAULT_STATE);
  }

  // --- SagaContext interface ---

  @Test
  void getSagaId_called_returnsSagaId() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThat(ctx.getSagaId()).isEqualTo("saga-1");
  }

  @Test
  void put_stringGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.put("name", "Alice");

    // Assert
    assertThat(ctx.get("name", String.class)).hasValue("Alice");
  }

  @Test
  void put_integerGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.put("count", 42);

    // Assert
    assertThat(ctx.get("count", Integer.class)).hasValue(42);
  }

  @Test
  void put_longGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.put("timestamp", 123456789L);

    // Assert
    assertThat(ctx.get("timestamp", Long.class)).hasValue(123456789L);
  }

  @Test
  void put_doubleGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.put("rate", 2.5);

    // Assert
    assertThat(ctx.get("rate", Double.class)).hasValue(2.5);
  }

  @Test
  void put_floatGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.put("score", 1.5f);

    // Assert
    assertThat(ctx.get("score", Float.class)).hasValue(1.5f);
  }

  @Test
  void put_booleanGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.put("active", true);

    // Assert
    assertThat(ctx.get("active", Boolean.class)).hasValue(true);
  }

  @Test
  void put_bigDecimalGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    BigDecimal amount = new BigDecimal("99.95");

    // Act
    ctx.put("amount", amount);

    // Assert
    assertThat(ctx.get("amount", BigDecimal.class)).hasValue(amount);
  }

  @SuppressWarnings("unchecked")
  @Test
  void put_listOfAllowedTypesGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    List<String> items = List.of("a", "b", "c");

    // Act
    ctx.put("items", items);

    // Assert
    assertThat(ctx.get("items", List.class)).hasValue(items);
  }

  @SuppressWarnings("unchecked")
  @Test
  void put_mapOfAllowedTypesGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    Map<String, Integer> scores = Map.of("alice", 100, "bob", 90);

    // Act
    ctx.put("scores", scores);

    // Assert
    assertThat(ctx.get("scores", Map.class)).hasValue(scores);
  }

  @SuppressWarnings("unchecked")
  @Test
  void put_nestedCollectionGiven_storesValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    List<Map<String, Integer>> nested = List.of(Map.of("x", 1), Map.of("y", 2));

    // Act
    ctx.put("nested", nested);

    // Assert
    assertThat(ctx.get("nested", List.class)).hasValue(nested);
  }

  @Test
  void put_customObjectGiven_throwsIllegalArgumentException() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThatThrownBy(() -> ctx.put("obj", new Object()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void put_listWithNullElementGiven_throwsIllegalArgumentException() {
    // Arrange
    ExecutionContext ctx = createContext();
    List<String> listWithNull = new java.util.ArrayList<>();
    listWithNull.add("a");
    listWithNull.add(null);

    // Act & Assert
    assertThatThrownBy(() -> ctx.put("items", listWithNull))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void put_mapWithNonStringKeyGiven_throwsIllegalArgumentException() {
    // Arrange
    ExecutionContext ctx = createContext();
    Map<Integer, String> badMap = Map.of(1, "a");

    // Act & Assert
    assertThatThrownBy(() -> ctx.put("map", badMap)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void put_mapWithNullValueGiven_throwsIllegalArgumentException() {
    // Arrange
    ExecutionContext ctx = createContext();
    Map<String, String> mapWithNull = new HashMap<>();
    mapWithNull.put("key", null);

    // Act & Assert
    assertThatThrownBy(() -> ctx.put("map", mapWithNull))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void put_nullKeyGiven_throwsNullPointerException() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThatThrownBy(() -> ctx.put(null, "value")).isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void put_nullValueGiven_throwsNullPointerException() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThatThrownBy(() -> ctx.put("key", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void get_missingKey_returnsEmpty() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThat(ctx.get("missing", String.class)).isEmpty();
  }

  @Test
  void get_numericCoercionIntToLong_returnsCoercedValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("value", 42);

    // Act & Assert
    assertThat(ctx.get("value", Long.class)).hasValue(42L);
  }

  @Test
  void get_numericCoercionLongToInt_returnsCoercedValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("value", 42L);

    // Act & Assert
    assertThat(ctx.get("value", Integer.class)).hasValue(42);
  }

  @Test
  void get_numericCoercionIntToDouble_returnsCoercedValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("value", 42);

    // Act & Assert
    assertThat(ctx.get("value", Double.class)).hasValue(42.0);
  }

  @Test
  void get_numericCoercionDoubleToBigDecimal_returnsCoercedValue() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("amount", 99.95);

    // Act
    Optional<BigDecimal> result = ctx.get("amount", BigDecimal.class);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualByComparingTo(new BigDecimal("99.95"));
  }

  @Test
  void get_numericCoercionDoubleOverflowsFloat_throwsClassCastException() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("big", Double.MAX_VALUE);

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("big", Float.class)).isInstanceOf(ClassCastException.class);
  }

  @Test
  void get_numericCoercionLongOverflowsInt_throwsClassCastException() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("big", Long.MAX_VALUE);

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("big", Integer.class)).isInstanceOf(ClassCastException.class);
  }

  @Test
  void get_numericCoercionDoubleToInteger_throwsClassCastException() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("value", 99.95);

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("value", Integer.class))
        .isInstanceOf(ClassCastException.class);
  }

  @Test
  void get_numericCoercionDoubleToLong_throwsClassCastException() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("value", 99.95);

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("value", Long.class)).isInstanceOf(ClassCastException.class);
  }

  @Test
  void get_bigDecimalToInteger_throwsClassCastException() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("amount", new BigDecimal("99.95"));

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("amount", Integer.class))
        .isInstanceOf(ClassCastException.class);
  }

  @Test
  void get_bigDecimalToLong_throwsClassCastException() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("amount", new BigDecimal("99.95"));

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("amount", Long.class)).isInstanceOf(ClassCastException.class);
  }

  @Test
  void get_incompatibleTypeRequested_throwsClassCastException() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("name", "Alice");

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("name", Integer.class)).isInstanceOf(ClassCastException.class);
  }

  // --- Engine-internal methods ---

  @Test
  void nextSequence_initial_returnsZero() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThat(ctx.nextSequence()).isEqualTo(0);
  }

  @Test
  void advanceSequence_called_incrementsSequence() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.advanceSequence();
    ctx.advanceSequence();

    // Assert
    assertThat(ctx.nextSequence()).isEqualTo(2);
  }

  @Test
  void setNextEventSequence_valueGiven_setsSequence() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.setNextEventSequence(5);

    // Assert
    assertThat(ctx.nextSequence()).isEqualTo(5);
  }

  @Test
  void getCurrentState_afterConstruction_returnsInitialState() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThat(ctx.getCurrentState()).isSameAs(DEFAULT_STATE);
  }

  @Test
  void setCurrentState_snapshotGiven_setsState() {
    // Arrange
    ExecutionContext ctx = createContext();
    SagaStateSnapshot newState =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.COMPENSATING, "owner-1", 0, "v1", NOW, NOW);

    // Act
    ctx.setCurrentState(newState);

    // Assert
    assertThat(ctx.getCurrentState()).isSameAs(newState);
  }

  @Test
  void markStepFailed_stepIndexGiven_tracksFailure() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act
    ctx.markStepFailed(2);

    // Assert
    assertThat(ctx.hasFailureEvent(2)).isTrue();
    assertThat(ctx.hasFailureEvent(0)).isFalse();
  }

  @Test
  void merge_stepResultGiven_mergesOutputIntoData() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("existing", "value");
    StepResult result = StepResult.of(Map.of("new_key", "new_value", "count", 42));

    // Act
    ctx.merge(result);

    // Assert
    assertThat(ctx.get("existing", String.class)).hasValue("value");
    assertThat(ctx.get("new_key", String.class)).hasValue("new_value");
    assertThat(ctx.get("count", Integer.class)).hasValue(42);
  }

  @Test
  void merge_emptyStepResultGiven_dataUnchanged() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("existing", "value");

    // Act
    ctx.merge(StepResult.empty());

    // Assert
    assertThat(ctx.get("existing", String.class)).hasValue("value");
  }

  @Test
  void constructor_inputMapGiven_populatesData() {
    // Arrange
    Map<String, Object> input = Map.of("orderId", "order-123", "amount", 100);

    // Act
    ExecutionContext ctx = createContext(input);

    // Assert
    assertThat(ctx.get("orderId", String.class)).hasValue("order-123");
    assertThat(ctx.get("amount", Integer.class)).hasValue(100);
  }

  @Test
  void getData_called_returnsUnmodifiableCopy() {
    // Arrange
    ExecutionContext ctx = createContext();
    ctx.put("key", "value");

    // Act
    Map<String, Object> data = ctx.getData();

    // Assert
    assertThat(data).containsEntry("key", "value");
    assertThatThrownBy(() -> data.put("new", "val"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void get_nullKeyGiven_throwsNullPointerException() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThatThrownBy(() -> ctx.get(null, String.class)).isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void get_nullTypeGiven_throwsNullPointerException() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThatThrownBy(() -> ctx.get("key", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void merge_resultWithDisallowedTypeGiven_throwsIllegalArgumentException() {
    // Arrange
    ExecutionContext ctx = createContext();
    StepResult result = StepResult.of(Map.of("bad", new Object()));

    // Act & Assert
    assertThatThrownBy(() -> ctx.merge(result)).isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void merge_nullResultGiven_throwsNullPointerException() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThatThrownBy(() -> ctx.merge(null)).isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void setCurrentState_nullGiven_throwsNullPointerException() {
    // Arrange
    ExecutionContext ctx = createContext();

    // Act & Assert
    assertThatThrownBy(() -> ctx.setCurrentState(null)).isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new ExecutionContext(null, Map.of(), DEFAULT_STATE))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullInputGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new ExecutionContext("saga-1", null, DEFAULT_STATE))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullInitialStateGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new ExecutionContext("saga-1", Map.of(), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_inputWithDisallowedTypeGiven_throwsIllegalArgumentException() {
    // Arrange
    Map<String, Object> input = Map.of("bad", new Object());

    // Act & Assert
    assertThatThrownBy(() -> new ExecutionContext("saga-1", input, DEFAULT_STATE))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
