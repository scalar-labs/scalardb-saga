package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.exception.SagaDefinitionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SagaDefinitionTest {

  @Nested
  class Builder {

    @Test
    void build_withSagaMode_usesDefaultValues() {
      // Arrange & Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test-saga", SagaMode.SAGA)
              .step("step1", "com.example.Step1")
              .add()
              .build();

      // Assert
      assertThat(definition.getName()).isEqualTo("test-saga");
      assertThat(definition.getVersion()).isEqualTo("1.0");
      assertThat(definition.getMode()).isEqualTo(SagaMode.SAGA);
      assertThat(definition.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.BACKWARD);
      assertThat(definition.getTimeoutMillis()).isZero();
      assertThat(definition.getDefaultRetryPolicy()).isNull();
    }

    @Test
    void build_withTccMode_usesPredefinedRecoveryStrategy() {
      // Arrange & Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test-saga", SagaMode.TCC)
              .step("step1", "com.example.Step1")
              .add()
              .build();

      // Assert
      assertThat(definition.getMode()).isEqualTo(SagaMode.TCC);
      assertThat(definition.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.PREDEFINED);
    }

    @Test
    void build_withAllOptions_setsAllFields() {
      // Arrange
      RetryPolicy policy = RetryPolicy.defaultPolicy();

      // Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("order-saga", SagaMode.TCC)
              .version("2.0")
              .timeoutMillis(30_000)
              .defaultRetryPolicy(policy)
              .step("reserve", "com.example.ReserveStep")
              .timeoutMillis(5000)
              .add()
              .build();

      // Assert
      assertThat(definition.getName()).isEqualTo("order-saga");
      assertThat(definition.getVersion()).isEqualTo("2.0");
      assertThat(definition.getMode()).isEqualTo(SagaMode.TCC);
      assertThat(definition.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.PREDEFINED);
      assertThat(definition.getTimeoutMillis()).isEqualTo(30_000);
      assertThat(definition.getDefaultRetryPolicy()).isSameAs(policy);
    }

    @Test
    void step_stringStepClassGiven_setsClassName() {
      // Arrange & Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.MyStep")
              .add()
              .build();

      // Assert
      assertThat(definition.getSteps().get(0).getStepClass()).isEqualTo("com.example.MyStep");
    }

    @Test
    void step_classObjectGiven_setsFullyQualifiedName() {
      // Arrange & Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", DummyStep.class)
              .add()
              .build();

      // Assert
      assertThat(definition.getSteps().get(0).getStepClass()).isEqualTo(DummyStep.class.getName());
    }

    @Test
    void step_classNotImplementingStepOrTccStep_throwsIllegalArgumentException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () -> SagaDefinition.newBuilder("test", SagaMode.SAGA).step("s1", String.class))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void step_withRetryPolicy_setsPolicy() {
      // Arrange
      RetryPolicy policy =
          RetryPolicy.newBuilder().maxAttempts(5).initialIntervalMillis(2000).build();

      // Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .retryPolicy(policy)
              .add()
              .build();

      // Assert
      assertThat(definition.getSteps().get(0).getRetryPolicy()).isSameAs(policy);
    }

    @Test
    void build_withMultipleSteps_preservesOrder() {
      // Arrange & Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("first", "com.example.First")
              .add()
              .step("second", "com.example.Second")
              .add()
              .step("third", "com.example.Third")
              .add()
              .build();

      // Assert
      assertThat(definition.getSteps()).hasSize(3);
      assertThat(definition.getSteps().get(0).getName()).isEqualTo("first");
      assertThat(definition.getSteps().get(1).getName()).isEqualTo("second");
      assertThat(definition.getSteps().get(2).getName()).isEqualTo("third");
    }

    @Test
    void getSteps_afterBuild_returnsUnmodifiableList() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();

      // Act & Assert
      assertThatThrownBy(() -> definition.getSteps().add(null))
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Nested
  class Validate {

    @Test
    void newBuilder_blankSagaNameGiven_throwsIllegalArgumentException() {
      // Arrange & Act & Assert
      assertThatThrownBy(() -> SagaDefinition.newBuilder("  ", SagaMode.SAGA))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void step_blankStepNameGiven_throwsIllegalArgumentException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () -> SagaDefinition.newBuilder("test", SagaMode.SAGA).step("", "com.example.S1"))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void step_blankStepClassGiven_throwsIllegalArgumentException() {
      // Arrange & Act & Assert
      assertThatThrownBy(() -> SagaDefinition.newBuilder("test", SagaMode.SAGA).step("s1", "  "))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validate_noSteps_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(() -> SagaDefinition.newBuilder("empty", SagaMode.SAGA).build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_withNegativeSagaTimeout_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .timeoutMillis(-1)
                      .step("s1", "com.example.S1")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_withNegativeStepTimeout_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .step("s1", "com.example.S1")
                      .timeoutMillis(-100)
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_withZeroTimeout_succeeds() {
      // Arrange & Act — 0 means no timeout, should be valid
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .timeoutMillis(0)
              .step("s1", "com.example.S1")
              .timeoutMillis(0)
              .add()
              .build();

      // Assert
      assertThat(definition.getTimeoutMillis()).isZero();
      assertThat(definition.getSteps().get(0).getTimeoutMillis()).isZero();
    }

    @Test
    void validate_duplicateStepNames_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("dup", SagaMode.SAGA)
                      .step("step1", "com.example.Step1")
                      .add()
                      .step("step1", "com.example.Step1")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_backwardWithPivot_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .recoveryStrategy(RecoveryStrategy.BACKWARD)
                      .step("s1", "com.example.S1")
                      .pivot(true)
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_forwardWithPivot_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .recoveryStrategy(RecoveryStrategy.FORWARD)
                      .step("s1", "com.example.S1")
                      .pivot(true)
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_mixedWithNoPivot_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .recoveryStrategy(RecoveryStrategy.MIXED)
                      .step("s1", "com.example.S1")
                      .add()
                      .step("s2", "com.example.S2")
                      .add()
                      .step("s3", "com.example.S3")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_mixedWithMultiplePivots_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .recoveryStrategy(RecoveryStrategy.MIXED)
                      .step("s1", "com.example.S1")
                      .pivot(true)
                      .add()
                      .step("s2", "com.example.S2")
                      .pivot(true)
                      .add()
                      .step("s3", "com.example.S3")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_mixedWithPivotAtFirst_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .recoveryStrategy(RecoveryStrategy.MIXED)
                      .step("s1", "com.example.S1")
                      .pivot(true)
                      .add()
                      .step("s2", "com.example.S2")
                      .add()
                      .step("s3", "com.example.S3")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_mixedWithPivotAtLast_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .recoveryStrategy(RecoveryStrategy.MIXED)
                      .step("s1", "com.example.S1")
                      .add()
                      .step("s2", "com.example.S2")
                      .add()
                      .step("s3", "com.example.S3")
                      .pivot(true)
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_mixedWithValidPivot_succeeds() {
      // Arrange & Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .recoveryStrategy(RecoveryStrategy.MIXED)
              .step("s1", "com.example.S1")
              .add()
              .step("s2", "com.example.S2")
              .pivot(true)
              .add()
              .step("s3", "com.example.S3")
              .add()
              .build();

      // Assert
      assertThat(definition.getSteps()).hasSize(3);
    }

    @Test
    void validate_tccWithExplicitRecoveryStrategy_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.TCC)
                      .recoveryStrategy(RecoveryStrategy.FORWARD)
                      .step("s1", "com.example.S1")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_tccWithExplicitBackward_throwsSagaDefinitionException() {
      // Arrange & Act & Assert — even BACKWARD is rejected when explicitly set
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.TCC)
                      .recoveryStrategy(RecoveryStrategy.BACKWARD)
                      .step("s1", "com.example.S1")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_tccWithPivotStep_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.TCC)
                      .step("s1", "com.example.S1")
                      .pivot(true)
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void validate_tccWithValidDefinition_succeeds() {
      // Arrange & Act
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.TCC)
              .step("reserve-inventory", "com.example.ReserveInventory")
              .add()
              .step("reserve-payment", "com.example.ReservePayment")
              .add()
              .build();

      // Assert
      assertThat(definition.getMode()).isEqualTo(SagaMode.TCC);
      assertThat(definition.getSteps()).hasSize(2);
    }

    @Test
    void validate_sagaWithPredefinedStrategy_throwsSagaDefinitionException() {
      // Arrange & Act & Assert
      assertThatThrownBy(
              () ->
                  SagaDefinition.newBuilder("test", SagaMode.SAGA)
                      .recoveryStrategy(RecoveryStrategy.PREDEFINED)
                      .step("s1", "com.example.S1")
                      .add()
                      .build())
          .isInstanceOf(SagaDefinitionException.class);
    }
  }

  @Nested
  class EqualsHashCodeToString {

    @Test
    void equals_sameDefinitions_returnsTrue() {
      // Arrange
      SagaDefinition a =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();
      SagaDefinition b =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();

      // Act & Assert
      assertThat(a).isEqualTo(b);
    }

    @Test
    void equals_differentName_returnsFalse() {
      // Arrange
      SagaDefinition a =
          SagaDefinition.newBuilder("saga-a", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();
      SagaDefinition b =
          SagaDefinition.newBuilder("saga-b", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();

      // Act & Assert
      assertThat(a).isNotEqualTo(b);
    }

    @Test
    void equals_differentMode_returnsFalse() {
      // Arrange
      SagaDefinition saga =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();
      SagaDefinition tcc =
          SagaDefinition.newBuilder("test", SagaMode.TCC)
              .step("s1", "com.example.S1")
              .add()
              .build();

      // Act & Assert
      assertThat(saga).isNotEqualTo(tcc);
    }

    @Test
    void equals_nullGiven_returnsFalse() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();

      // Act & Assert
      assertThat(definition).isNotEqualTo(null);
    }

    @Test
    void hashCode_equalObjects_sameHashCode() {
      // Arrange
      SagaDefinition a =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();
      SagaDefinition b =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();

      // Act & Assert
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void toString_called_containsNameAndMode() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();

      // Act
      String result = definition.toString();

      // Assert
      assertThat(result).contains("order-saga");
      assertThat(result).contains("SAGA");
    }

    @Test
    void stepDefinitionEquals_sameStep_returnsTrue() {
      // Arrange
      SagaDefinition defA =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .recoveryStrategy(RecoveryStrategy.MIXED)
              .step("s1", "com.example.S1")
              .add()
              .step("s2", "com.example.S2")
              .pivot(true)
              .add()
              .step("s3", "com.example.S3")
              .add()
              .build();
      SagaDefinition defB =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .recoveryStrategy(RecoveryStrategy.MIXED)
              .step("s1", "com.example.S1")
              .add()
              .step("s2", "com.example.S2")
              .pivot(true)
              .add()
              .step("s3", "com.example.S3")
              .add()
              .build();

      // Act & Assert
      assertThat(defA.getSteps().get(1)).isEqualTo(defB.getSteps().get(1));
    }

    @Test
    void stepDefinitionEquals_differentStepClass_returnsFalse() {
      // Arrange
      SagaDefinition defA =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S1")
              .add()
              .build();
      SagaDefinition defB =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("s1", "com.example.S2")
              .add()
              .build();

      // Act & Assert
      assertThat(defA.getSteps().get(0)).isNotEqualTo(defB.getSteps().get(0));
    }

    @Test
    void stepDefinitionToString_called_containsName() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("reserve-inventory", "com.example.S1")
              .add()
              .build();

      // Act & Assert
      assertThat(definition.getSteps().get(0).toString()).contains("reserve-inventory");
    }
  }

  @Nested
  class GetPivotIndex {

    @Test
    void getPivotIndex_withBackwardStrategy_returnsLastIndex() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .recoveryStrategy(RecoveryStrategy.BACKWARD)
              .step("s1", "com.example.S1")
              .add()
              .step("s2", "com.example.S2")
              .add()
              .step("s3", "com.example.S3")
              .add()
              .build();

      // Act & Assert
      assertThat(definition.getPivotIndex()).isEqualTo(2);
    }

    @Test
    void getPivotIndex_withForwardStrategy_returnsMinusOne() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .recoveryStrategy(RecoveryStrategy.FORWARD)
              .step("s1", "com.example.S1")
              .add()
              .step("s2", "com.example.S2")
              .add()
              .build();

      // Act & Assert
      assertThat(definition.getPivotIndex()).isEqualTo(-1);
    }

    @Test
    void getPivotIndex_withMixedStrategy_returnsPivotStepIndex() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .recoveryStrategy(RecoveryStrategy.MIXED)
              .step("s1", "com.example.S1")
              .add()
              .step("s2", "com.example.S2")
              .pivot(true)
              .add()
              .step("s3", "com.example.S3")
              .add()
              .build();

      // Act & Assert
      assertThat(definition.getPivotIndex()).isEqualTo(1);
    }

    @Test
    void getPivotIndex_withTccMode_returnsLastIndex() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.TCC)
              .step("s1", "com.example.S1")
              .add()
              .step("s2", "com.example.S2")
              .add()
              .build();

      // Act & Assert
      assertThat(definition.getPivotIndex()).isEqualTo(1);
    }

    @Test
    void getPivotIndex_withBackwardAndSingleStep_returnsZero() {
      // Arrange
      SagaDefinition definition =
          SagaDefinition.newBuilder("test", SagaMode.SAGA)
              .step("only", "com.example.Only")
              .add()
              .build();

      // Act & Assert
      assertThat(definition.getPivotIndex()).isEqualTo(0);
    }
  }

  abstract static class DummyStep implements Step {}
}
