package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaStepNotParkedExceptionTest {

  @Test
  void constructor_sagaIdAndStepNameGiven_carriesCodeMetadataAndRetryableMessage() {
    // Act
    SagaStepNotParkedException e = new SagaStepNotParkedException("saga-1", "charge");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_STEP_NOT_PARKED);
    assertThat(e.getErrorCode().category())
        .isEqualTo(SagaErrorCode.Category.RETRYABLE_SERVER_ERROR);
    assertThat(e.getMetadata())
        .containsEntry("saga_id", "saga-1")
        .containsEntry("step_name", "charge")
        .hasSize(2);
    assertThat(e.getMessage())
        .isEqualTo(
            "DB-SAGA-20007: The step has not finished parking yet"
                + " [saga_id=saga-1, step_name=charge]");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaStepNotParkedException("saga-1", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void fromWire_metadataGiven_reconstructsTheSameException() {
    // Arrange
    SagaStepNotParkedException original = new SagaStepNotParkedException("saga-1", "charge");

    // Act
    SagaStepNotParkedException rebuilt =
        SagaStepNotParkedException.fromWire(original.getMetadata());

    // Assert
    assertThat(rebuilt.getSagaId()).isEqualTo("saga-1");
    assertThat(rebuilt.getStepName()).isEqualTo("charge");
  }
}
