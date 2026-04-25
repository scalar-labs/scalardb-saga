package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import org.junit.jupiter.api.Test;

class TccConfirmStepTest {

  @Test
  void getName_called_returnsTccStepNameWithConfirmSuffix() {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    when(tccStep.getName()).thenReturn("payment");
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act & Assert
    assertThat(step.getName()).isEqualTo("payment.confirm");
  }

  @Test
  void execute_called_delegatesToConfirmAndReturnsEmpty() throws StepExecutionException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act
    StepResult result = step.execute(context);

    // Assert
    verify(tccStep).confirm(context);
    assertThat(result).isEqualTo(StepResult.empty());
  }

  @Test
  void compensate_called_isNoOp() throws StepCompensationException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act
    step.compensate(context);

    // Assert
    verifyNoInteractions(tccStep);
  }

  @Test
  void execute_confirmThrows_propagatesException() throws StepExecutionException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    StepExecutionException exception = new StepExecutionException("confirm failed", true);
    // Use doThrow since confirm returns void
    doThrow(exception).when(tccStep).confirm(context);
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act & Assert
    assertThatThrownBy(() -> step.execute(context)).isSameAs(exception);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullTccStepGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new TccConfirmStep(null)).isInstanceOf(NullPointerException.class);
  }
}
