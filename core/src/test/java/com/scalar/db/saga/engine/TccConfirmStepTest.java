package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
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
  void execute_called_delegatesToConfirmAndReturnsItsResult() throws StepExecutionException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    StepResult confirmResult = StepResult.of(Map.of("confirmationId", "C-1"));
    when(tccStep.confirm(context)).thenReturn(confirmResult);
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act
    StepResult result = step.execute(context);

    // Assert
    verify(tccStep).confirm(context);
    assertThat(result).isSameAs(confirmResult);
  }

  @Test
  void execute_confirmReturnsPending_propagatesPending() throws StepExecutionException {
    // Arrange — an async confirmation (202 Accepted) parks the saga; the pending result must not be
    // swallowed.
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    when(tccStep.confirm(context)).thenReturn(StepResult.pending());
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act
    StepResult result = step.execute(context);

    // Assert
    assertThat(result.isPending()).isTrue();
  }

  @Test
  void compensate_called_throwsUnsupportedOperationException() {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act & Assert
    assertThatThrownBy(() -> step.compensate(context))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void execute_confirmThrows_propagatesException() throws StepExecutionException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    StepExecutionException exception = new StepExecutionException("confirm failed", true);
    when(tccStep.confirm(context)).thenThrow(exception);
    TccConfirmStep step = new TccConfirmStep(tccStep);

    // Act & Assert
    assertThatThrownBy(() -> step.execute(context)).isSameAs(exception);
  }
}
