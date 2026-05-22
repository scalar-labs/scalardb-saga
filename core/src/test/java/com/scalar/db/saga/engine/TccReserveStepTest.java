package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import org.junit.jupiter.api.Test;

class TccReserveStepTest {

  @Test
  void getName_called_returnsTccStepNameWithReserveSuffix() {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    when(tccStep.getName()).thenReturn("payment");
    TccReserveStep step = new TccReserveStep(tccStep);

    // Act & Assert
    assertThat(step.getName()).isEqualTo("payment.reserve");
  }

  @Test
  void execute_called_delegatesToReserve() throws StepExecutionException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    StepResult expected = StepResult.of("key", "value");
    when(tccStep.reserve(context)).thenReturn(expected);
    TccReserveStep step = new TccReserveStep(tccStep);

    // Act
    StepResult result = step.execute(context);

    // Assert
    assertThat(result).isSameAs(expected);
    verify(tccStep).reserve(context);
  }

  @Test
  void compensate_called_delegatesToCancel() throws StepCompensationException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    TccReserveStep step = new TccReserveStep(tccStep);

    // Act
    step.compensate(context);

    // Assert
    verify(tccStep).cancel(context);
  }

  @Test
  void execute_reserveThrows_propagatesException() throws StepExecutionException {
    // Arrange
    TccStep tccStep = mock(TccStep.class);
    SagaContext context = mock(SagaContext.class);
    StepExecutionException exception = new StepExecutionException("reserve failed", true);
    when(tccStep.reserve(context)).thenThrow(exception);
    TccReserveStep step = new TccReserveStep(tccStep);

    // Act & Assert
    assertThatThrownBy(() -> step.execute(context)).isSameAs(exception);
  }
}
