package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.HttpCall;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeclarativeBindingStepTest {

  private static final HttpCall EXECUTION = HttpCall.newBuilder("/debit").build();
  private static final HttpCall COMPENSATION = HttpCall.newBuilder("/reverse").build();
  private static final SagaContext CTX = new FakeSagaContext("saga-1", Map.of());

  private static DeclarativeBindingStep adapter(TransportAdapter transport) {
    return new DeclarativeBindingStep(
        "debit", transport, Map.of(Phase.EXECUTION, EXECUTION, Phase.COMPENSATION, COMPENSATION));
  }

  @Test
  void getName_returnsStepName() {
    // Act & Assert
    assertThat(adapter(mock(TransportAdapter.class)).getName()).isEqualTo("debit");
  }

  @Test
  void execute_callsExecutionSpec_returnsStepResultFromOutput() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(eq(EXECUTION), any(), eq("debit"))).thenReturn(Map.of("debitId", "DBT-1"));

    // Act
    StepResult result = adapter(transport).execute(CTX);

    // Assert
    assertThat(result.getOutput()).containsEntry("debitId", "DBT-1");
    verify(transport).call(eq(EXECUTION), any(), eq("debit"));
  }

  @Test
  void execute_transportRetryable_throwsRetryableStepExecutionException() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("busy", true));

    // Act
    Throwable thrown = catchThrowable(() -> adapter(transport).execute(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).isRetryable()).isTrue();
  }

  @Test
  void execute_transportNonRetryable_throwsNonRetryableStepExecutionException() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("bad", false));

    // Act
    Throwable thrown = catchThrowable(() -> adapter(transport).execute(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).isRetryable()).isFalse();
  }

  @Test
  void compensate_callsCompensationSpec() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);

    // Act
    adapter(transport).compensate(CTX);

    // Assert
    verify(transport).call(eq(COMPENSATION), any(), eq("debit"));
  }

  @Test
  void compensate_transportException_throwsStepCompensationException() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("undo", false));

    // Act & Assert
    assertThat(catchThrowable(() -> adapter(transport).compensate(CTX)))
        .isInstanceOf(StepCompensationException.class);
  }
}
