package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.HttpCall;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DeclarativeBindingStepTest {

  private static final HttpCall EXECUTION = HttpCall.newBuilder("/debit").build();
  private static final HttpCall COMPENSATION = HttpCall.newBuilder("/reverse").build();
  private static final SagaContext CTX = new FakeSagaContext("saga-1", Map.of());

  private static DeclarativeBindingStep adapter(TransportAdapter transport) {
    return new DeclarativeBindingStep(
        "debit",
        service -> transport,
        "account",
        Map.of(Phase.EXECUTION, EXECUTION, Phase.COMPENSATION, COMPENSATION));
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
    when(transport.call(eq(EXECUTION), any(), eq("debit")))
        .thenReturn(StepResult.of(Map.of("debitId", "DBT-1")));

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
  void execute_transportKnownNotCommitted_propagatesFlag() throws Exception {
    // Arrange — a proven non-delivery rides the TransportException into the step exception.
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any()))
        .thenThrow(new TransportException("refused", new RuntimeException(), true, true));

    // Act
    Throwable thrown = catchThrowable(() -> adapter(transport).execute(CTX));

    // Assert — the engine may skip the failed step from compensation; retryable is independent.
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).knownNotCommitted()).isTrue();
    assertThat(((StepExecutionException) thrown).isRetryable()).isTrue();
  }

  @Test
  void execute_transportCommitted_flagIsFalse() throws Exception {
    // Arrange — an unproven failure (the default) → the failed step must be compensated.
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("in-doubt", false));

    // Act
    Throwable thrown = catchThrowable(() -> adapter(transport).execute(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).knownNotCommitted()).isFalse();
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

  @Test
  void execute_resolveMiss_throwsRetryableKnownNotCommittedStepExecutionException() {
    // Arrange — the resolver reports no endpoint for the service (removed, or configuration not
    // yet propagated): a pre-send miss that must be retryable and known-not-committed.
    DeclarativeBindingStep step =
        new DeclarativeBindingStep(
            "debit",
            service -> {
              throw new TransportException("no endpoint for " + service, true, true);
            },
            "account",
            Map.of(Phase.EXECUTION, EXECUTION, Phase.COMPENSATION, COMPENSATION));

    // Act
    Throwable thrown = catchThrowable(() -> step.execute(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).isRetryable()).isTrue();
    assertThat(((StepExecutionException) thrown).knownNotCommitted()).isTrue();
  }

  @Test
  void compensate_resolveMiss_throwsStepCompensationException() {
    // Arrange — a resolve miss on the compensation path surfaces as a compensation failure, which
    // the engine never retries inline (recovery retries the compensation later).
    DeclarativeBindingStep step =
        new DeclarativeBindingStep(
            "debit",
            service -> {
              throw new TransportException("no endpoint for " + service, true, true);
            },
            "account",
            Map.of(Phase.EXECUTION, EXECUTION, Phase.COMPENSATION, COMPENSATION));

    // Act
    Throwable thrown = catchThrowable(() -> step.compensate(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepCompensationException.class);
  }

  @Test
  void execute_resolvesPerCall_eachCallSeesTheCurrentAdapter() throws Exception {
    // Arrange — the resolver's answer changes between calls, as a configuration swap would make it
    AtomicReference<TransportAdapter> current = new AtomicReference<>();
    TransportAdapter first = mock(TransportAdapter.class);
    TransportAdapter second = mock(TransportAdapter.class);
    when(first.call(any(), any(), any())).thenReturn(StepResult.empty());
    when(second.call(any(), any(), any())).thenReturn(StepResult.empty());
    DeclarativeBindingStep step =
        new DeclarativeBindingStep(
            "debit",
            service -> java.util.Objects.requireNonNull(current.get()),
            "account",
            Map.of(Phase.EXECUTION, EXECUTION, Phase.COMPENSATION, COMPENSATION));

    // Act
    current.set(first);
    step.execute(CTX);
    current.set(second);
    step.execute(CTX);

    // Assert — late binding: one call each, not two on the first
    verify(first).call(eq(EXECUTION), any(), eq("debit"));
    verify(second).call(eq(EXECUTION), any(), eq("debit"));
  }
}
