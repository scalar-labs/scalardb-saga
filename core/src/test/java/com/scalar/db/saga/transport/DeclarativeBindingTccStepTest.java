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

class DeclarativeBindingTccStepTest {

  private static final HttpCall RESERVATION = HttpCall.newBuilder("/reserve").build();
  private static final HttpCall CONFIRMATION = HttpCall.newBuilder("/confirm").build();
  private static final HttpCall CANCELLATION = HttpCall.newBuilder("/cancel").build();
  private static final SagaContext CTX = new FakeSagaContext("saga-1", Map.of());

  private static DeclarativeBindingTccStep adapter(TransportAdapter transport) {
    return new DeclarativeBindingTccStep(
        "seat",
        service -> transport,
        "booking",
        Map.of(
            Phase.RESERVATION, RESERVATION,
            Phase.CONFIRMATION, CONFIRMATION,
            Phase.CANCELLATION, CANCELLATION));
  }

  @Test
  void reserve_callsReservationSpec_returnsStepResultFromOutput() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(eq(RESERVATION), any(), eq("seat.reserve")))
        .thenReturn(StepResult.of(Map.of("reservationId", "R-1")));

    // Act
    StepResult result = adapter(transport).reserve(CTX);

    // Assert
    assertThat(result.getOutput()).containsEntry("reservationId", "R-1");
    verify(transport).call(eq(RESERVATION), any(), eq("seat.reserve"));
  }

  @Test
  void reserve_transportRetryable_throwsRetryableStepExecutionException() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("busy", true));

    // Act
    Throwable thrown = catchThrowable(() -> adapter(transport).reserve(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).isRetryable()).isTrue();
  }

  @Test
  void reserve_transportKnownNotCommitted_propagatesFlag() throws Exception {
    // Arrange — a proven non-delivery on reserve rides into the step exception.
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any()))
        .thenThrow(new TransportException("refused", new RuntimeException(), true, true));

    // Act
    Throwable thrown = catchThrowable(() -> adapter(transport).reserve(CTX));

    // Assert — the engine may skip the failed reservation from cancellation.
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).knownNotCommitted()).isTrue();
  }

  @Test
  void reserve_transportCommitted_flagIsFalse() throws Exception {
    // Arrange — an unproven reserve failure (the default) → cancel the failed reservation too.
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("in-doubt", false));

    // Act
    Throwable thrown = catchThrowable(() -> adapter(transport).reserve(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).knownNotCommitted()).isFalse();
  }

  @Test
  void confirm_callsConfirmationSpec() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);

    // Act
    adapter(transport).confirm(CTX);

    // Assert
    verify(transport).call(eq(CONFIRMATION), any(), eq("seat.confirm"));
  }

  @Test
  void confirm_transportException_throwsStepExecutionException() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("confirm", false));

    // Act & Assert
    assertThat(catchThrowable(() -> adapter(transport).confirm(CTX)))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void cancel_callsCancellationSpec() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);

    // Act
    adapter(transport).cancel(CTX);

    // Assert
    verify(transport).call(eq(CANCELLATION), any(), eq("seat.cancel"));
  }

  @Test
  void cancel_transportException_throwsStepCompensationException() throws Exception {
    // Arrange
    TransportAdapter transport = mock(TransportAdapter.class);
    when(transport.call(any(), any(), any())).thenThrow(new TransportException("cancel", false));

    // Act & Assert
    assertThat(catchThrowable(() -> adapter(transport).cancel(CTX)))
        .isInstanceOf(StepCompensationException.class);
  }

  @Test
  void reserve_resolveMiss_throwsRetryableKnownNotCommittedStepExecutionException() {
    // Arrange
    DeclarativeBindingTccStep step =
        new DeclarativeBindingTccStep(
            "seat",
            service -> {
              throw new TransportException("no endpoint for " + service, true, true);
            },
            "booking",
            Map.of(
                Phase.RESERVATION, RESERVATION,
                Phase.CONFIRMATION, CONFIRMATION,
                Phase.CANCELLATION, CANCELLATION));

    // Act
    Throwable thrown = catchThrowable(() -> step.reserve(CTX));

    // Assert
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).isRetryable()).isTrue();
    assertThat(((StepExecutionException) thrown).knownNotCommitted()).isTrue();
  }

  @Test
  void reserveThenConfirm_swapBetweenPhases_confirmResolvesTheNewAdapter() throws Exception {
    // Arrange — the resolver's answer changes between reserve and confirm, as a configuration
    // swap between TCC phases would make it: reserve lands on the old endpoint, confirm on its
    // replacement (which is why endpoint changes must stay backward-compatible for in-flight
    // sagas).
    AtomicReference<TransportAdapter> current = new AtomicReference<>();
    TransportAdapter oldAdapter = mock(TransportAdapter.class);
    TransportAdapter newAdapter = mock(TransportAdapter.class);
    when(oldAdapter.call(any(), any(), any())).thenReturn(StepResult.empty());
    when(newAdapter.call(any(), any(), any())).thenReturn(StepResult.empty());
    DeclarativeBindingTccStep step =
        new DeclarativeBindingTccStep(
            "seat",
            service -> java.util.Objects.requireNonNull(current.get()),
            "booking",
            Map.of(
                Phase.RESERVATION, RESERVATION,
                Phase.CONFIRMATION, CONFIRMATION,
                Phase.CANCELLATION, CANCELLATION));

    // Act
    current.set(oldAdapter);
    step.reserve(CTX);
    current.set(newAdapter);
    step.confirm(CTX);

    // Assert
    verify(oldAdapter).call(eq(RESERVATION), any(), eq("seat.reserve"));
    verify(newAdapter).call(eq(CONFIRMATION), any(), eq("seat.confirm"));
  }
}
