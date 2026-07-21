package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the shared recovery decision {@link RecoveryActionResolver#resolve}. These pin the
 * exact pivot-aware, {@code knownNotCommitted}-aware "compensate-from-N vs resume-from-M" rules so
 * the recovery manager and the Admin API can never diverge from them.
 */
class RecoveryActionResolverTest {

  private static final String KNOWN_NOT_COMMITTED = "{\"knownNotCommitted\":true}";

  // A 2-step BACKWARD saga: pivot = the last step (index 1).
  private static SagaDefinition backwardDef() {
    return SagaDefinition.newBuilder("order-saga")
        .saga()
        .step("debit", "com.example.DebitStep")
        .add()
        .step("credit", "com.example.CreditStep")
        .add()
        .build();
  }

  // A 2-step FORWARD saga: pivot = -1, so every step is post-pivot.
  private static SagaDefinition forwardDef() {
    return SagaDefinition.newBuilder("order-saga")
        .saga()
        .recoveryStrategy(RecoveryStrategy.FORWARD)
        .step("debit", "com.example.DebitStep")
        .add()
        .step("credit", "com.example.CreditStep")
        .add()
        .build();
  }

  @Test
  public void resolve_runningCleanForwardGiven_resumesFromNextStep() {
    // Arrange — step 0 completed, no failure.
    List<SagaEvent> events =
        List.of(StatusEvent.started(null), StepEvent.completed(0, "debit", null));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.RUNNING);

    // Assert — resume after the highest completed step.
    assertThat(action).isEqualTo(new RecoveryAction.Resume(1));
  }

  @Test
  public void resolve_runningNoStepsCompletedGiven_resumesFromZero() {
    // Arrange — nothing completed yet.
    List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(0, "debit"));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.RUNNING);

    // Assert
    assertThat(action).isEqualTo(new RecoveryAction.Resume(0));
  }

  @Test
  public void resolve_runningInDoubtPrePivotFailureGiven_compensatesIncludingFailedStep() {
    // Arrange — step 1's pre-pivot forward failure is in-doubt (null payload => not
    // knownNotCommitted).
    List<SagaEvent> events =
        List.of(StepEvent.completed(0, "debit", null), StepEvent.failed(1, "credit", null));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.RUNNING);

    // Assert — the possibly-committed failed step (1) is included.
    assertThat(action).isEqualTo(new RecoveryAction.Compensate(1));
  }

  @Test
  public void resolve_runningKnownNotCommittedPrePivotFailureGiven_compensatesSkippingFailedStep() {
    // Arrange — step 1's failure proved non-delivery (knownNotCommitted=true).
    List<SagaEvent> events =
        List.of(
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", KNOWN_NOT_COMMITTED));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.RUNNING);

    // Assert — the proven-non-delivery step (1) is skipped; compensate from the highest completed
    // (0).
    assertThat(action).isEqualTo(new RecoveryAction.Compensate(0));
  }

  @Test
  public void resolve_runningPostPivotFailureGiven_resumesForward() {
    // Arrange — FORWARD saga (pivot = -1): the failed step is post-pivot, so it is re-attempted.
    List<SagaEvent> events =
        List.of(StepEvent.completed(0, "debit", null), StepEvent.failed(1, "credit", null));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, forwardDef(), SagaStatus.RUNNING);

    // Assert
    assertThat(action).isEqualTo(new RecoveryAction.Resume(1));
  }

  @Test
  public void resolve_compensatingWithCompensatedStepGiven_compensatesBelowLowest() {
    // Arrange — compensation already reached step 1.
    List<SagaEvent> events =
        List.of(
            StepEvent.completed(0, "debit", null),
            StepEvent.completed(1, "credit", null),
            StepEvent.compensated(1, "credit"));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.COMPENSATING);

    // Assert — continue one below the lowest already-compensated step.
    assertThat(action).isEqualTo(new RecoveryAction.Compensate(0));
  }

  @Test
  public void resolve_compensatingNoneCompensatedYetGiven_compensatesFromHighestCompleted() {
    // Arrange — crashed after the COMPENSATING transition, before compensating any step.
    List<SagaEvent> events =
        List.of(StepEvent.completed(0, "debit", null), StepEvent.completed(1, "credit", null));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.COMPENSATING);

    // Assert
    assertThat(action).isEqualTo(new RecoveryAction.Compensate(1));
  }

  @Test
  public void
      resolve_compensatingNoneCompensatedFailedAboveCompletedGiven_compensatesFromFailedStep() {
    // Arrange — crashed after the COMPENSATING transition, nothing compensated; a
    // possibly-committed
    // pre-pivot failure (step 1, null payload => not knownNotCommitted) sits above the highest
    // completed step (0). The failedIndicesToCompensate term must dominate the highest-completed
    // one
    // — the exact case the old inline DefaultSagaOrchestrator.compensate copy dropped.
    List<SagaEvent> events =
        List.of(StepEvent.completed(0, "debit", null), StepEvent.failed(1, "credit", null));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.COMPENSATING);

    // Assert — compensate from the possibly-committed failed step (1), not the highest completed
    // (0).
    assertThat(action).isEqualTo(new RecoveryAction.Compensate(1));
  }

  // --- ESCALATED: direction reconstructed from the stream (Admin API un-escalation) ---

  @Test
  public void resolve_escalatedThatWasCompensatingGiven_continuesCompensation() {
    // Arrange — escalated out of COMPENSATING (a SAGA_COMPENSATING event is in the stream), step 1
    // already compensated.
    List<SagaEvent> events =
        List.of(
            StepEvent.completed(0, "debit", null),
            StepEvent.completed(1, "credit", null),
            StatusEvent.compensating(),
            StepEvent.compensated(1, "credit"));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.ESCALATED);

    // Assert — reconstructed as COMPENSATING; continue one below the lowest compensated step.
    assertThat(action).isEqualTo(new RecoveryAction.Compensate(0));
  }

  @Test
  public void resolve_escalatedPostPivotFailureGiven_resumesForward() {
    // Arrange — FORWARD saga, failed post-pivot, never compensated -> reconstructed as RUNNING.
    List<SagaEvent> events =
        List.of(StepEvent.completed(0, "debit", null), StepEvent.failed(1, "credit", null));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, forwardDef(), SagaStatus.ESCALATED);

    // Assert — resume forward from the step after the highest completed one.
    assertThat(action).isEqualTo(new RecoveryAction.Resume(1));
  }

  @Test
  public void resolve_escalatedPrePivotFailureGiven_compensatesIncludingFailedStep() {
    // Arrange — pre-pivot failure, never reached COMPENSATING -> reconstructed as RUNNING, but the
    // unresolved pre-pivot failure means compensate.
    List<SagaEvent> events =
        List.of(StepEvent.completed(0, "debit", null), StepEvent.failed(1, "credit", null));

    // Act
    RecoveryAction action =
        RecoveryActionResolver.resolve(events, backwardDef(), SagaStatus.ESCALATED);

    // Assert
    assertThat(action).isEqualTo(new RecoveryAction.Compensate(1));
  }
}
