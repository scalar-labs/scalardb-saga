package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStateAndEvents;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SagaDetailReader}: it pairs a saga's snapshot with a redacted timeline, and
 * the redaction rule — metadata plus failure error / intervention reason only, never a raw step
 * input/output payload — is the substantive behavior under test.
 */
class SagaDetailReaderTest {

  private static final String SAGA_ID = "s1";
  private static final Instant TS = Instant.parse("2026-07-18T10:00:00Z");

  private final SagaStore store = mock(SagaStore.class);

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot(SAGA_ID, "order-saga", status, "owner", "v1", TS, TS);
  }

  @Test
  void read_missingSaga_throwsNotFound() {
    // Arrange
    when(store.getStateWithEvents(SAGA_ID, Integer.MAX_VALUE)).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(() -> SagaDetailReader.read(store, SAGA_ID, Integer.MAX_VALUE))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void read_truncatedStream_forwardsBoundAndFlagsDetailTruncated() {
    // Arrange — the store reports the stream was cut to the newest events
    SagaStateSnapshot snap = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events = List.of(StepEvent.completed(7, "ship", "{}").withTimestamp(TS));
    when(store.getStateWithEvents(SAGA_ID, 1))
        .thenReturn(Optional.of(new SagaStateAndEvents(snap, events, true)));

    // Act
    SagaDetail detail = SagaDetailReader.read(store, SAGA_ID, 1);

    // Assert — the caller's bound reaches the store and the flag reaches the detail
    assertThat(detail.isTruncated()).isTrue();
    assertThat(detail.getTimeline()).hasSize(1);
    assertThat(detail.getTimeline().get(0).getStepName()).isEqualTo("ship");
  }

  @Test
  void read_mapsEventsToTimeline_omitsRawPayloadsExposesErrorsAndReasons() {
    // Arrange
    SagaStateSnapshot snap = snapshot(SagaStatus.COMPENSATING);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started("{\"amount\":100}").withTimestamp(TS),
            StepEvent.completed(0, "debit", "{\"balance\":900}").withTimestamp(TS),
            StepEvent.failed(1, "credit", "{\"message\":\"gateway down\"}").withTimestamp(TS),
            StatusEvent.escalated("retries exhausted").withTimestamp(TS),
            StatusEvent.recovering(SagaStatus.COMPENSATING, "bob", "rolling back")
                .withTimestamp(TS));
    when(store.getStateWithEvents(SAGA_ID, Integer.MAX_VALUE))
        .thenReturn(Optional.of(new SagaStateAndEvents(snap, events, false)));

    // Act
    SagaDetail detail = SagaDetailReader.read(store, SAGA_ID, Integer.MAX_VALUE);

    // Assert — the snapshot and timeline come from the one atomic read
    assertThat(detail.getSnapshot()).isEqualTo(snap);
    assertThat(detail.isTruncated()).isFalse();
    List<TimelineEvent> timeline = detail.getTimeline();
    assertThat(timeline).hasSize(5);

    // SAGA_STARTED — the saga input payload is never exposed
    assertThat(timeline.get(0).getType()).isEqualTo("SAGA_STARTED");
    assertThat(timeline.get(0).getDetail()).isNull();
    assertThat(timeline.get(0).getResultingStatus()).isEqualTo(SagaStatus.RUNNING);

    // STEP_COMPLETED — the step output payload is never exposed
    assertThat(timeline.get(1).getType()).isEqualTo("STEP_COMPLETED");
    assertThat(timeline.get(1).getStepIndex()).isEqualTo(0);
    assertThat(timeline.get(1).getStepName()).isEqualTo("debit");
    assertThat(timeline.get(1).getDetail()).isNull();

    // STEP_FAILED — the error message is surfaced
    assertThat(timeline.get(2).getType()).isEqualTo("STEP_FAILED");
    assertThat(timeline.get(2).getDetail()).isEqualTo("gateway down");

    // SAGA_ESCALATED — the escalation reason is surfaced
    assertThat(timeline.get(3).getDetail()).isEqualTo("retries exhausted");

    // SAGA_RECOVERING — the operator and reason are surfaced
    assertThat(timeline.get(4).getType()).isEqualTo("SAGA_RECOVERING");
    assertThat(timeline.get(4).getResultingStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(timeline.get(4).getDetail()).isEqualTo("rolling back");
    assertThat(timeline.get(4).getOperator()).isEqualTo("bob");
  }
}
