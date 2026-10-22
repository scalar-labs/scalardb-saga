package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.integration.IntegrationTestStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Behavioural contract of {@link SagaStore}, exercised only through the interface against a real
 * ScalarDB. It asserts what callers observe (statuses, snapshots, which sweeps return a saga, which
 * writes are refused) and never how the store lays rows out, so it holds for any table layout that
 * keeps the contract. Listing and the operator events have their own suites ({@link
 * ScalarDbSagaStoreListIntegrationTest}, {@link ScalarDbSagaStoreAdminEventsIntegrationTest}).
 */
class ScalarDbSagaStoreContractIntegrationTest {

  private static final String OWNER = "engine-1";
  private static final String OTHER_OWNER = "engine-2";
  private static final Duration HOUR = Duration.ofHours(1);

  @TempDir Path tempDir;

  private Path dbPath;
  private SagaStore store;

  @BeforeEach
  void setUp() {
    dbPath = tempDir.resolve("saga-contract-it.db");
    Properties props = new Properties();
    IntegrationTestStore.configure(props, dbPath);
    // Several buckets, so sweeps have to walk the whole ring to find a saga.
    props.setProperty("scalar.db.saga.store.scalardb.num_buckets", "4");
    store = ScalarDbSagaStoreFactory.create(props).createStore();
  }

  @AfterEach
  void tearDown() throws Exception {
    store.close();
    Files.deleteIfExists(dbPath);
  }

  // ---------------------------------------------------------------------------
  // Create and read
  // ---------------------------------------------------------------------------

  @Test
  void createSaga_newIdGiven_returnsRunningSnapshotThatReadsBackTheSame() {
    // Act
    SagaStateSnapshot created = newSaga("saga-1");

    // Assert
    assertThat(created.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), created);
    assertThat(eventTypes("saga-1")).containsExactly(EventType.SAGA_STARTED);
  }

  @Test
  void createSaga_nullIdGiven_generatesAnIdThatReadsBack() {
    // Act
    SagaStateSnapshot created = store.createSaga(null, "order-saga", OWNER, Map.of(), "v1");

    // Assert
    assertThat(created.getSagaId()).isNotBlank();
    assertThat(store.getStateSnapshot(created.getSagaId())).isPresent();
  }

  @Test
  void createSaga_idAlreadyTaken_throwsSagaAlreadyExistsException() {
    // Arrange
    newSaga("saga-1");

    // Act & Assert
    assertThatThrownBy(() -> newSaga("saga-1")).isInstanceOf(SagaAlreadyExistsException.class);
  }

  @Test
  void reads_unknownSaga_returnEmpty() {
    // Act & Assert
    assertThat(store.getStateSnapshot("missing")).isEmpty();
    assertThat(store.getStateWithEvents("missing", 10)).isEmpty();
    assertThat(store.getEvents("missing")).isEmpty();
    assertThat(store.getEventCount("missing")).isZero();
    assertThat(store.getNewestEvent("missing")).isEmpty();
  }

  @Test
  void recordStepEvent_runningSaga_appendsWithoutChangingTheState() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");

    // Act
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}"));
    store.recordStepEvent("saga-1", 2, StepEvent.completed(1, "charge", "{}"));

    // Assert
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), created);
    assertThat(eventTypes("saga-1"))
        .containsExactly(
            EventType.SAGA_STARTED, EventType.STEP_COMPLETED, EventType.STEP_COMPLETED);
    assertThat(store.getEventCount("saga-1")).isEqualTo(3);
    assertThat(store.getNewestEvent("saga-1").orElseThrow().type())
        .isEqualTo(EventType.STEP_COMPLETED);
  }

  @Test
  void getStateWithEvents_maxBelowEventCount_returnsCurrentStateAndNewestEventsTruncated() {
    // Arrange
    newSaga("saga-1");
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}"));
    store.recordStepEvent("saga-1", 2, StepEvent.completed(1, "charge", "{}"));
    complete("saga-1", 3);

    // Act
    SagaStateAndEvents result = store.getStateWithEvents("saga-1", 2).orElseThrow();

    // Assert
    assertThat(result.snapshot().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(result.truncated()).isTrue();
    assertThat(result.events())
        .extracting(SagaEvent::getEventType)
        .containsExactly(EventType.STEP_COMPLETED, EventType.SAGA_COMPLETED);
  }

  @Test
  void getStateWithEvents_maxAboveEventCount_returnsAllEventsNotTruncated() {
    // Arrange
    newSaga("saga-1");
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}"));

    // Act
    SagaStateAndEvents result = store.getStateWithEvents("saga-1", 10).orElseThrow();

    // Assert
    assertThat(result.snapshot().getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(result.truncated()).isFalse();
    assertThat(result.events()).hasSize(2);
  }

  @Test
  void reads_moreStepEventsThanAStoragePage_stillFindTheStateBelowThem() {
    // Arrange — the state lives on the newest status-changing event, here under 25 step events,
    // more than one page of the storage fetch
    SagaStateSnapshot created = newSaga("saga-1");
    for (int sequence = 1; sequence <= 25; sequence++) {
      store.recordStepEvent("saga-1", sequence, StepEvent.completed(sequence, "s", "{}"));
    }

    // Act
    SagaStateSnapshot state = store.getStateSnapshot("saga-1").orElseThrow();
    SagaStateAndEvents withEvents = store.getStateWithEvents("saga-1", 5).orElseThrow();
    SagaStore.NewestEvent newest = probe("saga-1");

    // Assert
    assertSameState(state, created);
    assertSameState(withEvents.snapshot(), created);
    assertThat(withEvents.events()).hasSize(5);
    assertThat(newest.sequence()).isEqualTo(25);
  }

  // ---------------------------------------------------------------------------
  // Status transitions and fencing
  // ---------------------------------------------------------------------------

  @Test
  void recordStatusEvent_currentSnapshot_transitionsAndReadsBack() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");

    // Act
    SagaStateSnapshot after =
        store.recordStatusEvent(created, 1, StatusEvent.compensating(), OWNER);

    // Assert
    assertThat(after.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), after);
  }

  @Test
  void recordStatusEvent_staleSnapshot_throwsSagaConcurrentModificationException() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    store.recordStatusEvent(created, 1, StatusEvent.compensating(), OWNER);

    // Act & Assert
    assertThatThrownBy(() -> store.recordStatusEvent(created, 2, StatusEvent.completed(), OWNER))
        .isInstanceOf(SagaConcurrentModificationException.class);
    assertThat(store.getStateSnapshot("saga-1").orElseThrow().getStatus())
        .isEqualTo(SagaStatus.COMPENSATING);
  }

  @Test
  void recordStatusEvent_sequenceAlreadyTaken_throwsSagaConcurrentModificationException() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}"));

    // Act & Assert
    assertThatThrownBy(() -> store.recordStatusEvent(created, 1, StatusEvent.completed(), OWNER))
        .isInstanceOf(SagaConcurrentModificationException.class);
    assertThat(store.getStateSnapshot("saga-1").orElseThrow().getStatus())
        .isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void recordStepEvent_sequenceAlreadyTaken_throwsSagaConcurrentModificationException() {
    // Arrange
    newSaga("saga-1");
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}"));

    // Act & Assert
    assertThatThrownBy(
            () -> store.recordStepEvent("saga-1", 1, StepEvent.completed(1, "charge", "{}")))
        .isInstanceOf(SagaConcurrentModificationException.class);
    assertThat(store.getEventCount("saga-1")).isEqualTo(2);
  }

  @Test
  void
      recordStatusEvent_epochUpdatedAtGiven_readsBackEpochAndThatSnapshotDrivesTheNextTransition() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");

    // Act
    store.recordStatusEvent(created, 1, StatusEvent.compensating(), OWNER, Instant.EPOCH);
    SagaStateSnapshot read = store.getStateSnapshot("saga-1").orElseThrow();
    SagaStateSnapshot next = store.recordStatusEvent(read, 2, StatusEvent.compensated(), OWNER);

    // Assert
    assertThat(read.getUpdatedAt()).isEqualTo(Instant.EPOCH);
    assertThat(next.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
  }

  // ---------------------------------------------------------------------------
  // Parking
  // ---------------------------------------------------------------------------

  @Test
  void park_deadlinePassed_waitsAndIsReturnedByTheOverdueSweep() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    Instant deadline = Instant.now().minus(Duration.ofMinutes(1));

    // Act
    SagaStateSnapshot parked = store.park(created, 1, StepEvent.pending(0, "approve"), deadline);

    // Assert
    assertThat(parked.getStatus()).isEqualTo(SagaStatus.WAITING);
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), parked);
    assertThat(overdueIds(Instant.now())).containsExactly("saga-1");
    assertThat(overdueIds(deadline.minus(HOUR))).isEmpty();
  }

  @Test
  void park_noDeadline_waitsAndIsNeverReturnedByTheOverdueSweep() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");

    // Act
    SagaStateSnapshot parked = store.park(created, 1, StepEvent.pending(0, "approve"), null);

    // Assert
    assertThat(parked.getStatus()).isEqualTo(SagaStatus.WAITING);
    assertThat(overdueIds(Instant.now().plus(Duration.ofDays(3650)))).isEmpty();
  }

  @Test
  void park_staleSnapshot_throwsSagaConcurrentModificationException() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    store.recordStatusEvent(created, 1, StatusEvent.compensating(), OWNER);

    // Act & Assert
    assertThatThrownBy(() -> store.park(created, 2, StepEvent.pending(0, "approve"), null))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void parkedSaga_neitherStaleSweepNorRetentionReturnsIt() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    store.park(created, 1, StepEvent.pending(0, "approve"), null);
    Instant future = Instant.now().plus(HOUR);

    // Act & Assert
    assertThat(recoverableIds(future)).isEmpty();
    assertThat(store.findByStatusOlderThan(SagaStatus.COMPLETED, future, 100, OWNER, 0)).isEmpty();
  }

  @Test
  void resumeParkedStep_boundedPark_runsAgainAndLeavesTheOverdueSweep() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));

    // Act
    SagaStateSnapshot resumed =
        store.resumeParkedStep(parked, 2, StepEvent.completed(0, "approve", "{}"));

    // Assert
    assertThat(resumed.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), resumed);
    assertThat(overdueIds(Instant.now())).isEmpty();
    assertThat(eventTypes("saga-1"))
        .containsExactly(EventType.SAGA_STARTED, EventType.STEP_PENDING, EventType.STEP_COMPLETED);
  }

  @Test
  void resumeParkedStep_unboundedPark_runsAgain() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", null);

    // Act
    SagaStateSnapshot resumed =
        store.resumeParkedStep(parked, 2, StepEvent.completed(0, "approve", "{}"));

    // Assert
    assertThat(resumed.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), resumed);
  }

  @Test
  void failParkedStep_compensatingTarget_compensatesAndLeavesTheOverdueSweep() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));

    // Act
    SagaStateSnapshot failed =
        store.failParkedStep(
            parked, 2, StepEvent.failed(0, "approve", null), SagaStatus.COMPENSATING);

    // Assert
    assertThat(failed.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), failed);
    assertThat(overdueIds(Instant.now())).isEmpty();
  }

  @Test
  void failParkedStep_escalatedTarget_escalates() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));

    // Act
    SagaStateSnapshot failed =
        store.failParkedStep(parked, 2, StepEvent.failed(0, "approve", null), SagaStatus.ESCALATED);

    // Assert
    assertThat(failed.getStatus()).isEqualTo(SagaStatus.ESCALATED);
    assertThat(overdueIds(Instant.now())).isEmpty();
  }

  @Test
  void failParkedStep_runningTarget_throwsIllegalArgumentException() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", null);

    // Act & Assert
    assertThatThrownBy(
            () ->
                store.failParkedStep(
                    parked, 2, StepEvent.failed(0, "approve", null), SagaStatus.RUNNING))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void redriveParkedStep_boundedPark_runsAgainAndLeavesTheOverdueSweep() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));

    // Act
    SagaStateSnapshot redriven =
        store.redriveParkedStep(parked, 2, StepEvent.reissuing(0, "approve"));

    // Assert
    assertThat(redriven.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), redriven);
    assertThat(overdueIds(Instant.now())).isEmpty();
  }

  @Test
  void redriveParkedStep_reissueWithAReportedError_keepsItsPayload() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));
    String error = "{\"message\":\"participant reported failure: busy\"}";

    // Act
    store.redriveParkedStep(parked, 2, StepEvent.reissuing(0, "approve", error));

    // Assert
    SagaEvent last = store.getEvents("saga-1").get(2);
    assertThat(last.getEventType()).isEqualTo(EventType.STEP_REISSUING);
    assertThat(((StepEvent) last).getPayload()).isEqualTo(error);
  }

  @Test
  void resumeParkedStep_afterTheTimeoutFailedTheStep_throwsSagaConcurrentModificationException() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));
    store.failParkedStep(parked, 2, StepEvent.failed(0, "approve", null), SagaStatus.COMPENSATING);

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(parked, 2, StepEvent.completed(0, "approve", "{}")))
        .isInstanceOf(SagaConcurrentModificationException.class);
    assertThat(store.getStateSnapshot("saga-1").orElseThrow().getStatus())
        .isEqualTo(SagaStatus.COMPENSATING);
  }

  @Test
  void resumeParkedStep_afterRedriveAndRepark_throwsSagaConcurrentModificationException() {
    // Arrange — the saga is WAITING again, so only the moved sweep row can refuse the stale resume
    SagaStateSnapshot firstPark = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));
    SagaStateSnapshot redriven =
        store.redriveParkedStep(firstPark, 2, StepEvent.reissuing(0, "approve"));
    store.park(redriven, 3, StepEvent.pending(0, "approve"), Instant.now().plus(HOUR));

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(firstPark, 4, StepEvent.completed(0, "approve", "{}")))
        .isInstanceOf(SagaConcurrentModificationException.class);
    assertThat(store.getStateSnapshot("saga-1").orElseThrow().getStatus())
        .isEqualTo(SagaStatus.WAITING);
  }

  @Test
  void parkAfterResume_newDeadline_isOverdueOnlyByTheNewDeadline() {
    // Arrange
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().minus(Duration.ofMinutes(1)));
    SagaStateSnapshot redriven =
        store.redriveParkedStep(parked, 2, StepEvent.reissuing(0, "approve"));
    Instant newDeadline = Instant.now().plus(HOUR);

    // Act
    store.park(redriven, 3, StepEvent.pending(0, "approve"), newDeadline);

    // Assert
    assertThat(overdueIds(Instant.now())).isEmpty();
    assertThat(overdueIds(newDeadline.plus(Duration.ofMinutes(1)))).containsExactly("saga-1");
  }

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  @Test
  void findRecoverable_runningAndCompensatingOlderThanThreshold_returnsThemOnly() {
    // Arrange
    newSaga("running");
    SagaStateSnapshot toCompensate = newSaga("compensating");
    store.recordStatusEvent(toCompensate, 1, StatusEvent.compensating(), OWNER);
    complete("completed", newSaga("completed"), 1);
    parkedSaga("waiting", null);

    // Act
    List<String> ids = recoverableIds(Instant.now().plus(HOUR));

    // Assert
    assertThat(ids).containsExactlyInAnyOrder("running", "compensating");
  }

  @Test
  void findRecoverable_thresholdBeforeTheLastTransition_returnsNothing() {
    // Arrange
    newSaga("saga-1");

    // Act & Assert
    assertThat(recoverableIds(Instant.now().minus(HOUR))).isEmpty();
  }

  @Test
  void claimForRecovery_currentSnapshot_returnsTheSagaInItsStatus() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(created, probe("saga-1"), OTHER_OWNER);

    // Assert
    assertThat(claimed).isPresent();
    assertThat(claimed.get().getSagaId()).isEqualTo("saga-1");
    assertThat(claimed.get().getStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void claimForRecovery_sameViewClaimedTwice_secondClaimReturnsEmpty() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    SagaStore.NewestEvent view = probe("saga-1");
    store.claimForRecovery(created, view, OTHER_OWNER);

    // Act
    Optional<SagaStateSnapshot> second = store.claimForRecovery(created, view, "engine-3");

    // Assert
    assertThat(second).isEmpty();
  }

  @Test
  void claimForRecovery_sagaTransitionedSinceTheView_returnsEmpty() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    SagaStore.NewestEvent view = probe("saga-1");
    store.recordStatusEvent(created, 1, StatusEvent.compensating(), OWNER);

    // Act & Assert
    assertThat(store.claimForRecovery(created, view, OTHER_OWNER)).isEmpty();
  }

  @Test
  void claimForRecovery_sagaTransitionedBetweenTheSweepAndTheProbe_returnsEmptyAndWritesNothing() {
    // Arrange — the sweep saw RUNNING; the saga then started compensating and gave one up, which
    // recovery does not count as being driven. The probe is fresh, so only the state fences it.
    SagaStateSnapshot swept = newSaga("saga-1");
    SagaStateSnapshot compensating =
        store.recordStatusEvent(swept, 1, StatusEvent.compensating(), OWNER);
    store.recordStepEvent("saga-1", 2, StepEvent.compensationFailed(0, "reserve", null));
    SagaStore.NewestEvent view = probe("saga-1");

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(swept, view, OTHER_OWNER);

    // Assert — the log still says COMPENSATING, the state the sweep row is in
    assertThat(claimed).isEmpty();
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), compensating);
    assertThat(eventTypes("saga-1")).doesNotContain(EventType.SAGA_CLAIMED);
  }

  @Test
  void claimForRecovery_stepEventAppendedSinceTheView_returnsEmpty() {
    // Arrange — the owner is alive after all: it appended a step event after the probe
    SagaStateSnapshot created = newSaga("saga-1");
    SagaStore.NewestEvent view = probe("saga-1");
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}"));

    // Act & Assert
    assertThat(store.claimForRecovery(created, view, OTHER_OWNER)).isEmpty();
  }

  @Test
  void claimForRecovery_presumedDeadOwnerAppendsNext_throwsSagaConcurrentModificationException() {
    // Arrange — the claim takes the sequence the old owner writes next, so the owner is stopped at
    // its next step event, not only at its next status change
    SagaStateSnapshot created = newSaga("saga-1");
    assertThat(store.claimForRecovery(created, probe("saga-1"), OTHER_OWNER)).isPresent();

    // Act & Assert
    assertThatThrownBy(
            () -> store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}")))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void claimForRecovery_sagaPurgedSinceTheProbe_returnsEmptyAndWritesNothing() {
    // Arrange — the owner finished and retention purged the saga while the claim was delayed;
    // the purge freed the sequence the claim inserts at
    SagaStateSnapshot swept = newSaga("saga-1");
    SagaStore.NewestEvent view = probe("saga-1");
    complete("saga-1", swept, 1);
    assertThat(store.deleteSaga("saga-1")).isTrue();

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(swept, view, OTHER_OWNER);

    // Assert — nothing was brought back
    assertThat(claimed).isEmpty();
    assertThat(store.getEvents("saga-1")).isEmpty();
    assertThat(store.getStateSnapshot("saga-1")).isEmpty();
  }

  @Test
  void claimForRecovery_sagaRecreatedUnderTheSameIdSinceTheProbe_returnsEmptyAndLeavesItAlone() {
    // Arrange — after the purge a new saga took the id; it has not reached the claimed sequence
    SagaStateSnapshot swept = newSaga("saga-1");
    SagaStore.NewestEvent view = probe("saga-1");
    complete("saga-1", swept, 1);
    assertThat(store.deleteSaga("saga-1")).isTrue();
    // The sweep row's key is the creation time in milliseconds; make the new saga's differ
    while (Instant.now().toEpochMilli() <= swept.getUpdatedAt().toEpochMilli()) {
      Thread.onSpinWait();
    }
    SagaStateSnapshot recreated = newSaga("saga-1");

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(swept, view, OTHER_OWNER);

    // Assert
    assertThat(claimed).isEmpty();
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), recreated);
    assertThat(eventTypes("saga-1")).containsExactly(EventType.SAGA_STARTED);
  }

  @Test
  void claimForRecovery_scheduledSaga_recordsTheClaimWithoutChangingTheState() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");

    // Act
    assertThat(store.claimForRecovery(created, probe("saga-1"), OTHER_OWNER)).isPresent();

    // Assert — updatedAt keeps meaning the last status change
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), created);
    assertThat(eventTypes("saga-1"))
        .containsExactly(EventType.SAGA_STARTED, EventType.SAGA_CLAIMED);
  }

  @Test
  void getNewestEvent_claimFollowedByAStepEvent_stillReportsTheClaim() {
    // Arrange — a stuck compensation: the claimed drive fails its compensation again
    SagaStateSnapshot created = newSaga("saga-1");
    SagaStateSnapshot compensating =
        store.recordStatusEvent(created, 1, StatusEvent.compensating(), OWNER);
    assertThat(store.claimForRecovery(compensating, probe("saga-1"), OTHER_OWNER)).isPresent();
    store.recordStepEvent("saga-1", 3, StepEvent.compensationFailed(0, "reserve", null));

    // Act
    SagaStore.NewestEvent newest = probe("saga-1");

    // Assert — the claim's time is what keeps recovery from claiming it again on every pass
    assertThat(newest.type()).isEqualTo(EventType.STEP_COMPENSATION_FAILED);
    assertThat(newest.lastClaimedAt()).isNotNull();
  }

  @Test
  void getNewestEvent_statusChangedAfterTheClaim_reportsNoClaim() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    SagaStateSnapshot claimed =
        store.claimForRecovery(created, probe("saga-1"), OTHER_OWNER).orElseThrow();
    store.recordStatusEvent(claimed, 2, StatusEvent.compensating(), OTHER_OWNER);

    // Act & Assert
    assertThat(probe("saga-1").lastClaimedAt()).isNull();
  }

  @Test
  void claimForRecovery_claimedSnapshot_drivesTheNextTransition() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    SagaStateSnapshot claimed =
        store.claimForRecovery(created, probe("saga-1"), OTHER_OWNER).orElseThrow();
    int sequence = store.getEventCount("saga-1");

    // Act
    SagaStateSnapshot after =
        store.recordStatusEvent(claimed, sequence, StatusEvent.completed(), OTHER_OWNER);

    // Assert
    assertThat(after.getStatus()).isEqualTo(SagaStatus.COMPLETED);
  }

  @Test
  void markForRecovery_runningSaga_readsBackEpochAndIsRecoverableAtOnce() {
    // Arrange
    newSaga("saga-1");

    // Act
    store.markForRecovery("saga-1");

    // Assert
    SagaStateSnapshot read = store.getStateSnapshot("saga-1").orElseThrow();
    assertThat(read.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(read.getUpdatedAt()).isEqualTo(Instant.EPOCH);
    assertThat(recoverableIds(Instant.now().minus(HOUR))).containsExactly("saga-1");
  }

  @Test
  void markForRecovery_thenReadByIdSnapshot_drivesTheNextTransition() {
    // Arrange
    newSaga("saga-1");
    store.markForRecovery("saga-1");
    SagaStateSnapshot read = store.getStateSnapshot("saga-1").orElseThrow();

    // Act
    SagaStateSnapshot after =
        store.recordStatusEvent(
            read, store.getEventCount("saga-1"), StatusEvent.completed(), OWNER);

    // Assert
    assertThat(after.getStatus()).isEqualTo(SagaStatus.COMPLETED);
  }

  @Test
  void markForRecovery_thenClaim_claimsTheHandedOffSaga() {
    // Arrange
    newSaga("saga-1");
    store.markForRecovery("saga-1");
    SagaStateSnapshot handedOff = recoverable(Instant.now().minus(HOUR)).get(0);

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(handedOff, probe("saga-1"), OTHER_OWNER);

    // Assert
    assertThat(claimed).isPresent();
    assertThat(recoverableIds(Instant.now().minus(HOUR))).isEmpty();
  }

  @Test
  void markForRecovery_alreadyHandedOff_rewritesTheSameSweepRowAndStaysClaimable() {
    // Arrange — the second hand-off deletes and re-inserts the sweep row at the same key in one
    // transaction; markForRecovery swallows failures, so the second release must be seen landing
    newSaga("saga-1");
    store.markForRecovery("saga-1");

    // Act
    store.markForRecovery("saga-1");

    // Assert
    assertThat(eventTypes("saga-1")).filteredOn(type -> type == EventType.SAGA_RELEASED).hasSize(2);
    List<SagaStateSnapshot> handedOff = recoverable(Instant.now().minus(HOUR));
    assertThat(handedOff).hasSize(1);
    assertThat(store.claimForRecovery(handedOff.get(0), probe("saga-1"), OTHER_OWNER)).isPresent();
    assertThat(recoverableIds(Instant.now().minus(HOUR))).isEmpty();
  }

  @Test
  void markForRecovery_unknownSaga_doesNothing() {
    // Act
    store.markForRecovery("missing");

    // Assert
    assertThat(store.getStateSnapshot("missing")).isEmpty();
  }

  @Test
  void markForRecovery_completedSaga_leavesItUntouched() {
    // Arrange — stamping a finished saga EPOCH would put it first in line for the retention purge
    newSaga("saga-1");
    complete("saga-1", 1);
    SagaStateSnapshot completed = store.getStateSnapshot("saga-1").orElseThrow();

    // Act
    store.markForRecovery("saga-1");

    // Assert
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), completed);
    assertThat(eventTypes("saga-1")).doesNotContain(EventType.SAGA_RELEASED);
  }

  @Test
  void markForRecovery_parkedSaga_leavesItUntouched() {
    // Arrange — a parked saga is timed out by its deadline, not driven by recovery
    SagaStateSnapshot parked = parkedSaga("saga-1", Instant.now().plus(HOUR));

    // Act
    store.markForRecovery("saga-1");

    // Assert
    assertSameState(store.getStateSnapshot("saga-1").orElseThrow(), parked);
    assertThat(eventTypes("saga-1")).doesNotContain(EventType.SAGA_RELEASED);
  }

  @Test
  void markForRecovery_runningSaga_takesTheSequenceTheDriveWritesNext() {
    // Arrange
    newSaga("saga-1");
    store.markForRecovery("saga-1");

    // Act & Assert — a drive still running after the hand-off cannot record anything more
    assertThatThrownBy(
            () -> store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "reserve", "{}")))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void bulkResetShape_listedEscalatedSnapshotWithEpoch_resetsAndHandsTheSagaToRecovery() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    store.recordStatusEvent(created, 1, StatusEvent.escalated("stuck"), OWNER);
    SagaStateSnapshot listed =
        store
            .listStateSnapshots(SagaQuery.newBuilder().status(SagaStatus.ESCALATED).build())
            .getItems()
            .get(0);

    // Act
    SagaStateSnapshot reset =
        store.recordStatusEvent(
            listed,
            2,
            StatusEvent.reset(SagaStatus.RUNNING, "alice", "fixed"),
            OWNER,
            Instant.EPOCH);

    // Assert
    assertThat(reset.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(store.getStateSnapshot("saga-1").orElseThrow().getUpdatedAt())
        .isEqualTo(Instant.EPOCH);
    assertThat(recoverableIds(Instant.now().minus(HOUR))).containsExactly("saga-1");
  }

  // ---------------------------------------------------------------------------
  // Retention
  // ---------------------------------------------------------------------------

  @Test
  void findByStatusOlderThan_terminalSagasOlderThanThreshold_returnsOnlyThatStatus() {
    // Arrange
    complete("done-1", newSaga("done-1"), 1);
    complete("done-2", newSaga("done-2"), 1);
    SagaStateSnapshot toCompensate = newSaga("undone");
    SagaStateSnapshot compensating =
        store.recordStatusEvent(toCompensate, 1, StatusEvent.compensating(), OWNER);
    store.recordStatusEvent(compensating, 2, StatusEvent.compensated(), OWNER);
    newSaga("running");

    // Act
    List<SagaStateSnapshot> completed =
        store.findByStatusOlderThan(SagaStatus.COMPLETED, Instant.now().plus(HOUR), 100, OWNER, 0);

    // Assert
    assertThat(completed)
        .extracting(SagaStateSnapshot::getSagaId)
        .containsExactlyInAnyOrder("done-1", "done-2");
    assertThat(
            store.findByStatusOlderThan(
                SagaStatus.COMPLETED, Instant.now().minus(HOUR), 100, OWNER, 0))
        .isEmpty();
  }

  @Test
  void findByStatusOlderThan_maxResultsBelowMatches_returnsAtMostMaxResults() {
    // Arrange
    for (int i = 0; i < 5; i++) {
      complete("done-" + i, newSaga("done-" + i), 1);
    }

    // Act
    List<SagaStateSnapshot> found =
        store.findByStatusOlderThan(SagaStatus.COMPLETED, Instant.now().plus(HOUR), 2, OWNER, 0);

    // Assert
    assertThat(found).hasSize(2);
  }

  @Test
  void deleteSaga_completedSaga_removesEverythingAndReturnsTrue() {
    // Arrange
    complete("saga-1", newSaga("saga-1"), 1);

    // Act
    boolean deleted = store.deleteSaga("saga-1");

    // Assert
    assertThat(deleted).isTrue();
    assertThat(store.getStateSnapshot("saga-1")).isEmpty();
    assertThat(store.getEvents("saga-1")).isEmpty();
    assertThat(
            store.findByStatusOlderThan(
                SagaStatus.COMPLETED, Instant.now().plus(HOUR), 100, OWNER, 0))
        .isEmpty();
  }

  @Test
  void deleteSaga_alreadyDeleted_returnsFalse() {
    // Arrange
    complete("saga-1", newSaga("saga-1"), 1);
    store.deleteSaga("saga-1");

    // Act & Assert
    assertThat(store.deleteSaga("saga-1")).isFalse();
  }

  @Test
  void deleteSaga_runningSaga_throwsIllegalStateExceptionAndKeepsIt() {
    // Arrange
    newSaga("saga-1");

    // Act & Assert
    assertThatThrownBy(() -> store.deleteSaga("saga-1")).isInstanceOf(IllegalStateException.class);
    assertThat(store.getStateSnapshot("saga-1")).isPresent();
    assertThat(store.getEvents("saga-1")).hasSize(1);
  }

  @Test
  void deleteSaga_deletedId_canBeCreatedAgain() {
    // Arrange
    complete("saga-1", newSaga("saga-1"), 1);
    store.deleteSaga("saga-1");

    // Act
    SagaStateSnapshot recreated = newSaga("saga-1");

    // Assert
    assertThat(recreated.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(eventTypes("saga-1")).containsExactly(EventType.SAGA_STARTED);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private SagaStateSnapshot newSaga(String sagaId) {
    return store.createSaga(sagaId, "order-saga", OWNER, Map.of("amount", 100), "v1");
  }

  private SagaStateSnapshot parkedSaga(String sagaId, @Nullable Instant deadline) {
    SagaStateSnapshot created = newSaga(sagaId);
    return store.park(created, 1, StepEvent.pending(0, "approve"), deadline);
  }

  private void complete(String sagaId, int sequence) {
    complete(sagaId, store.getStateSnapshot(sagaId).orElseThrow(), sequence);
  }

  private void complete(String sagaId, SagaStateSnapshot current, int sequence) {
    assertThat(current.getSagaId()).isEqualTo(sagaId);
    store.recordStatusEvent(current, sequence, StatusEvent.completed(), OWNER);
  }

  /** The newest event, as recovery reads it just before a claim. */
  private SagaStore.NewestEvent probe(String sagaId) {
    return store.getNewestEvent(sagaId).orElseThrow();
  }

  private List<EventType> eventTypes(String sagaId) {
    return store.getEvents(sagaId).stream().map(SagaEvent::getEventType).toList();
  }

  /**
   * Compares two snapshots at the store's millisecond precision: a snapshot handed back by a write
   * carries the clock's full precision, one read back carries what the store persisted.
   */
  private static void assertSameState(SagaStateSnapshot actual, SagaStateSnapshot expected) {
    assertThat(actual.getSagaId()).isEqualTo(expected.getSagaId());
    assertThat(actual.getSagaName()).isEqualTo(expected.getSagaName());
    assertThat(actual.getStatus()).isEqualTo(expected.getStatus());
    assertThat(actual.getDefinitionVersion()).isEqualTo(expected.getDefinitionVersion());
    assertThat(actual.getCreatedAt().truncatedTo(ChronoUnit.MILLIS))
        .isEqualTo(expected.getCreatedAt().truncatedTo(ChronoUnit.MILLIS));
    assertThat(actual.getUpdatedAt().truncatedTo(ChronoUnit.MILLIS))
        .isEqualTo(expected.getUpdatedAt().truncatedTo(ChronoUnit.MILLIS));
  }

  private List<SagaStateSnapshot> recoverable(Instant threshold) {
    List<SagaStateSnapshot> found = new ArrayList<>();
    SagaStore.@Nullable ScanCursor cursor = null;
    do {
      SagaStore.Recoverables page = store.findRecoverable(threshold, cursor);
      found.addAll(page.sagas());
      cursor = page.nextCursor();
    } while (cursor != null);
    return found;
  }

  private List<String> recoverableIds(Instant threshold) {
    return recoverable(threshold).stream().map(SagaStateSnapshot::getSagaId).toList();
  }

  private List<String> overdueIds(Instant threshold) {
    List<String> found = new ArrayList<>();
    SagaStore.@Nullable ScanCursor cursor = null;
    do {
      SagaStore.OverdueParked page = store.findOverdueParkedSagas(threshold, cursor);
      found.addAll(page.sagaIds());
      cursor = page.nextCursor();
    } while (cursor != null);
    return found;
  }
}
