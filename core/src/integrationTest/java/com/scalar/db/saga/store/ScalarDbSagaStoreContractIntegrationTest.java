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
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(created, OTHER_OWNER);

    // Assert
    assertThat(claimed).isPresent();
    assertThat(claimed.get().getSagaId()).isEqualTo("saga-1");
    assertThat(claimed.get().getStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void claimForRecovery_sameViewClaimedTwice_secondClaimReturnsEmpty() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    store.claimForRecovery(created, OTHER_OWNER);

    // Act
    Optional<SagaStateSnapshot> second = store.claimForRecovery(created, "engine-3");

    // Assert
    assertThat(second).isEmpty();
  }

  @Test
  void claimForRecovery_sagaTransitionedSinceTheView_returnsEmpty() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    store.recordStatusEvent(created, 1, StatusEvent.compensating(), OWNER);

    // Act & Assert
    assertThat(store.claimForRecovery(created, OTHER_OWNER)).isEmpty();
  }

  @Test
  void claimForRecovery_claimedSnapshot_drivesTheNextTransition() {
    // Arrange
    SagaStateSnapshot created = newSaga("saga-1");
    SagaStateSnapshot claimed = store.claimForRecovery(created, OTHER_OWNER).orElseThrow();
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
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(handedOff, OTHER_OWNER);

    // Assert
    assertThat(claimed).isPresent();
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
