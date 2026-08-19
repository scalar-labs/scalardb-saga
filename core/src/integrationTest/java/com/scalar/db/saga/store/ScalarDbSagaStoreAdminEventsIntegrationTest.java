package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test proving the Admin API's operator-intervention events ({@code
 * SAGA_FORCE_COMPLETED / SAGA_RECOVERING / SAGA_RESET}) survive a real (SQLite-backed) ScalarDB
 * round trip. The variable target of {@code SAGA_RECOVERING}/{@code SAGA_RESET} ({@code
 * COMPENSATING} or {@code RUNNING}) is reconstructed from the persisted payload, which mock-based
 * unit tests cannot exercise.
 */
class ScalarDbSagaStoreAdminEventsIntegrationTest {

  @TempDir Path tempDir;

  private Path dbPath;
  private SagaStore store;

  @BeforeEach
  void setUp() {
    dbPath = tempDir.resolve("saga-admin-events-it.db");
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + dbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    store = ScalarDbSagaStoreFactory.create(props).createStore();
  }

  @AfterEach
  void tearDown() throws Exception {
    store.close();
    Files.deleteIfExists(dbPath);
  }

  private SagaStateSnapshot newRunningSaga(String sagaId) {
    return store.createSaga(sagaId, "order-saga", "owner", Map.of("amount", 100), "v1");
  }

  private StatusEvent lastEvent(String sagaId) {
    List<SagaEvent> events = store.getEvents(sagaId);
    return (StatusEvent) events.get(events.size() - 1);
  }

  @Test
  void recordStatusEvent_recoveringToCompensating_roundTripsTargetAndAudit() {
    // Arrange — a RUNNING saga (SAGA_STARTED is sequence 0)
    SagaStateSnapshot running = newRunningSaga("saga-recovering");

    // Act — an operator recovers it in the compensate direction
    SagaStateSnapshot after =
        store.recordStatusEvent(
            running,
            1,
            StatusEvent.recovering(SagaStatus.COMPENSATING, "alice", "rolling back"),
            running.getOwnerId());

    // Assert — status transitioned, and the event reconstructs with the right variable target +
    // audit
    assertThat(after.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    StatusEvent reloaded = lastEvent("saga-recovering");
    assertThat(reloaded.getEventType()).isEqualTo(EventType.SAGA_RECOVERING);
    assertThat(reloaded.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(AdminAuditPayload.operator(reloaded.getPayload())).isEqualTo("alice");
    assertThat(AdminAuditPayload.reason(reloaded.getPayload())).isEqualTo("rolling back");
  }

  @Test
  void recordStatusEvent_resetToRunning_roundTripsRunningTarget() {
    // Arrange — a RUNNING saga escalated by the engine, then reset by an operator
    SagaStateSnapshot running = newRunningSaga("saga-reset");
    SagaStateSnapshot escalated =
        store.recordStatusEvent(
            running, 1, StatusEvent.escalated("retries exhausted"), running.getOwnerId());

    // Act — un-escalate in the resume-forward direction
    SagaStateSnapshot after =
        store.recordStatusEvent(
            escalated,
            2,
            StatusEvent.reset(SagaStatus.RUNNING, "bob", "downstream restored"),
            escalated.getOwnerId());

    // Assert
    assertThat(after.getStatus()).isEqualTo(SagaStatus.RUNNING);
    StatusEvent reloaded = lastEvent("saga-reset");
    assertThat(reloaded.getEventType()).isEqualTo(EventType.SAGA_RESET);
    assertThat(reloaded.getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(AdminAuditPayload.reason(reloaded.getPayload())).isEqualTo("downstream restored");
  }

  @Test
  void recordStatusEvent_forceCompleted_roundTripsCompletedTargetAndAudit() {
    // Arrange — a RUNNING saga escalated by the engine
    SagaStateSnapshot running = newRunningSaga("saga-forced");
    SagaStateSnapshot escalated =
        store.recordStatusEvent(running, 1, StatusEvent.escalated("stuck"), running.getOwnerId());

    // Act — an operator force-completes it
    SagaStateSnapshot after =
        store.recordStatusEvent(
            escalated,
            2,
            StatusEvent.forceCompleted("carol", "confirmed done"),
            escalated.getOwnerId());

    // Assert — a distinct event type survives, so the forced override is never mistaken for a
    // genuine completion, and the prior ESCALATED history remains in the stream
    assertThat(after.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    List<SagaEvent> events = store.getEvents("saga-forced");
    assertThat(events).extracting(SagaEvent::getEventType).contains(EventType.SAGA_ESCALATED);
    StatusEvent reloaded = (StatusEvent) events.get(events.size() - 1);
    assertThat(reloaded.getEventType()).isEqualTo(EventType.SAGA_FORCE_COMPLETED);
    assertThat(reloaded.getTargetStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(AdminAuditPayload.operator(reloaded.getPayload())).isEqualTo("carol");
  }

  @Test
  void getStateWithEvents_returnsSnapshotAndFullEventStreamAtomically() {
    // Arrange — a saga escalated by the engine (SAGA_STARTED @ seq 0, SAGA_ESCALATED @ seq 1)
    SagaStateSnapshot running = newRunningSaga("saga-detail");
    store.recordStatusEvent(running, 1, StatusEvent.escalated("stuck"), running.getOwnerId());

    // Act
    Optional<SagaStateAndEvents> result =
        store.getStateWithEvents("saga-detail", Integer.MAX_VALUE);

    // Assert — the snapshot's status is paired with the full, ordered event stream in one read
    assertThat(result).isPresent();
    assertThat(result.get().snapshot().getStatus()).isEqualTo(SagaStatus.ESCALATED);
    assertThat(result.get().truncated()).isFalse();
    assertThat(result.get().events())
        .extracting(SagaEvent::getEventType)
        .containsExactly(EventType.SAGA_STARTED, EventType.SAGA_ESCALATED);
  }

  @Test
  void getStateWithEvents_streamLongerThanMax_returnsNewestEventsAscendingAndFlagsTruncated() {
    // Arrange — four events: STARTED @ 0, ESCALATED @ 1, RECOVERING @ 2, ESCALATED @ 3
    SagaStateSnapshot s0 = newRunningSaga("saga-truncated");
    SagaStateSnapshot s1 =
        store.recordStatusEvent(s0, 1, StatusEvent.escalated("stuck"), s0.getOwnerId());
    SagaStateSnapshot s2 =
        store.recordStatusEvent(
            s1, 2, StatusEvent.recovering(SagaStatus.RUNNING, "bob", "retry"), s1.getOwnerId());
    store.recordStatusEvent(s2, 3, StatusEvent.escalated("stuck again"), s2.getOwnerId());

    // Act
    Optional<SagaStateAndEvents> result = store.getStateWithEvents("saga-truncated", 2);

    // Assert — the newest two events, back in ascending order, flagged truncated
    assertThat(result).isPresent();
    assertThat(result.get().truncated()).isTrue();
    assertThat(result.get().events())
        .extracting(SagaEvent::getEventType)
        .containsExactly(EventType.SAGA_RECOVERING, EventType.SAGA_ESCALATED);
    StatusEvent newest = (StatusEvent) result.get().events().get(1);
    assertThat(newest.getPayload()).isEqualTo("stuck again");
  }

  @Test
  void getStateWithEvents_streamExactlyMax_returnsAllNotTruncated() {
    // Arrange — two events, bound of two: the extra-row probe must not report truncation
    SagaStateSnapshot running = newRunningSaga("saga-exact");
    store.recordStatusEvent(running, 1, StatusEvent.escalated("stuck"), running.getOwnerId());

    // Act
    Optional<SagaStateAndEvents> result = store.getStateWithEvents("saga-exact", 2);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().truncated()).isFalse();
    assertThat(result.get().events())
        .extracting(SagaEvent::getEventType)
        .containsExactly(EventType.SAGA_STARTED, EventType.SAGA_ESCALATED);
  }

  @Test
  void getStateWithEvents_maxEventsNotPositive_throwsIllegalArgument() {
    assertThatThrownBy(() -> store.getStateWithEvents("saga-any", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getStateWithEvents_missingSaga_returnsEmpty() {
    assertThat(store.getStateWithEvents("no-such-saga", Integer.MAX_VALUE)).isEmpty();
  }
}
