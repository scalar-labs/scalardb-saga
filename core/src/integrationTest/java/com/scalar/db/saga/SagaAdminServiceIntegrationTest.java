package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.store.StatusEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of the embedded {@link SagaAdminService} against a real (SQLite-backed) ScalarDB,
 * exercised through a {@link DefaultSagaOrchestrator}. Covers the drive-free admin paths ({@code
 * forceComplete}, {@code getSagaDetail}) so the timeline reconstruction and redaction run against
 * real persisted events, not mocks.
 */
class SagaAdminServiceIntegrationTest {

  @TempDir Path tempDir;

  private Path dbPath;
  private SagaStore store;
  private DefaultSagaOrchestrator orchestrator;
  private SagaAdminService admin;

  @BeforeEach
  void setUp() {
    dbPath = tempDir.resolve("saga-admin-it.db");
    java.util.Properties props = new java.util.Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + dbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    // Share one store instance between direct seeding and the orchestrator's admin service.
    store = ScalarDbSagaStoreFactory.create(props).createStore();
    orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .stepResolver(
                (name, cls, ctx) -> {
                  throw new IllegalStateException("no steps needed for this test");
                })
            .build();
    admin = orchestrator.adminService();
  }

  @AfterEach
  void tearDown() throws Exception {
    orchestrator.close(); // owns and closes the shared store
    Files.deleteIfExists(dbPath);
  }

  /** Seeds a saga and drives it to ESCALATED directly through the store. */
  private void seedEscalated(String sagaId) {
    SagaStateSnapshot running =
        store.createSaga(sagaId, "order-saga", "owner", Map.of("amount", 100), "v1");
    store.recordStatusEvent(running, 1, StatusEvent.escalated("retries exhausted"));
  }

  @Test
  void forceComplete_escalatedSaga_transitionsToCompletedAndRecordsAudit() {
    // Arrange
    seedEscalated("saga-1");

    // Act
    SagaStateSnapshot result = admin.forceComplete("saga-1", "confirmed done downstream");

    // Assert — COMPLETED, and the timeline shows the forced override attributed to the operator
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    SagaDetail detail = admin.getSagaDetail("saga-1");
    assertThat(detail.getSnapshot().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    TimelineEvent forced =
        detail.getTimeline().stream()
            .filter(e -> e.getType().equals("SAGA_FORCE_COMPLETED"))
            .findFirst()
            .orElseThrow();
    assertThat(forced.getResultingStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(forced.getOperator()).isEqualTo("embedded");
    assertThat(forced.getDetail()).isEqualTo("confirmed done downstream");
  }

  @Test
  void getSagaDetail_always_reconstructsTimelineAndRedactsInputPayload() {
    // Arrange
    seedEscalated("saga-2");

    // Act
    SagaDetail detail = admin.getSagaDetail("saga-2");

    // Assert — the SAGA_STARTED input payload is never surfaced; the escalation reason is
    TimelineEvent started =
        detail.getTimeline().stream()
            .filter(e -> e.getType().equals("SAGA_STARTED"))
            .findFirst()
            .orElseThrow();
    assertThat(started.getDetail()).isNull();
    assertThat(started.getResultingStatus()).isEqualTo(SagaStatus.RUNNING);

    TimelineEvent escalated =
        detail.getTimeline().stream()
            .filter(e -> e.getType().equals("SAGA_ESCALATED"))
            .findFirst()
            .orElseThrow();
    assertThat(escalated.getDetail()).isEqualTo("retries exhausted");
  }

  @Test
  void forceComplete_runningSaga_isRejectedAsWrongState() {
    // Arrange — a plain RUNNING saga (not escalated)
    store.createSaga("saga-3", "order-saga", "owner", Map.of(), "v1");

    // Act & Assert
    assertThatThrownBy(() -> admin.forceComplete("saga-3", "why"))
        .isInstanceOf(SagaStatePreconditionException.class);
  }
}
