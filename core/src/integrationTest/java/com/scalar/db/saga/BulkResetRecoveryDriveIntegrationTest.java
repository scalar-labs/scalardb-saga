package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.RecoveryConfig;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test that a bulk {@code resetEscalated} is actually carried out by the recovery loop
 * it hands the drive to, against a real (SQLite-backed) ScalarDB.
 *
 * <p>Bulk reset un-escalates each saga and defers the drive to the recovery sweeper. The sweeper
 * gates both of its drive paths behind the grace-period escalation policy, which measures from the
 * unresolved failure the saga was stuck on — a timestamp the reset does not change and that only
 * ages. So unless the intervention restarts that clock, the sweeper escalates the saga straight
 * back without ever driving it, and the sweep is a no-op that still reports success. These tests
 * pin that behaviour across the real store and a real recovery pass; the mocked-store unit tests
 * stop at the hand-off boundary and cannot see it.
 *
 * <p>The store stamps event timestamps from its own clock, so seeded events cannot be backdated and
 * a skewed recovery clock would age the failure and the intervention equally. Instead these tests
 * put real elapsed time between the failure and the reset, then pin the recovery clock relative to
 * the intervention's persisted timestamp — so the outcome does not depend on how long the store
 * round-trips take.
 */
class BulkResetRecoveryDriveIntegrationTest {

  private static final String SAGA_ID = "saga-reset-drive-1";
  private static final String SAGA_NAME = "order-saga";
  private static final String OWNER_ID = "owner";
  private static final Duration GRACE = Duration.ofMillis(500);

  /** A clock the test pins, so a recovery pass reads a controlled instant. */
  private static final class SettableClock extends Clock {
    private volatile Instant instant = Instant.now();

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instant;
    }

    void set(Instant newInstant) {
      this.instant = newInstant;
    }
  }

  /** A step that records every participant call, so "was it actually driven?" is observable. */
  static final class RecordingStep implements Step {
    static final List<String> CALLS = Collections.synchronizedList(new ArrayList<>());
    private final String name;

    RecordingStep(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public StepResult execute(SagaContext context) {
      CALLS.add("execute:" + name);
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) {
      CALLS.add("compensate:" + name);
    }
  }

  @TempDir Path tempDir;

  private Path dbPath;
  private SagaStore store;
  private DefaultSagaOrchestrator orchestrator;
  private SagaAdminService admin;
  private SagaDefinition def;
  private SettableClock clock;

  @BeforeEach
  void setUp() {
    RecordingStep.CALLS.clear();
    dbPath = tempDir.resolve("bulk-reset-drive-it.db");
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + dbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    // Share one store instance between direct seeding and the orchestrator's admin service.
    store = ScalarDbSagaStoreFactory.create(props).createStore();
    clock = new SettableClock();

    orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .ownerId(OWNER_ID)
            .recoveryConfig(new RecoveryConfig(60_000, 30, GRACE, 1000, 10, clock))
            .stepResolver((name, cls, ctx) -> new RecordingStep(name))
            .build();
    admin = orchestrator.adminService();

    def =
        SagaDefinition.newBuilder(SAGA_NAME)
            .saga()
            .step("debit", "com.example.DebitStep")
            .add()
            .step("credit", "com.example.CreditStep")
            .add()
            .build();
    orchestrator.register(def);
  }

  @AfterEach
  void tearDown() throws Exception {
    orchestrator.close(); // owns and closes the shared store
    Files.deleteIfExists(dbPath);
  }

  /**
   * Seeds a saga stuck in {@code COMPENSATING} on an unresolved compensation failure — the shape
   * the sweeper's own grace check escalates, and the one an operator most often bulk-resets.
   *
   * @return the {@code COMPENSATING} snapshot, for seeding a subsequent transition
   */
  private SagaStateSnapshot seedCompensationStuck() {
    SagaStateSnapshot running =
        store.createSaga(SAGA_ID, SAGA_NAME, OWNER_ID, Map.of("amount", 100), def.getVersion());
    store.recordStepEvent(SAGA_ID, 1, StepEvent.completed(0, "debit", null));
    SagaStateSnapshot compensating =
        store.recordStatusEvent(running, 2, StatusEvent.compensating(), OWNER_ID);
    store.recordStepEvent(SAGA_ID, 3, StepEvent.compensationFailed(0, "debit", null));
    return compensating;
  }

  private Instant timestampOf(EventType type) {
    return store.getEvents(SAGA_ID).stream()
        .filter(e -> e.getEventType() == type)
        .map(SagaEvent::getTimestamp)
        .reduce((first, second) -> second)
        .orElseThrow();
  }

  @Test
  void resetEscalated_bulkThenRecoveryPass_drivesTheSagaInsteadOfReEscalating()
      throws InterruptedException {
    // Arrange — a saga escalated out of a compensation that has been stuck past the grace period.
    // The sleep gives the failure real age, so it stays beyond the grace period no matter when the
    // recovery clock is pinned; that is what makes the intervention the only thing that can save
    // it.
    SagaStateSnapshot compensating = seedCompensationStuck();
    Thread.sleep(GRACE.toMillis() + 300);
    store.recordStatusEvent(
        compensating, 4, StatusEvent.escalated("compensation stuck for over " + GRACE), OWNER_ID);

    // Act — bulk reset un-escalates and defers the drive to the sweeper
    ResetResult result =
        admin.resetEscalated(
            SagaQuery.newBuilder().status(SagaStatus.ESCALATED).pageSize(10).build(),
            "ops: downstream fixed, retry compensation");

    assertThat(result.getResetCount()).isEqualTo(1);
    assertThat(store.getStateSnapshot(SAGA_ID).orElseThrow().getStatus())
        .isEqualTo(SagaStatus.COMPENSATING);

    // Act — one recovery pass, seen from just after the operator's intervention
    clock.set(timestampOf(EventType.SAGA_RESET).plusMillis(10));
    orchestrator.recover();

    // Assert — the reported reset actually happened: compensated, participant really called
    assertThat(store.getStateSnapshot(SAGA_ID).orElseThrow().getStatus())
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(RecordingStep.CALLS).contains("compensate:debit");
    // Only the original escalation — the sweeper must not have escalated it a second time.
    List<SagaEvent> events = store.getEvents(SAGA_ID);
    assertThat(events).filteredOn(e -> e.getEventType() == EventType.SAGA_ESCALATED).hasSize(1);
    assertThat(events.get(events.size() - 1).getEventType()).isEqualTo(EventType.SAGA_COMPENSATED);
  }

  @Test
  void recover_compensationStuckPastGraceWithNoIntervention_escalatesWithoutDriving() {
    // Arrange — the regression guard: with no operator intervention the clock still anchors on the
    // failure, so a saga genuinely stuck past the grace period escalates exactly as it always has.
    seedCompensationStuck();

    // Act — a pass seen from well beyond the grace period. Far enough forward that the saga is also
    // past the staleness threshold, since without a reset nothing backdates its updated_at and the
    // sweeper would otherwise not consider it at all.
    clock.set(timestampOf(EventType.STEP_COMPENSATION_FAILED).plusSeconds(120));
    orchestrator.recover();

    // Assert
    assertThat(store.getStateSnapshot(SAGA_ID).orElseThrow().getStatus())
        .isEqualTo(SagaStatus.ESCALATED);
    assertThat(RecordingStep.CALLS).isEmpty();
  }
}
