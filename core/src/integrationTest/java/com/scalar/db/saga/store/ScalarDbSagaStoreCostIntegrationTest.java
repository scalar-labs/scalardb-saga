package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Storage cost of each saga use case, counted underneath Consensus Commit, against a recorded
 * baseline. A layout change must not raise any use case's round trips or written records; rows and
 * bytes read are reported alongside, because a layout may trade them for fewer round trips.
 *
 * <p>Each use case replays the store calls the engine makes for it, so the count is the store's
 * alone. It always runs on SQLite: the transaction shapes it counts do not depend on the backend.
 * The printed table goes in the pull request that moves a baseline.
 */
class ScalarDbSagaStoreCostIntegrationTest {

  private static final String OWNER = "engine-1";
  private static final int NUM_BUCKETS = 16;

  /**
   * The counts the current layout produces: round trips, then records written. A use case may only
   * move to lower numbers, unless a pull request explains the trade.
   */
  private static final Map<String, long[]> BASELINE = new LinkedHashMap<>();

  static {
    BASELINE.put("sync success", new long[] {16, 21});
    BASELINE.put("sync failure, compensated", new long[] {29, 37});
    BASELINE.put("async, callback in time", new long[] {24, 36});
    BASELINE.put("async, timed out after 2 re-drives", new long[] {49, 75});
    BASELINE.put("approval, accepted", new long[] {24, 32});
    BASELINE.put("approval, rejected", new long[] {27, 35});
    BASELINE.put("poll (getStateSnapshot)", new long[] {1, 0});
    BASELINE.put("detail (getStateWithEvents)", new long[] {2, 0});
    BASELINE.put("recovery claim", new long[] {8, 5});
    BASELINE.put("stuck compensation, one pass", new long[] {12, 8});
    BASELINE.put("shutdown hand-off", new long[] {4, 5});
    BASELINE.put("retention purge", new long[] {5, 13});
    BASELINE.put("idle sweeps (recovery + timeout + retention)", new long[] {80, 0});
    BASELINE.put("admin listing page", new long[] {16, 0});
  }

  @TempDir Path tempDir;

  private CountingStorage counter;
  private DistributedStorage storage;
  private DistributedStorageAdmin storageAdmin;
  private SagaStore store;
  private final Map<String, CountingStorage.Cost> measured = new LinkedHashMap<>();

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:"
            + tempDir.resolve("saga-cost-it.db").toAbsolutePath()
            + "?busy_timeout=10000&journal_mode=WAL");
    props.setProperty("scalar.db.saga.store.scalardb.num_buckets", String.valueOf(NUM_BUCKETS));
    ScalarDbSagaStoreFactory.create(props); // creates the schema

    StorageFactory storageFactory = StorageFactory.create(props);
    counter = new CountingStorage();
    storage = counter.wrap(storageFactory.getStorage());
    storageAdmin = storageFactory.getStorageAdmin();
    store =
        new ScalarDbSagaStore(
            new ConsensusCommitManager(storage, storageAdmin, new DatabaseConfig(props)),
            new ObjectMapper().deactivateDefaultTyping(),
            new SagaSchema(NUM_BUCKETS),
            ScalarDbSagaStoreConfig.builder().numBuckets(NUM_BUCKETS).build());
  }

  @AfterEach
  void tearDown() {
    store.close(); // closes the transaction manager, and with it the storage and its admin
  }

  @Test
  void storeCost_everyUseCase_staysWithinTheBaseline() {
    // Act
    measure("sync success", this::syncSuccess);
    measure("sync failure, compensated", this::syncFailure);
    measure("async, callback in time", id -> asyncCallback(id, deadline()));
    measure("async, timed out after 2 re-drives", this::asyncTimedOut);
    measure("approval, accepted", id -> asyncCallback(id, null));
    measure("approval, rejected", this::approvalRejected);
    measureAfter("poll (getStateSnapshot)", this::running, id -> store.getStateSnapshot(id));
    measureAfter(
        "detail (getStateWithEvents)",
        this::runningWithSteps,
        id -> store.getStateWithEvents(id, 1000));
    measureAfter("recovery claim", this::running, this::recoveryClaim);
    measureAfter("stuck compensation, one pass", this::stuckCompensating, this::stuckPass);
    measureAfter("shutdown hand-off", this::running, store::markForRecovery);
    measureAfter("retention purge", this::completed, store::deleteSaga);
    measure("idle sweeps (recovery + timeout + retention)", id -> idleSweeps());
    measureAfter("admin listing page", this::running, id -> listingPage());

    // Assert
    System.out.println(report());
    measured.forEach(
        (useCase, cost) -> {
          long[] baseline = BASELINE.get(useCase);
          assertThat(baseline).as(useCase + " has a baseline").isNotNull();
          if (baseline != null) {
            assertThat(cost.roundTrips())
                .as(useCase + " round trips")
                .isLessThanOrEqualTo(baseline[0]);
            assertThat(cost.recordsWritten())
                .as(useCase + " records written")
                .isLessThanOrEqualTo(baseline[1]);
          }
        });
  }

  // ---------------------------------------------------------------------------
  // Use cases: the store calls the engine makes, in order
  // ---------------------------------------------------------------------------

  private void syncSuccess(String id) {
    SagaStateSnapshot s = create(id);
    store.recordStepEvent(id, 1, StepEvent.completed(0, "reserve", "{}"));
    store.recordStepEvent(id, 2, StepEvent.completed(1, "charge", "{}"));
    store.recordStepEvent(id, 3, StepEvent.completed(2, "ship", "{}"));
    store.recordStatusEvent(s, 4, StatusEvent.completed(), OWNER);
  }

  private void syncFailure(String id) {
    SagaStateSnapshot s = create(id);
    store.recordStepEvent(id, 1, StepEvent.completed(0, "reserve", "{}"));
    store.recordStepEvent(id, 2, StepEvent.completed(1, "charge", "{}"));
    store.recordStepEvent(id, 3, StepEvent.failed(2, "ship", null));
    s = store.recordStatusEvent(s, 4, StatusEvent.compensating(), OWNER);
    store.recordStepEvent(id, 5, StepEvent.compensated(2, "ship"));
    store.recordStepEvent(id, 6, StepEvent.compensated(1, "charge"));
    store.recordStepEvent(id, 7, StepEvent.compensated(0, "reserve"));
    store.recordStatusEvent(s, 8, StatusEvent.compensated(), OWNER);
  }

  private void asyncCallback(String id, @Nullable Instant deadline) {
    SagaStateSnapshot s = create(id);
    store.recordStepEvent(id, 1, StepEvent.completed(0, "reserve", "{}"));
    store.park(s, 2, StepEvent.pending(1, "approve"), deadline);
    // The callback reads the saga, then resumes it.
    SagaStateSnapshot waiting = store.getStateSnapshot(id).orElseThrow();
    store.getEvents(id);
    s = store.resumeParkedStep(waiting, 3, StepEvent.completed(1, "approve", "{}"));
    store.recordStepEvent(id, 4, StepEvent.completed(2, "ship", "{}"));
    store.recordStatusEvent(s, 5, StatusEvent.completed(), OWNER);
  }

  private void asyncTimedOut(String id) {
    SagaStateSnapshot s = create(id);
    store.recordStepEvent(id, 1, StepEvent.completed(0, "reserve", "{}"));
    store.park(s, 2, StepEvent.pending(1, "approve"), deadline());
    int sequence = 3;
    for (int redrive = 0; redrive < 2; redrive++) {
      SagaStateSnapshot waiting = store.getStateSnapshot(id).orElseThrow();
      store.getEvents(id);
      s = store.redriveParkedStep(waiting, sequence++, StepEvent.reissuing(1, "approve"));
      store.park(s, sequence++, StepEvent.pending(1, "approve"), deadline());
    }
    SagaStateSnapshot waiting = store.getStateSnapshot(id).orElseThrow();
    store.getEvents(id);
    s =
        store.failParkedStep(
            waiting, sequence++, StepEvent.failed(1, "approve", null), SagaStatus.COMPENSATING);
    store.recordStepEvent(id, sequence++, StepEvent.compensated(1, "approve"));
    store.recordStepEvent(id, sequence++, StepEvent.compensated(0, "reserve"));
    store.recordStatusEvent(s, sequence, StatusEvent.compensated(), OWNER);
  }

  private void approvalRejected(String id) {
    SagaStateSnapshot s = create(id);
    store.recordStepEvent(id, 1, StepEvent.completed(0, "reserve", "{}"));
    store.park(s, 2, StepEvent.pending(1, "approve"), null);
    SagaStateSnapshot waiting = store.getStateSnapshot(id).orElseThrow();
    store.getEvents(id);
    s =
        store.failParkedStep(
            waiting, 3, StepEvent.failed(1, "approve", null), SagaStatus.COMPENSATING);
    store.recordStepEvent(id, 4, StepEvent.compensated(1, "approve"));
    store.recordStepEvent(id, 5, StepEvent.compensated(0, "reserve"));
    store.recordStatusEvent(s, 6, StatusEvent.compensated(), OWNER);
  }

  /** Probe twice (screen, then under the permit), claim, then load the events to replay. */
  private void recoveryClaim(String id) {
    SagaStateSnapshot stale = store.getStateSnapshot(id).orElseThrow();
    store.getNewestEvent(id);
    store.getNewestEvent(id);
    store.claimForRecovery(stale, "engine-2");
    store.getEvents(id);
  }

  /** One recovery pass over a saga whose compensation keeps failing. */
  private void stuckPass(String id) {
    SagaStateSnapshot stale = store.getStateSnapshot(id).orElseThrow();
    store.getNewestEvent(id);
    store.getNewestEvent(id);
    assertThat(store.claimForRecovery(stale, "engine-2")).isPresent();
    store.getEvents(id);
    store.recordStepEvent(
        id, store.getEventCount(id), StepEvent.compensationFailed(0, "reserve", null));
  }

  private void idleSweeps() {
    Instant now = Instant.now();
    SagaStore.@Nullable ScanCursor cursor = store.initialSweepCursor(OWNER);
    do {
      cursor = store.findRecoverable(now.minus(Duration.ofMinutes(1)), cursor).nextCursor();
    } while (cursor != null);
    cursor = store.initialSweepCursor(OWNER);
    do {
      cursor = store.findOverdueParkedSagas(now, cursor).nextCursor();
    } while (cursor != null);
    Instant retention = now.minus(Duration.ofDays(7));
    store.findByStatusOlderThan(SagaStatus.COMPLETED, retention, 10_000, OWNER, 0);
    store.findByStatusOlderThan(SagaStatus.COMPENSATED, retention, 10_000, OWNER, 0);
  }

  private void listingPage() {
    store.listStateSnapshots(SagaQuery.newBuilder().status(SagaStatus.RUNNING).build());
  }

  // ---------------------------------------------------------------------------
  // Set-up states, not counted
  // ---------------------------------------------------------------------------

  private SagaStateSnapshot create(String id) {
    return store.createSaga(id, "order-saga", OWNER, Map.of("amount", 100), "v1");
  }

  private void running(String id) {
    create(id);
  }

  private void runningWithSteps(String id) {
    create(id);
    store.recordStepEvent(id, 1, StepEvent.completed(0, "reserve", "{}"));
    store.recordStepEvent(id, 2, StepEvent.completed(1, "charge", "{}"));
  }

  private void stuckCompensating(String id) {
    SagaStateSnapshot s = create(id);
    store.recordStepEvent(id, 1, StepEvent.completed(0, "reserve", "{}"));
    store.recordStepEvent(id, 2, StepEvent.failed(1, "charge", null));
    store.recordStatusEvent(s, 3, StatusEvent.compensating(), OWNER);
    store.recordStepEvent(id, 4, StepEvent.compensationFailed(0, "reserve", null));
  }

  private void completed(String id) {
    syncSuccess(id);
  }

  private static Instant deadline() {
    return Instant.now().plus(Duration.ofMinutes(5));
  }

  // ---------------------------------------------------------------------------
  // Measuring
  // ---------------------------------------------------------------------------

  private int nextId;

  private void measure(String useCase, Consumer<String> action) {
    measureAfter(useCase, id -> {}, action);
  }

  private void measureAfter(String useCase, Consumer<String> setUp, Consumer<String> action) {
    String id = "saga-" + nextId++;
    setUp.accept(id);
    counter.reset();
    action.accept(id);
    measured.put(useCase, counter.cost());
  }

  private String report() {
    StringBuilder table = new StringBuilder();
    table.append("| use case | round trips | records written | rows read | bytes read |\n");
    table.append("|---|---|---|---|---|\n");
    measured.forEach(
        (useCase, cost) ->
            table
                .append("| ")
                .append(useCase)
                .append(" | ")
                .append(cost.roundTrips())
                .append(" | ")
                .append(cost.recordsWritten())
                .append(" | ")
                .append(cost.rowsRead())
                .append(" | ")
                .append(cost.bytesRead())
                .append(" |\n"));
    return table.toString();
  }
}
