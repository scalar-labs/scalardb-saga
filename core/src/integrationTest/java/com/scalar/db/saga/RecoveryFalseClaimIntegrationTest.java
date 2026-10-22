package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.RecoveryConfig;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.integration.IntegrationTestStore;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.store.StepEvent;
import com.scalar.db.saga.testing.ForwardingSagaStore;
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
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Recovery must not claim a saga that is still being executed.
 *
 * <p>Step execution appends plain step events and never moves the saga's sweep row, so a saga whose
 * steps outlive {@code recovery.staleness_threshold_millis} looks exactly like one whose process
 * died. Claiming it appends an event at the sequence the running drive writes next, so the drive
 * fails at its next append, and under load that snowballs.
 *
 * <p>These run the real recovery manager against a real store, because the guards live between the
 * scan and the claim: the mocked-store unit tests pin each guard's decision, but only a real pass
 * over a real row shows the drive surviving it.
 *
 * <p>Clock note: the store stamps rows from its own clock, so seeded rows cannot be backdated.
 * These tests instead push the <b>recovery</b> clock forward, which makes everything written at
 * wall-clock time look arbitrarily stale to the sweeper — the same technique {@code
 * BulkResetRecoveryDriveIntegrationTest} documents.
 */
class RecoveryFalseClaimIntegrationTest {

  private static final String SAGA_NAME = "long-step-saga";
  private static final String OWNER_ID = "owner";
  private static final String OWNER_ID_B = "owner-b";
  private static final Duration GRACE = Duration.ofHours(1);
  private static final long RECOVERY_TIMEOUT_MILLIS = 60_000;

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

  /**
   * A step that parks on a latch, so the saga can be caught mid-execution. An inner class, not a
   * static one: JUnit builds a fresh test instance per test, so per-instance latches cannot leak
   * between tests the way static ones silently did.
   */
  private final class BlockingStep implements Step {

    private final String name;

    BlockingStep(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public StepResult execute(SagaContext context) {
      executions.add(name);
      // start() only returns once the saga finishes, so a test inspecting a parked saga cannot get
      // its id from that call. The step running inside it can.
      runningSagaId.set(context.getSagaId());
      if ("first".equals(name)) {
        // Stored timestamps are millisecond-granular, and creating the saga plus completing a
        // trivial step lands inside one millisecond — leaving no threshold that can make the state
        // row stale while the event stream is fresh, which is the whole point of the probe. A short
        // real delay separates them.
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      if ("slow".equals(name)) {
        stepStarted.countDown();
        try {
          if (!stepRelease.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("step was never released");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) {
      executions.add("compensate:" + name);
    }
  }

  /**
   * A second replica's step: records its runs apart from the first replica's, and blocks in "slow"
   * on latches of its own.
   */
  private final class ReplicaBStep implements Step {

    private final String name;

    ReplicaBStep(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public StepResult execute(SagaContext context) {
      executions.add("B:" + name);
      if ("slow".equals(name)) {
        replicaBStepStarted.countDown();
        try {
          if (!replicaBStepRelease.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("replica B's step was never released");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) {
      executions.add("B:compensate:" + name);
    }
  }

  @TempDir Path tempDir;

  private final List<String> executions = Collections.synchronizedList(new ArrayList<>());
  private final AtomicReference<String> runningSagaId = new AtomicReference<>();
  private CountDownLatch stepStarted;
  private CountDownLatch stepRelease;
  private final CountDownLatch replicaBStepStarted = new CountDownLatch(1);
  private final CountDownLatch replicaBStepRelease = new CountDownLatch(1);

  private Path dbPath;
  private SagaStore store;
  private DefaultSagaOrchestrator orchestrator;
  private SettableClock clock;

  @BeforeEach
  void setUp() {
    executions.clear();
    runningSagaId.set(null);
    stepStarted = new CountDownLatch(1);
    stepRelease = new CountDownLatch(1);
    dbPath = tempDir.resolve("recovery-false-claim-it.db");
    store = ScalarDbSagaStoreFactory.create(storeProps()).createStore();
    clock = new SettableClock();

    orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .ownerId(OWNER_ID)
            .recoveryConfig(new RecoveryConfig(RECOVERY_TIMEOUT_MILLIS, 30, GRACE, 1000, 10, clock))
            .stepResolver((name, cls, ctx) -> new BlockingStep(name))
            .build();

    orchestrator.register(definition());
  }

  private static SagaDefinition definition() {
    // "first" completes at once, so a real step event exists in the store before "slow" blocks —
    // which is what a second replica's progress probe has to find.
    return SagaDefinition.newBuilder(SAGA_NAME)
        .saga()
        .step("first", "com.example.FirstStep")
        .add()
        .step("slow", "com.example.SlowStep")
        .add()
        .step("after", "com.example.AfterStep")
        .add()
        .build();
  }

  private Properties storeProps() {
    Properties props = new Properties();
    IntegrationTestStore.configure(props, dbPath);
    props.setProperty("scalar.db.saga.store.scalardb.num_buckets", "1");
    return props;
  }

  @AfterEach
  void tearDown() throws Exception {
    stepRelease.countDown(); // never leave a drive parked
    replicaBStepRelease.countDown();
    orchestrator.close(); // owns and closes the shared store
    Files.deleteIfExists(dbPath);
  }

  @Test
  void recover_driverOnAnotherReplicaWroteRecently_isNotClaimed() throws Exception {
    // The local-active check cannot help here: to a second replica the saga is simply a stale row
    // it is not driving, so only the progress probe stands between it and a destructive claim.
    // This is the path the single-replica test above never reaches, and the only coverage of the
    // real store's descending, projected event scan.

    // Arrange — replica A starts the saga and blocks in "slow", after "first" has written an event.
    AtomicReference<String> sagaId = new AtomicReference<>();
    AtomicReference<Throwable> driveFailure = new AtomicReference<>();
    Thread drive =
        new Thread(
            () -> {
              try {
                sagaId.set(orchestrator.start(SAGA_NAME, Map.of()));
              } catch (Throwable t) {
                driveFailure.set(t);
              }
            });
    drive.start();
    assertThat(stepStarted.await(30, TimeUnit.SECONDS)).isTrue();
    sagaId.set(runningSagaId.get());
    String runningId =
        Objects.requireNonNull(sagaId.get(), "the step ran without recording its saga id");

    // Replica B: its own store on the same database, its own owner id and clock.
    SagaStore storeB = ScalarDbSagaStoreFactory.create(storeProps()).createStore();
    SettableClock clockB = new SettableClock();
    DefaultSagaOrchestrator replicaB =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> storeB)
            .ownerId(OWNER_ID_B)
            .recoveryConfig(
                new RecoveryConfig(RECOVERY_TIMEOUT_MILLIS, 30, GRACE, 1000, 10, clockB))
            .stepResolver((name, cls, ctx) -> new BlockingStep(name))
            .build();
    try {
      replicaB.register(definition());

      // Put B's threshold exactly at the newest event, so the saga's creation-time row is stale to
      // B while its event stream is not. Reading the stamp back also exercises the real scan.
      Instant newestEvent = storeB.getNewestEvent(runningId).orElseThrow().createdAt();
      clockB.set(newestEvent.plusMillis(RECOVERY_TIMEOUT_MILLIS));

      // The saga really is a candidate at that threshold — the probe is what spares it, not the
      // scan failing to reach it. Without this the test could pass for the wrong reason.
      Instant stateUpdatedAt = storeB.getStateSnapshot(runningId).orElseThrow().getUpdatedAt();
      assertThat(storeB.findRecoverable(newestEvent, storeB.initialSweepCursor(OWNER_ID_B)).sagas())
          .as("newestEvent=%s stateUpdatedAt=%s", newestEvent, stateUpdatedAt)
          .extracting(SagaStateSnapshot::getSagaId)
          .contains(runningId);

      // Act — a full recovery pass from the other replica
      replicaB.recover();

      // Assert — B left it alone. Every claim appends a SAGA_CLAIMED event, so the log proves no
      // claim landed.
      assertThat(storeB.getEvents(runningId))
          .extracting(SagaEvent::getEventType)
          .doesNotContain(EventType.SAGA_CLAIMED);
      assertThat(driveFailure.get()).isNull();

      // Act — release A and let it finish
      stepRelease.countDown();
      drive.join(30_000);

      // Assert — A completed normally, each step once, with no redrive from B
      assertThat(driveFailure.get()).isNull();
      assertThat(orchestrator.getStateSnapshot(runningId).getStatus())
          .isEqualTo(SagaStatus.COMPLETED);
      assertThat(executions).containsExactly("first", "slow", "after");
    } finally {
      replicaB.close();
    }
  }

  @Test
  void recover_sagaStillExecutingPastTheTimeout_isNotClaimedAndCompletesNormally()
      throws Exception {
    // Arrange — start the saga on another thread and catch it inside its first step.
    AtomicReference<String> sagaId = new AtomicReference<>();
    AtomicReference<Throwable> driveFailure = new AtomicReference<>();
    Thread drive =
        new Thread(
            () -> {
              try {
                sagaId.set(orchestrator.start(SAGA_NAME, Map.of()));
              } catch (Throwable t) {
                driveFailure.set(t);
              }
            });
    drive.start();
    assertThat(stepStarted.await(30, TimeUnit.SECONDS)).isTrue();

    // Push the recovery clock an hour past wall time: every row written by the live drive is now
    // far older than the staleness threshold, so nothing but the guards keeps this saga safe.
    clock.set(Instant.now().plus(Duration.ofHours(1)));

    // Act — a full recovery pass while the saga is mid-step.
    orchestrator.recover();

    // Assert — the pass left the row alone, so the drive is still alive and its step ran once.
    assertThat(driveFailure.get()).isNull();
    assertThat(executions).containsExactly("first", "slow");

    // Act — release the step and let the saga finish.
    stepRelease.countDown();
    drive.join(30_000);

    // Assert — it completed normally, with no step executed twice. Before the guards, the pass
    // above would have claimed the live saga and killed this drive at its next transition.
    assertThat(drive.isAlive()).isFalse();
    assertThat(driveFailure.get()).isNull();
    SagaStateSnapshot result =
        orchestrator.getStateSnapshot(
            Objects.requireNonNull(sagaId.get(), "the drive never returned a saga id"));
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(executions).containsExactly("first", "slow", "after");
  }

  @Test
  void recover_slowOwnerClaimedByAnotherReplica_isStoppedAtItsNextStepEvent() throws Exception {
    // A replica that is only slow, not dead, is claimed once its step outlives the window. The
    // claim takes the sequence that replica writes next, so it stops at its very next step event
    // and never runs another step alongside the new owner.

    // Arrange — replica A blocks in "slow"; replica B's clock runs an hour ahead, so A looks dead
    AtomicReference<Throwable> driveFailure = new AtomicReference<>();
    Thread drive =
        new Thread(
            () -> {
              try {
                orchestrator.start(SAGA_NAME, Map.of());
              } catch (Throwable t) {
                driveFailure.set(t);
              }
            });
    drive.start();
    assertThat(stepStarted.await(30, TimeUnit.SECONDS)).isTrue();
    String runningId =
        Objects.requireNonNull(runningSagaId.get(), "the step ran without recording its saga id");

    SagaStore storeB = ScalarDbSagaStoreFactory.create(storeProps()).createStore();
    SettableClock clockB = new SettableClock();
    clockB.set(Instant.now().plus(Duration.ofHours(1)));
    DefaultSagaOrchestrator replicaB =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> storeB)
            .ownerId(OWNER_ID_B)
            .recoveryConfig(
                new RecoveryConfig(RECOVERY_TIMEOUT_MILLIS, 30, GRACE, 1000, 10, clockB))
            .stepResolver((name, cls, ctx) -> new ReplicaBStep(name))
            .build();
    try {
      replicaB.register(definition());
      Thread recovery = new Thread(replicaB::recover);
      recovery.start();
      assertThat(replicaBStepStarted.await(30, TimeUnit.SECONDS)).isTrue();

      // Act — A's step returns while B holds the saga
      stepRelease.countDown();
      drive.join(30_000);

      // Assert — A recorded nothing after the claim and never reached "after"
      assertThat(drive.isAlive()).isFalse();
      assertThat(driveFailure.get()).isNotNull();
      assertThat(executions).containsExactly("first", "slow", "B:slow");
      assertThat(storeB.getEvents(runningId))
          .extracting(SagaEvent::getEventType)
          .containsExactly(
              EventType.SAGA_STARTED, EventType.STEP_COMPLETED, EventType.SAGA_CLAIMED);

      // Act — B finishes the saga
      replicaBStepRelease.countDown();
      recovery.join(30_000);

      // Assert
      awaitStatus(storeB, runningId, SagaStatus.COMPLETED);
      assertThat(executions).containsExactly("first", "slow", "B:slow", "B:after");
    } finally {
      replicaBStepRelease.countDown();
      replicaB.close();
    }
  }

  @Test
  void close_stepStillRunningAtTheHandOff_recordsNoFailureAndRecoveryRunsItAgain()
      throws Exception {
    // The shutdown hand-off appends its event at the sequence the drive writes next. The step the
    // shutdown interrupts therefore cannot record a failure and start a compensation nobody asked
    // for; recovery runs the step again instead, which the idempotency contract covers.

    // Arrange — a replica whose store stays open past its close, so the drive's last write reaches
    // the database and is refused there rather than by a closed store
    List<RuntimeException> refusedWrites = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch slowStepWriteAttempted = new CountDownLatch(1);
    SagaStore keptOpen =
        new ForwardingSagaStore(store) {
          @Override
          public void recordStepEvent(String sagaId, int sequence, StepEvent event) {
            try {
              super.recordStepEvent(sagaId, sequence, event);
            } catch (RuntimeException e) {
              refusedWrites.add(e);
              throw e;
            } finally {
              if ("slow".equals(event.getStepName())) {
                slowStepWriteAttempted.countDown();
              }
            }
          }

          @Override
          public void close() {
            // The test's own orchestrator closes the store.
          }
        };
    DefaultSagaOrchestrator stopping =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> keptOpen)
            .ownerId("stopping")
            .recoveryConfig(new RecoveryConfig(RECOVERY_TIMEOUT_MILLIS, 30, GRACE, 1000, 10, clock))
            .stepResolver((name, cls, ctx) -> new BlockingStep(name))
            .build();
    stopping.register(definition());
    Thread drive =
        new Thread(
            () -> {
              try {
                stopping.start(SAGA_NAME, Map.of());
              } catch (Throwable t) {
                // the drive is expected to fail once it is handed off
              }
            });
    drive.start();
    assertThat(stepStarted.await(30, TimeUnit.SECONDS)).isTrue();
    String runningId =
        Objects.requireNonNull(runningSagaId.get(), "the step ran without recording its saga id");

    // Act — shut down with a short budget: the step is still running when it runs out
    stopping.close(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200));
    assertThat(slowStepWriteAttempted.await(30, TimeUnit.SECONDS)).isTrue();
    drive.join(30_000);

    // Assert — the interrupted step's failure was refused, so nothing compensates
    assertThat(refusedWrites)
        .singleElement()
        .isInstanceOf(SagaConcurrentModificationException.class);
    assertThat(store.getEvents(runningId))
        .extracting(SagaEvent::getEventType)
        .containsExactly(EventType.SAGA_STARTED, EventType.STEP_COMPLETED, EventType.SAGA_RELEASED);
    SagaStateSnapshot handedOff = store.getStateSnapshot(runningId).orElseThrow();
    assertThat(handedOff.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(handedOff.getUpdatedAt()).isEqualTo(Instant.EPOCH);

    // Act — another replica takes the hand-off at once
    stepRelease.countDown();
    orchestrator.recover();

    // Assert — "slow" ran again, and the saga finished without compensating
    awaitStatus(store, runningId, SagaStatus.COMPLETED);
    assertThat(executions).containsExactly("first", "slow", "slow", "after");
  }

  private static void awaitStatus(SagaStore store, String sagaId, SagaStatus expected)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (store.getStateSnapshot(sagaId).orElseThrow().getStatus() != expected) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("saga " + sagaId + " never reached " + expected);
      }
      Thread.sleep(50);
    }
  }
}
