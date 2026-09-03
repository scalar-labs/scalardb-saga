package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaOverloadedException;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.testing.FakeStep;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The admission cap against a real store, where the semantics live.
 *
 * <p>The unit tests pin each decision with a mocked engine; these exist for the claims that are
 * about the whole path — that a refusal really leaves nothing behind for the next attempt to trip
 * over, and that a parked saga really stops occupying a seat once its state is durable. Both are
 * properties of the store as much as of the semaphore.
 *
 * <p>Cap of 1 throughout: it is the smallest configuration that can be saturated deterministically,
 * and every assertion here is about the boundary rather than about a number.
 */
class AdmissionControlIntegrationTest {

  private static final String SAGA_NAME = "capped-saga";
  private static final String STEP_CLASS = "com.example.Step";

  @TempDir Path tempDir;

  private Path dbPath;
  private Properties props;

  /** Held open by the blocking step so a drive can be pinned in flight. */
  private CountDownLatch stepRelease;

  private CountDownLatch stepStarted;

  @BeforeEach
  void setUp() {
    dbPath = tempDir.resolve("admission-control-it.db");
    props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + dbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    stepRelease = new CountDownLatch(1);
    stepStarted = new CountDownLatch(1);
  }

  @AfterEach
  void tearDown() throws Exception {
    stepRelease.countDown(); // never leave a drive pinned
    Files.deleteIfExists(dbPath);
  }

  private DefaultSagaOrchestrator orchestrator(int cap, Map<String, Object> steps) {
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(ScalarDbSagaStoreFactory.create(props))
            .maxConcurrentSagaExecutions(cap)
            .stepResolver(
                (name, cls, ctx) -> {
                  Object step = steps.get(name);
                  if (step == null) {
                    throw new IllegalArgumentException("No step registered for: " + name);
                  }
                  return step;
                })
            .build();
    orchestrator.register(definition(SAGA_NAME));
    return orchestrator;
  }

  private static SagaDefinition definition(String name) {
    return SagaDefinition.newBuilder(name)
        .saga()
        .timeoutMillis(60_000)
        .step("only", STEP_CLASS)
        .add()
        .build();
  }

  /** A step that pins its drive until the test lets go, so the cap can be saturated on purpose. */
  private final class BlockingStep implements Step {

    @Override
    public String getName() {
      return "only";
    }

    @Override
    public StepResult execute(SagaContext context) {
      stepStarted.countDown();
      try {
        if (!stepRelease.await(30, TimeUnit.SECONDS)) {
          throw new IllegalStateException("the test never released the drive");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) {
      // Nothing to undo: this step exists only to hold its drive in flight.
    }
  }

  @Test
  void start_atTheCap_refusesWithoutPersistingAnything() throws Exception {
    // The claim that makes "retry" honest advice: a refused start leaves no saga and no consumed
    // ID, so the same request — including the same caller-supplied ID — succeeds afterwards.
    // Arrange
    try (DefaultSagaOrchestrator orchestrator =
            orchestrator(1, Map.of("only", new BlockingStep()));
        ExecutorService caller = Executors.newSingleThreadExecutor()) {
      caller.execute(() -> orchestrator.start("held-saga", SAGA_NAME, Map.of()));
      assertThat(stepStarted.await(30, TimeUnit.SECONDS)).isTrue();

      // Act
      assertThatThrownBy(() -> orchestrator.start("refused-saga", SAGA_NAME, Map.of()))
          .isInstanceOf(SagaOverloadedException.class);

      // Assert — nothing was written under the refused ID.
      assertThatThrownBy(() -> orchestrator.getStateSnapshot("refused-saga"))
          .isInstanceOf(SagaNotFoundException.class);

      // And the same ID is still usable once a seat frees up.
      stepRelease.countDown();
      caller.shutdown();
      assertThat(caller.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      assertThatCode(() -> orchestrator.start("refused-saga", SAGA_NAME, Map.of()))
          .doesNotThrowAnyException();
      assertThat(orchestrator.getStateSnapshot("refused-saga").getStatus())
          .isEqualTo(SagaStatus.COMPLETED);
    }
  }

  @Test
  void start_parkedSaga_freesItsSeat() {
    // A saga waiting on an outside system can wait for hours. Holding a seat for it would turn the
    // cap into a bound on outstanding sagas rather than on work in progress, which is not what an
    // operator sizes it against.
    // Arrange — the step parks instead of completing.
    FakeStep parking = FakeStep.newBuilder("only").executeReturns(StepResult.pending()).build();

    try (DefaultSagaOrchestrator orchestrator = orchestrator(1, Map.of("only", parking))) {
      // Act
      orchestrator.start("parked-saga", SAGA_NAME, Map.of());

      // Assert — parked, and the seat came back with it.
      assertThat(orchestrator.getStateSnapshot("parked-saga").getStatus())
          .isEqualTo(SagaStatus.WAITING);
      assertThatCode(() -> orchestrator.start("next-saga", SAGA_NAME, Map.of()))
          .doesNotThrowAnyException();
    }
  }

  @Test
  void start_unknownDefinitionAtTheCap_reportsTheDefinitionErrorNotOverload() throws Exception {
    // Validation answers first even when the engine is full: a caller told to retry a saga that
    // does not exist would loop forever on a request that can never succeed.
    // Arrange
    try (DefaultSagaOrchestrator orchestrator =
            orchestrator(1, Map.of("only", new BlockingStep()));
        ExecutorService caller = Executors.newSingleThreadExecutor()) {
      caller.execute(() -> orchestrator.start("held-saga", SAGA_NAME, Map.of()));
      assertThat(stepStarted.await(30, TimeUnit.SECONDS)).isTrue();

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.start("no-such-saga", Map.of()))
          .isInstanceOf(SagaDefinitionNotFoundException.class);

      stepRelease.countDown();
      caller.shutdown();
      assertThat(caller.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void startAsync_atTheCap_refusesWithoutPersistingAnything() throws Exception {
    // The async path persists before it drives, so the refusal has to land before createSaga or the
    // caller would hold an ID for a saga that was written and never run.
    // Arrange
    try (DefaultSagaOrchestrator orchestrator =
        orchestrator(1, Map.of("only", new BlockingStep()))) {
      orchestrator.startAsync("held-saga", SAGA_NAME, Map.of());
      assertThat(stepStarted.await(30, TimeUnit.SECONDS)).isTrue();

      // Act
      assertThatThrownBy(() -> orchestrator.startAsync("refused-saga", SAGA_NAME, Map.of()))
          .isInstanceOf(SagaOverloadedException.class);

      // Assert
      assertThatThrownBy(() -> orchestrator.getStateSnapshot("refused-saga"))
          .isInstanceOf(SagaNotFoundException.class);
      stepRelease.countDown();
    }
  }

  @Test
  void start_withNoCapConfigured_admitsBeyondAnyPlausibleCap() throws Exception {
    // The default has to stay the old behavior exactly: no controller, no bound.
    // Arrange
    FakeStep parking = FakeStep.newBuilder("only").executeReturns(StepResult.pending()).build();

    try (DefaultSagaOrchestrator orchestrator = orchestrator(0, Map.of("only", parking))) {
      // Act & Assert
      for (int i = 0; i < 25; i++) {
        String sagaId = "saga-" + i;
        assertThatCode(() -> orchestrator.start(sagaId, SAGA_NAME, Map.of()))
            .doesNotThrowAnyException();
      }
      assertThat(orchestrator.getStateSnapshot("saga-24").getStatus())
          .isEqualTo(SagaStatus.WAITING);
    }
  }
}
