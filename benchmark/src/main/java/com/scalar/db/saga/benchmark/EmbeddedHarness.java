package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.RecoveryConfig;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The embedded mode: a {@link DefaultSagaOrchestrator} in this process, running {@link
 * BenchmarkStep}s against the configured ScalarDB store (a throwaway SQLite database by default).
 * Background recovery and retention run exactly as they would in an application, so their
 * interaction with the live workload — recovery re-claiming slow sagas above all — is part of what
 * the benchmark measures.
 */
final class EmbeddedHarness implements BenchHarness {

  private final DefaultSagaOrchestrator orchestrator;
  private final List<BenchmarkStep> steps;
  private final @Nullable Path tempDbPath;
  private final String description;

  private EmbeddedHarness(
      DefaultSagaOrchestrator orchestrator,
      List<BenchmarkStep> steps,
      @Nullable Path tempDbPath,
      String description) {
    this.orchestrator = orchestrator;
    this.steps = steps;
    this.tempDbPath = tempDbPath;
    this.description = description;
  }

  /**
   * Builds the orchestrator, registers the benchmark definition, and starts background tasks.
   *
   * @param propertiesFile ScalarDB store properties, or {@code null} for a throwaway SQLite file
   * @param overrides extra properties applied on top (e.g. {@code
   *     scalar.db.saga.store.num_buckets})
   * @param sagaName the definition name to register and start
   * @param stepCount sequential steps per saga
   * @param stepDelayMillis per-step sleep emulating participant latency
   * @param stalenessThresholdMillis staleness threshold override, {@code 0} for the engine default
   *     — shrinking it makes recovery contend with live sagas without waiting a full minute
   * @param recoveryIntervalSeconds recovery sweep interval override, {@code 0} for the default
   */
  static EmbeddedHarness create(
      @Nullable Path propertiesFile,
      Map<String, String> overrides,
      String sagaName,
      int stepCount,
      long stepDelayMillis,
      long stalenessThresholdMillis,
      long recoveryIntervalSeconds) {
    HarnessSupport.StoreSetup store = HarnessSupport.storeProperties(propertiesFile);
    HarnessSupport.applyOverrides(store.properties(), overrides);

    Map<String, BenchmarkStep> stepsByName = new LinkedHashMap<>();
    for (int i = 0; i < stepCount; i++) {
      String name = BenchmarkDefinitions.stepName(i);
      stepsByName.put(name, new BenchmarkStep(name, stepDelayMillis));
    }

    DefaultSagaOrchestrator orchestrator = null;
    try {
      DefaultSagaOrchestrator.Builder builder =
          DefaultSagaOrchestrator.newBuilder()
              .storeFactory(ScalarDbSagaStoreFactory.create(store.properties()))
              .stepResolver(
                  (name, stepClass, context) -> {
                    BenchmarkStep step = stepsByName.get(name);
                    if (step == null) {
                      throw SagaDefinitionException.stepClassInvalid(
                          stepClass, "unknown benchmark step '" + name + "'");
                    }
                    return step;
                  });
      if (stalenessThresholdMillis > 0 || recoveryIntervalSeconds > 0) {
        RecoveryConfig defaults = RecoveryConfig.defaults();
        builder.recoveryConfig(
            new RecoveryConfig(
                stalenessThresholdMillis > 0
                    ? stalenessThresholdMillis
                    : defaults.stalenessThresholdMillis(),
                recoveryIntervalSeconds > 0 ? recoveryIntervalSeconds : defaults.intervalSeconds(),
                defaults.compensationGracePeriod(),
                defaults.maxRecoveriesPerSweep(),
                defaults.maxConcurrentRecoveries(),
                defaults.clock()));
      }
      orchestrator = builder.build();
      orchestrator.register(BenchmarkDefinitions.embeddedDefinition(sagaName, stepCount));
      orchestrator.startBackgroundTasks();
      return new EmbeddedHarness(
          orchestrator,
          List.copyOf(stepsByName.values()),
          store.tempDbPath(),
          "embedded (" + store.description() + ")");
    } catch (RuntimeException e) {
      if (orchestrator != null) {
        orchestrator.close();
      }
      HarnessSupport.deleteQuietly(store.tempDbPath());
      throw e;
    }
  }

  @Override
  public DefaultSagaOrchestrator orchestrator() {
    return orchestrator;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public long duplicateStepExecutions() {
    long total = 0;
    for (BenchmarkStep step : steps) {
      total += step.duplicateExecutions();
    }
    return total;
  }

  @Override
  public void close() {
    try {
      orchestrator.close();
    } finally {
      HarnessSupport.deleteQuietly(tempDbPath);
    }
  }
}
