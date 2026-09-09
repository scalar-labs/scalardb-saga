package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.definition.SagaDefinition;

/**
 * Builds the benchmark saga definitions: {@code steps} sequential no-op steps named {@code step-0
 * .. step-N-1}. The embedded form uses class steps resolved to {@link BenchmarkStep} instances; the
 * daemon form (declarative-only) uses service steps against a registered service whose paths the
 * benchmark {@link Participant} serves.
 */
final class BenchmarkDefinitions {

  /** The service name the daemon-mode definition calls and the harness registers. */
  static final String SERVICE = "bench";

  private BenchmarkDefinitions() {}

  /** The name of step {@code index}. */
  static String stepName(int index) {
    return "step-" + index;
  }

  /** The embedded-mode definition: {@code steps} class steps backed by {@link BenchmarkStep}. */
  static SagaDefinition embeddedDefinition(String sagaName, int steps) {
    checkSteps(steps);
    SagaDefinition.SagaBuilder builder = SagaDefinition.newBuilder(sagaName).saga();
    for (int i = 0; i < steps; i++) {
      builder = builder.step(stepName(i), BenchmarkStep.class).add();
    }
    return builder.build();
  }

  /**
   * The daemon-mode definition as JSON: {@code steps} service steps against {@value #SERVICE}, step
   * {@code i} executing {@code POST /step/i} and compensating with {@code POST /undo/i}.
   */
  static String serviceDefinitionJson(String sagaName, int steps) {
    checkSteps(steps);
    StringBuilder sb = new StringBuilder();
    sb.append("{ \"name\": \"").append(sagaName).append("\", \"mode\": \"SAGA\", \"steps\": [\n");
    for (int i = 0; i < steps; i++) {
      if (i > 0) {
        sb.append(",\n");
      }
      sb.append("  { \"name\": \"")
          .append(stepName(i))
          .append("\", \"service\": \"")
          .append(SERVICE)
          .append("\",\n")
          .append("    \"execution\":    { \"method\": \"POST\", \"path\": \"/step/")
          .append(i)
          .append("\" },\n")
          .append("    \"compensation\": { \"method\": \"POST\", \"path\": \"/undo/")
          .append(i)
          .append("\" } }");
    }
    sb.append("\n] }\n");
    return sb.toString();
  }

  private static void checkSteps(int steps) {
    if (steps < 1) {
      throw new IllegalArgumentException("steps must be >= 1, got " + steps);
    }
  }
}
