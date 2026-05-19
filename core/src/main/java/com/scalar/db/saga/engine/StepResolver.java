package com.scalar.db.saga.engine;

/**
 * Resolves step class names to step instances.
 *
 * <p>The engine calls this interface during saga definition registration and execution to obtain
 * {@link com.scalar.db.saga.api.Step} or {@link com.scalar.db.saga.api.TccStep} instances from
 * fully-qualified class names stored in saga definitions.
 *
 * <h3>Contract</h3>
 *
 * <ul>
 *   <li>Must never return {@code null} — throw an exception on resolution failure
 *   <li>Must be thread-safe — the engine may call {@code resolve} concurrently from multiple
 *       threads
 *   <li>The returned object must be an instance of {@link com.scalar.db.saga.api.Step} or {@link
 *       com.scalar.db.saga.api.TccStep} — the engine verifies this after resolution
 * </ul>
 *
 * <h3>Built-in implementations</h3>
 *
 * <p>The default implementation ({@code ReflectiveStepResolver}) resolves steps via
 * reflection-based constructor injection, matching constructor parameter types against registered
 * resources.
 *
 * <h3>Custom implementations</h3>
 *
 * <p>Supply a custom resolver via {@link SagaManagerBuilder#stepResolver(StepResolver)} for full
 * control over step instantiation (e.g., manual lookup, DI framework integration). The returned
 * instances must conform to the lifecycle contract documented in {@link
 * com.scalar.db.saga.api.Step}: steps are application-level singletons shared across concurrent
 * executions and must be thread-safe.
 *
 * <pre>{@code
 * SagaManagerBuilder.newBuilder()
 *     .store(store)
 *     .stepResolver((name, className) -> applicationContext.getBean(Class.forName(className)))
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface StepResolver {

  /**
   * Resolves a step by name and fully-qualified class name.
   *
   * <p>The engine may call this method multiple times for the same step. Implementations should
   * return the same instance each time to satisfy the singleton contract of {@link
   * com.scalar.db.saga.api.Step}.
   *
   * @param stepName the step name from the saga definition
   * @param stepClass the fully-qualified class name from the saga definition
   * @return the resolved step instance (must be a {@link com.scalar.db.saga.api.Step} or {@link
   *     com.scalar.db.saga.api.TccStep})
   * @throws com.scalar.db.saga.exception.SagaDefinitionException if the step cannot be resolved
   */
  Object resolve(String stepName, String stepClass);
}
