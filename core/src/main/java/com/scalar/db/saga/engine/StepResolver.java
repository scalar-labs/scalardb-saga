package com.scalar.db.saga.engine;

// Imported for the Javadoc references below: these types are named throughout this interface's
// contract but appear in no signature, since resolve() is deliberately typed as Object.
import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.TccStep;

/**
 * Resolves step class names to step instances.
 *
 * <p>The engine calls this interface during saga definition registration and execution to obtain
 * {@link Step} or {@link TccStep} instances from fully-qualified class names stored in saga
 * definitions.
 *
 * <h2>Contract</h2>
 *
 * <ul>
 *   <li>Must never return {@code null} — throw an exception on resolution failure
 *   <li>Must be thread-safe — the engine may call {@code resolve} concurrently from multiple
 *       threads
 *   <li>The returned object must be an instance of {@link Step} or {@link TccStep} — the engine
 *       verifies this after resolution
 * </ul>
 *
 * <h2>Built-in implementation (default)</h2>
 *
 * <p>{@code ReflectiveStepResolver} resolves steps via reflection-based constructor injection,
 * matching constructor parameter types against registered resources and injecting an
 * {@code @Named(name) SagaHttpClient} for any {@code httpEndpoint(name, baseUrl)} on the builder.
 * With it a step just declares the client it needs — no custom resolver and no manual lookup:
 *
 * <pre>{@code
 * class DebitStep implements Step {
 *   private final SagaHttpClient http;
 *   DebitStep(SagaHttpClient http) { this.http = http; }
 *   // ...
 * }
 * // builder: .httpEndpoint("account-svc", "https://account-svc:8443").add()
 * }</pre>
 *
 * <p>With more than one endpoint registered, qualify the parameter to select one, e.g.
 * {@code @Named("account-svc") SagaHttpClient}.
 *
 * <h2>Custom implementations</h2>
 *
 * <p>Supply a custom resolver via {@link
 * DefaultSagaOrchestrator.Builder#stepResolver(StepResolver)} for full control (e.g. DI-framework
 * integration). A resolver must (a) dispatch on the step {@code name} (or {@code className}) and
 * (b) return the same thread-safe singleton each time, per the {@link Step} lifecycle contract —
 * e.g. a DI container whose beans are singletons:
 *
 * <pre>{@code
 * .stepResolver((name, className, ctx) -> applicationContext.getBean(Class.forName(className)))
 * }</pre>
 *
 * <p>To inject the framework {@link SagaHttpClient} into a manually-constructed step, obtain it
 * from the {@link ResolutionContext} — not a self-built client, which would lose the saga
 * correlation headers, SSRF allowlist, and retryable classification. The client exists only at
 * resolve time and a step may be resolved more than once, so cache by step name:
 *
 * <pre>{@code
 * String accountSvc = "account-svc";
 * Map<String, Object> steps = new ConcurrentHashMap<>();
 * DefaultSagaOrchestrator.newBuilder()
 *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
 *     .httpEndpoint(accountSvc, "https://account-svc:8443").add()
 *     .stepResolver(
 *         (name, className, ctx) ->
 *             steps.computeIfAbsent(
 *                 name,
 *                 n ->
 *                     switch (n) {
 *                       case "debit" -> new DebitStep(ctx.httpClient(accountSvc));
 *                       default -> throw new SagaDefinitionException("unknown step: " + n);
 *                     }))
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface StepResolver {

  /**
   * Resolves a step by name and fully-qualified class name.
   *
   * <p>The engine may call this method multiple times for the same step. Implementations should
   * return the same instance each time to satisfy the singleton contract of {@link Step}.
   *
   * @param stepName the step name from the saga definition
   * @param stepClass the fully-qualified class name from the saga definition
   * @param context resolution context exposing the registered {@link SagaHttpClient}s
   * @return the resolved step instance (must be a {@link Step} or {@link TccStep})
   * @throws com.scalar.db.saga.exception.SagaDefinitionException if the step cannot be resolved
   */
  Object resolve(String stepName, String stepClass, ResolutionContext context);

  /**
   * Context handed to {@link #resolve(String, String, ResolutionContext)} at resolution time. It
   * exposes the {@link SagaHttpClient}s registered via {@code httpEndpoint(name, baseUrl)} so a
   * custom resolver can inject the live, policy-enforcing client into a step it constructs.
   */
  interface ResolutionContext extends SagaHttpClientProvider {}
}
