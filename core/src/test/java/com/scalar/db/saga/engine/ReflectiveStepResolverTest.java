package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.Named;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResolver.ResolutionContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ReflectiveStepResolverTest {

  /** A {@link ResolutionContext} with no registered HTTP endpoints. */
  private static final ResolutionContext NO_ENDPOINTS = contextOf(Map.of());

  /**
   * Builds a {@link ResolutionContext} over a fixed {@code name → client} map, applying the same
   * "by name, or the sole endpoint when unqualified" rule as the real registry.
   */
  private static ResolutionContext contextOf(Map<String, SagaHttpClient> endpoints) {
    return new ResolutionContext() {
      @Override
      public SagaHttpClient httpClient(String name) {
        SagaHttpClient client = endpoints.get(name);
        if (client == null) {
          throw new SagaDefinitionException("no endpoint: " + name);
        }
        return client;
      }

      @Override
      public SagaHttpClient httpClient() {
        if (endpoints.isEmpty()) {
          throw new SagaDefinitionException("no HTTP endpoint registered");
        }
        if (endpoints.size() > 1) {
          throw new SagaDefinitionException(
              "multiple HTTP endpoints registered: " + endpoints.keySet());
        }
        return endpoints.values().iterator().next();
      }
    };
  }

  // ---------------------------------------------------------------------------
  // No-arg resolution
  // ---------------------------------------------------------------------------

  @Nested
  class NoArgResolution {

    @Test
    void resolve_noArgStep_returnsStepInstance() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object result = resolver.resolve("step1", NoArgStep.class.getName(), NO_ENDPOINTS);

      // Assert
      assertThat(result).isInstanceOf(NoArgStep.class);
    }

    @Test
    void resolve_noArgTccStep_returnsTccStepInstance() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object result = resolver.resolve("step1", NoArgTccStep.class.getName(), NO_ENDPOINTS);

      // Assert
      assertThat(result).isInstanceOf(NoArgTccStep.class);
    }
  }

  // ---------------------------------------------------------------------------
  // Constructor injection
  // ---------------------------------------------------------------------------

  @Nested
  class ConstructorInjection {

    @Test
    void resolve_singleParamConstructor_injectsResource() {
      // Arrange
      String resource = "injected-value";
      ResourceRegistry registry = ResourceRegistry.newBuilder().add(String.class, resource).build();
      ReflectiveStepResolver resolver = new ReflectiveStepResolver(registry);

      // Act
      Object result = resolver.resolve("step1", SingleParamStep.class.getName(), NO_ENDPOINTS);

      // Assert
      assertThat(result).isInstanceOf(SingleParamStep.class);
      assertThat(((SingleParamStep) result).value).isEqualTo(resource);
    }

    @Test
    void resolve_multiParamConstructor_injectsAllResources() {
      // Arrange
      ResourceRegistry registry =
          ResourceRegistry.newBuilder()
              .add(String.class, "str-value")
              .add(Integer.class, 42)
              .build();
      ReflectiveStepResolver resolver = new ReflectiveStepResolver(registry);

      // Act
      Object result = resolver.resolve("step1", MultiParamStep.class.getName(), NO_ENDPOINTS);

      // Assert
      assertThat(result).isInstanceOf(MultiParamStep.class);
      MultiParamStep step = (MultiParamStep) result;
      assertThat(step.strValue).isEqualTo("str-value");
      assertThat(step.intValue).isEqualTo(42);
    }

    @Test
    void resolve_withNamedQualifier_injectsCorrectResource() {
      // Arrange
      ResourceRegistry registry =
          ResourceRegistry.newBuilder()
              .add(String.class, "source-channel", "source")
              .add(String.class, "target-channel", "target")
              .build();
      ReflectiveStepResolver resolver = new ReflectiveStepResolver(registry);

      // Act
      Object result = resolver.resolve("step1", NamedParamStep.class.getName(), NO_ENDPOINTS);

      // Assert
      assertThat(result).isInstanceOf(NamedParamStep.class);
      NamedParamStep step = (NamedParamStep) result;
      assertThat(step.sourceChannel).isEqualTo("source-channel");
      assertThat(step.targetChannel).isEqualTo("target-channel");
    }

    @Test
    void resolve_multiplePublicConstructors_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("step1", MultiConstructorStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }
  }

  // ---------------------------------------------------------------------------
  // Singleton caching
  // ---------------------------------------------------------------------------

  @Nested
  class SingletonCaching {

    @Test
    void resolve_calledTwice_returnsSameInstance() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object first = resolver.resolve("step1", NoArgStep.class.getName(), NO_ENDPOINTS);
      Object second = resolver.resolve("step1", NoArgStep.class.getName(), NO_ENDPOINTS);

      // Assert
      assertThat(first).isSameAs(second);
    }

    @Test
    void resolve_differentStepNamesSameClass_returnsSameInstance() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object first = resolver.resolve("step-a", NoArgStep.class.getName(), NO_ENDPOINTS);
      Object second = resolver.resolve("step-b", NoArgStep.class.getName(), NO_ENDPOINTS);

      // Assert
      assertThat(first).isSameAs(second);
    }
  }

  // ---------------------------------------------------------------------------
  // Error cases
  // ---------------------------------------------------------------------------

  @Nested
  class ErrorCases {

    @Test
    void resolve_classNotFound_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", "com.nonexistent.FakeStep", NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_classNotStepOrTccStep_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", NotAStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_abstractClass_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("step1", AbstractStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_interfaceClass_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", Step.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_noPublicConstructor_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("step1", PrivateConstructorStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_noSuitableConstructor_throwsSagaDefinitionException() {
      // Arrange — step requires Long, but only String is registered
      ResourceRegistry registry = ResourceRegistry.newBuilder().add(String.class, "unused").build();
      ReflectiveStepResolver resolver = new ReflectiveStepResolver(registry);

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("step1", RequiresLongStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_missingResource_throwsSagaDefinitionException() {
      // Arrange — step requires String, but registry is empty
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("step1", RequiresStringStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_constructorThrows_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () ->
                  resolver.resolve("step1", ThrowingConstructorStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_namedQualifierMismatch_throwsSagaDefinitionException() {
      // Arrange — step expects @Named("source") but only "other" is registered
      ResourceRegistry registry =
          ResourceRegistry.newBuilder().add(String.class, "value", "other").build();
      ReflectiveStepResolver resolver = new ReflectiveStepResolver(registry);

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("step1", NamedSourceOnlyStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }
  }

  // ---------------------------------------------------------------------------
  // Thread safety
  // ---------------------------------------------------------------------------

  @Nested
  class ThreadSafety {

    @Test
    void resolve_concurrentCalls_instantiatesOnce() throws InterruptedException {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());
      int threadCount = 10;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(threadCount);
      AtomicReference<Object> firstResult = new AtomicReference<>();
      AtomicInteger mismatchCount = new AtomicInteger(0);

      // Act
      for (int i = 0; i < threadCount; i++) {
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    startLatch.await();
                    Object result =
                        resolver.resolve("step1", CountingStep.class.getName(), NO_ENDPOINTS);
                    if (!firstResult.compareAndSet(null, result)) {
                      if (firstResult.get() != result) {
                        mismatchCount.incrementAndGet();
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    doneLatch.countDown();
                  }
                });
      }
      startLatch.countDown();
      doneLatch.await();

      // Assert — all threads got the same instance
      assertThat(mismatchCount.get()).isZero();
      // CountingStep tracks how many times it was constructed
      assertThat(CountingStep.instanceCount.get()).isEqualTo(1);
    }
  }

  // ---------------------------------------------------------------------------
  // SagaHttpClient injection
  // ---------------------------------------------------------------------------

  @Nested
  class HttpClientInjection {

    @Test
    void resolve_namedHttpClient_injectsFromResolutionContext() {
      // Arrange — the resolution context returns a stub client for "account-svc"
      SagaHttpClient stub = mock(SagaHttpClient.class);
      ResolutionContext context = contextOf(Map.of("account-svc", stub));
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object result = resolver.resolve("debit", HttpClientStep.class.getName(), context);

      // Assert — the @Named SagaHttpClient was injected from the context
      assertThat(result).isInstanceOf(HttpClientStep.class);
      assertThat(((HttpClientStep) result).http).isSameAs(stub);
    }

    @Test
    void resolve_namedHttpClientNoEndpoint_throwsSagaDefinitionException() {
      // Arrange — no endpoint registered under the requested name
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("debit", HttpClientStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_unqualifiedHttpClientSingleEndpoint_injectsSoleClient() {
      // Arrange — exactly one endpoint registered; an unqualified parameter selects it
      SagaHttpClient stub = mock(SagaHttpClient.class);
      ResolutionContext context = contextOf(Map.of("account-svc", stub));
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object result = resolver.resolve("debit", UnqualifiedHttpClientStep.class.getName(), context);

      // Assert — the sole SagaHttpClient was injected without @Named
      assertThat(result).isInstanceOf(UnqualifiedHttpClientStep.class);
      assertThat(((UnqualifiedHttpClientStep) result).http).isSameAs(stub);
    }

    @Test
    void resolve_unqualifiedHttpClientMultipleEndpoints_throwsSagaDefinitionException() {
      // Arrange — two endpoints registered; an unqualified parameter is ambiguous
      Map<String, SagaHttpClient> endpoints = new LinkedHashMap<>();
      endpoints.put("account-svc", mock(SagaHttpClient.class));
      endpoints.put("payment-svc", mock(SagaHttpClient.class));
      ResolutionContext context = contextOf(endpoints);
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () -> resolver.resolve("debit", UnqualifiedHttpClientStep.class.getName(), context))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_unqualifiedHttpClientNoEndpoint_throwsSagaDefinitionException() {
      // Arrange — no endpoint registered; an unqualified parameter cannot be resolved
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(
              () ->
                  resolver.resolve(
                      "debit", UnqualifiedHttpClientStep.class.getName(), NO_ENDPOINTS))
          .isInstanceOf(SagaDefinitionException.class);
    }
  }

  // ===========================================================================
  // Test fixtures
  // ===========================================================================

  /** Step with no-arg constructor. */
  public static class NoArgStep implements Step {
    @Override
    public String getName() {
      return "no-arg";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** TccStep with no-arg constructor. */
  public static class NoArgTccStep implements TccStep {
    @Override
    public String getName() {
      return "no-arg-tcc";
    }

    @Override
    public StepResult reserve(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void confirm(SagaContext context) throws StepExecutionException {}

    @Override
    public void cancel(SagaContext context) throws StepCompensationException {}
  }

  /** Step with a single constructor parameter. */
  public static class SingleParamStep implements Step {
    final String value;

    public SingleParamStep(String value) {
      this.value = value;
    }

    @Override
    public String getName() {
      return "single-param";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step that takes an {@code @Named} {@link SagaHttpClient} (injected from the endpoint). */
  public static class HttpClientStep implements Step {
    final SagaHttpClient http;

    public HttpClientStep(@Named("account-svc") SagaHttpClient http) {
      this.http = http;
    }

    @Override
    public String getName() {
      return "http-client";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step with an unqualified {@link SagaHttpClient} parameter (selects the sole endpoint). */
  public static class UnqualifiedHttpClientStep implements Step {
    final SagaHttpClient http;

    public UnqualifiedHttpClientStep(SagaHttpClient http) {
      this.http = http;
    }

    @Override
    public String getName() {
      return "unqualified-http-client";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step with multiple constructor parameters. */
  public static class MultiParamStep implements Step {
    final String strValue;
    final int intValue;

    public MultiParamStep(String strValue, Integer intValue) {
      this.strValue = strValue;
      this.intValue = intValue;
    }

    @Override
    public String getName() {
      return "multi-param";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step with @Named qualifiers on constructor parameters. */
  public static class NamedParamStep implements Step {
    final String sourceChannel;
    final String targetChannel;

    public NamedParamStep(
        @Named("source") String sourceChannel, @Named("target") String targetChannel) {
      this.sourceChannel = sourceChannel;
      this.targetChannel = targetChannel;
    }

    @Override
    public String getName() {
      return "named-param";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step with multiple public constructors — should be rejected. */
  public static class MultiConstructorStep implements Step {
    public MultiConstructorStep() {}

    public MultiConstructorStep(String value) {}

    @Override
    public String getName() {
      return "multi-constructor";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Not a Step — just a plain class. */
  public static class NotAStep {
    public NotAStep() {}
  }

  /** Abstract step — cannot be instantiated. */
  public abstract static class AbstractStep implements Step {}

  /** Step with only a private constructor. */
  public static class PrivateConstructorStep implements Step {
    private PrivateConstructorStep() {}

    @Override
    public String getName() {
      return "private";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /**
   * Step that requires a Long — for testing "no suitable constructor" when Long is not registered.
   */
  public static class RequiresLongStep implements Step {
    public RequiresLongStep(Long value) {}

    @Override
    public String getName() {
      return "requires-long";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step that requires a String (no no-arg fallback). */
  public static class RequiresStringStep implements Step {
    public RequiresStringStep(String value) {}

    @Override
    public String getName() {
      return "requires-string";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step whose constructor throws an exception — intentional for testing. */
  public static class ThrowingConstructorStep implements Step {
    public ThrowingConstructorStep() {
      throw new RuntimeException("constructor failure");
    }

    @Override
    public String getName() {
      return "throwing";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step with @Named("source") — for testing qualifier mismatch. */
  public static class NamedSourceOnlyStep implements Step {
    public NamedSourceOnlyStep(@Named("source") String channel) {}

    @Override
    public String getName() {
      return "named-source-only";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }

  /** Step that tracks construction count — for concurrency test. */
  public static class CountingStep implements Step {
    static final AtomicInteger instanceCount = new AtomicInteger(0);

    public CountingStep() {
      instanceCount.incrementAndGet();
    }

    @Override
    public String getName() {
      return "counting";
    }

    @Override
    public StepResult execute(SagaContext context) throws StepExecutionException {
      return StepResult.empty();
    }

    @Override
    public void compensate(SagaContext context) throws StepCompensationException {}
  }
}
