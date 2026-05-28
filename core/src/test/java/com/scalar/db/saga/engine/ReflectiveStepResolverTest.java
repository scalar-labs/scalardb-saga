package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.Named;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ReflectiveStepResolverTest {

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
      Object result = resolver.resolve("step1", NoArgStep.class.getName());

      // Assert
      assertThat(result).isInstanceOf(NoArgStep.class);
    }

    @Test
    void resolve_noArgTccStep_returnsTccStepInstance() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object result = resolver.resolve("step1", NoArgTccStep.class.getName());

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
      Object result = resolver.resolve("step1", SingleParamStep.class.getName());

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
      Object result = resolver.resolve("step1", MultiParamStep.class.getName());

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
      Object result = resolver.resolve("step1", NamedParamStep.class.getName());

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
      assertThatThrownBy(() -> resolver.resolve("step1", MultiConstructorStep.class.getName()))
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
      Object first = resolver.resolve("step1", NoArgStep.class.getName());
      Object second = resolver.resolve("step1", NoArgStep.class.getName());

      // Assert
      assertThat(first).isSameAs(second);
    }

    @Test
    void resolve_differentStepNamesSameClass_returnsSameInstance() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act
      Object first = resolver.resolve("step-a", NoArgStep.class.getName());
      Object second = resolver.resolve("step-b", NoArgStep.class.getName());

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
      assertThatThrownBy(() -> resolver.resolve("step1", "com.nonexistent.FakeStep"))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_classNotStepOrTccStep_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", NotAStep.class.getName()))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_abstractClass_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", AbstractStep.class.getName()))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_interfaceClass_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", Step.class.getName()))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_noPublicConstructor_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", PrivateConstructorStep.class.getName()))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_noSuitableConstructor_throwsSagaDefinitionException() {
      // Arrange — step requires Long, but only String is registered
      ResourceRegistry registry = ResourceRegistry.newBuilder().add(String.class, "unused").build();
      ReflectiveStepResolver resolver = new ReflectiveStepResolver(registry);

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", RequiresLongStep.class.getName()))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_missingResource_throwsSagaDefinitionException() {
      // Arrange — step requires String, but registry is empty
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", RequiresStringStep.class.getName()))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_constructorThrows_throwsSagaDefinitionException() {
      // Arrange
      ReflectiveStepResolver resolver =
          new ReflectiveStepResolver(ResourceRegistry.newBuilder().build());

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", ThrowingConstructorStep.class.getName()))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void resolve_namedQualifierMismatch_throwsSagaDefinitionException() {
      // Arrange — step expects @Named("source") but only "other" is registered
      ResourceRegistry registry =
          ResourceRegistry.newBuilder().add(String.class, "value", "other").build();
      ReflectiveStepResolver resolver = new ReflectiveStepResolver(registry);

      // Act & Assert
      assertThatThrownBy(() -> resolver.resolve("step1", NamedSourceOnlyStep.class.getName()))
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
                    Object result = resolver.resolve("step1", CountingStep.class.getName());
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
