package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.Named;
import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResolver;
import com.scalar.db.saga.api.StepResolver.ResolutionContext;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Resolves step instances via reflection-based constructor injection against a {@link
 * ResourceRegistry}.
 *
 * <p><b>Resolution algorithm:</b>
 *
 * <ol>
 *   <li>Load the class via {@link Class#forName(String)}
 *   <li>Verify the class is concrete and implements {@link Step} or {@link TccStep}
 *   <li>Verify the class has exactly one public constructor (error if multiple)
 *   <li>For each parameter: a {@link SagaHttpClient} is injected from the {@link
 *       ResolutionContext}'s HTTP endpoints — by its {@link Named} value, or, when unqualified, the
 *       sole registered endpoint; every other parameter is matched against the {@link
 *       ResourceRegistry} by exact type, narrowed by {@link Named} if present
 *   <li>Instantiate via reflection and cache the singleton instance
 * </ol>
 *
 * <p>Resolved instances are cached by fully-qualified class name (FQCN). A single instance is
 * shared across all saga definitions and executions.
 */
@ThreadSafe
class ReflectiveStepResolver implements StepResolver {

  private final ResourceRegistry resourceRegistry;
  private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

  ReflectiveStepResolver(ResourceRegistry resourceRegistry) {
    this.resourceRegistry = resourceRegistry;
  }

  @Override
  public Object resolve(String stepName, String stepClass, ResolutionContext context) {
    return cache.computeIfAbsent(stepClass, key -> createInstance(stepName, key, context));
  }

  private Object createInstance(String stepName, String stepClass, ResolutionContext context) {
    Class<?> clazz = loadClass(stepClass);
    validateStepClass(clazz, stepClass);
    Constructor<?> constructor = selectConstructor(clazz, stepClass);
    Object[] args = resolveArguments(constructor, stepName, stepClass, context);
    return instantiate(constructor, args, stepClass);
  }

  private static Class<?> loadClass(String stepClass) {
    try {
      return Class.forName(stepClass);
    } catch (ClassNotFoundException e) {
      throw new SagaDefinitionException("Step class not found on classpath: " + stepClass, e);
    }
  }

  private static void validateStepClass(Class<?> clazz, String stepClass) {
    if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
      throw new SagaDefinitionException(
          "Step class must be a concrete class, but "
              + stepClass
              + " is "
              + (clazz.isInterface() ? "an interface" : "abstract"));
    }
    if (!Step.class.isAssignableFrom(clazz) && !TccStep.class.isAssignableFrom(clazz)) {
      throw new SagaDefinitionException(
          "Step class "
              + stepClass
              + " must implement "
              + Step.class.getName()
              + " or "
              + TccStep.class.getName());
    }
  }

  /**
   * Returns the single public constructor of the step class. Throws if zero or multiple public
   * constructors exist.
   */
  private static Constructor<?> selectConstructor(Class<?> clazz, String stepClass) {
    Constructor<?>[] publicConstructors = clazz.getConstructors();
    if (publicConstructors.length == 0) {
      throw new SagaDefinitionException("Step class " + stepClass + " has no public constructors");
    }
    if (publicConstructors.length > 1) {
      throw new SagaDefinitionException(
          "Step class "
              + stepClass
              + " has "
              + publicConstructors.length
              + " public constructors, but exactly one is required");
    }
    return publicConstructors[0];
  }

  private Object[] resolveArguments(
      Constructor<?> ctor, String stepName, String stepClass, ResolutionContext context) {
    Parameter[] params = ctor.getParameters();
    Object[] args = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      Named named = params[i].getAnnotation(Named.class);
      String qualifier = named != null ? named.value() : null;
      if (params[i].getType() == SagaHttpClient.class) {
        args[i] = resolveHttpClient(qualifier, stepName, stepClass, i, context);
        continue;
      }
      try {
        args[i] = resourceRegistry.get(params[i].getType(), qualifier);
      } catch (IllegalArgumentException | IllegalStateException e) {
        throw new SagaDefinitionException(
            "Cannot resolve parameter "
                + i
                + " ("
                + params[i].getType().getName()
                + (qualifier != null ? " @Named(\"" + qualifier + "\")" : "")
                + ") of constructor for step class "
                + stepClass,
            e);
      }
    }
    return args;
  }

  /**
   * Injects a {@link SagaHttpClient} parameter from the {@link ResolutionContext}. When the
   * parameter is qualified with {@link Named}, the endpoint registered under that value is used;
   * when unqualified, the sole registered endpoint is used (and an ambiguity or absence is
   * reported). Any {@link SagaDefinitionException} from the lookup is wrapped with step and
   * parameter context.
   */
  private static SagaHttpClient resolveHttpClient(
      @Nullable String qualifier,
      String stepName,
      String stepClass,
      int index,
      ResolutionContext context) {
    try {
      return qualifier == null ? context.httpClient() : context.httpClient(qualifier);
    } catch (SagaDefinitionException e) {
      throw new SagaDefinitionException(
          "Cannot resolve "
              + SagaHttpClient.class.getSimpleName()
              + " parameter "
              + index
              + " of step '"
              + stepName
              + "' (class "
              + stepClass
              + "): "
              + e.getMessage(),
          e);
    }
  }

  private static Object instantiate(Constructor<?> ctor, Object[] args, String stepClass) {
    try {
      return ctor.newInstance(args);
    } catch (InvocationTargetException e) {
      throw new SagaDefinitionException(
          "Constructor of step class " + stepClass + " threw an exception",
          e.getCause() != null ? e.getCause() : e);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new SagaDefinitionException("Failed to instantiate step class " + stepClass, e);
    }
  }
}
