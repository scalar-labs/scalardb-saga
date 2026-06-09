package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.Named;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResolver;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;

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
 *   <li>For each parameter: match by exact type, then narrow by {@link Named} qualifier if present
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
  public Object resolve(String stepName, String stepClass) {
    return cache.computeIfAbsent(stepClass, this::createInstance);
  }

  private Object createInstance(String stepClass) {
    Class<?> clazz = loadClass(stepClass);
    validateStepClass(clazz, stepClass);
    Constructor<?> constructor = selectConstructor(clazz, stepClass);
    Object[] args = resolveArguments(constructor, stepClass);
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

  private Object[] resolveArguments(Constructor<?> ctor, String stepClass) {
    Parameter[] params = ctor.getParameters();
    Object[] args = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      Named named = params[i].getAnnotation(Named.class);
      String qualifier = named != null ? named.value() : null;
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
