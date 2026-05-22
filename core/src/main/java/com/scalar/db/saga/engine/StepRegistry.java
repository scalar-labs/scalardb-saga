package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.TccStep;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;

/**
 * Thread-safe registry mapping step names to {@link Step} or {@link TccStep} instances.
 *
 * <p>Steps are registered at engine startup and looked up during execution.
 */
@ThreadSafe
class StepRegistry {

  private final ConcurrentHashMap<String, Object> steps = new ConcurrentHashMap<>();

  /**
   * Registers a saga step by name.
   *
   * @throws IllegalArgumentException if a step with the same name is already registered
   */
  void register(String name, Step step) {
    Object existing = steps.putIfAbsent(name, step);
    if (existing != null) {
      throw new IllegalArgumentException("Step already registered: '" + name + "'");
    }
  }

  /**
   * Registers a TCC step by name.
   *
   * @throws IllegalArgumentException if a step with the same name is already registered
   */
  void register(String name, TccStep step) {
    Object existing = steps.putIfAbsent(name, step);
    if (existing != null) {
      throw new IllegalArgumentException("Step already registered: '" + name + "'");
    }
  }

  /**
   * Retrieves a saga step by name.
   *
   * @throws IllegalArgumentException if no step is registered with the given name
   * @throws IllegalStateException if the registered step is not a {@link Step}
   */
  Step getStep(String name) {
    Object step = getRegistered(name);
    if (step instanceof Step s) {
      return s;
    }
    throw new IllegalStateException(
        "Expected Step but found " + step.getClass().getName() + " for name: '" + name + "'");
  }

  /**
   * Retrieves a TCC step by name.
   *
   * @throws IllegalArgumentException if no step is registered with the given name
   * @throws IllegalStateException if the registered step is not a {@link TccStep}
   */
  TccStep getTccStep(String name) {
    Object step = getRegistered(name);
    if (step instanceof TccStep t) {
      return t;
    }
    throw new IllegalStateException(
        "Expected TccStep but found " + step.getClass().getName() + " for name: '" + name + "'");
  }

  private Object getRegistered(String name) {
    Object step = steps.get(name);
    if (step == null) {
      throw new IllegalArgumentException("No step registered with name: '" + name + "'");
    }
    return step;
  }
}
