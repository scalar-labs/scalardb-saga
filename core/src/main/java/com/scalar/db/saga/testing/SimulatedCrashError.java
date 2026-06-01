package com.scalar.db.saga.testing;

/**
 * Thrown by {@link CrashingStoreDecorator} to simulate a process crash during saga execution.
 *
 * <p>Extends {@link Error} (not {@link RuntimeException}) so that the engine's {@code
 * catch(RuntimeException)} blocks in {@code recordStepCompleted} do not intercept the crash — the
 * error propagates all the way up to the test, simulating a true process death.
 */
public final class SimulatedCrashError extends Error {

  public SimulatedCrashError(String message) {
    super(message);
  }
}
