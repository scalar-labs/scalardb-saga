/**
 * Test doubles for applications that embed the engine and test their sagas against it.
 *
 * <p>{@link FakeStep} and {@link FakeTccStep} are configurable {@code Step} and {@code TccStep}
 * implementations: each is built with a name, told what every phase returns or throws, and records
 * the saga IDs that invoked each phase, so a test can assert what the engine executed, confirmed or
 * compensated. {@link ForwardingSagaStore} is a {@code SagaStore} decorator base for altering one
 * store behaviour at a time, and {@link CrashingStoreDecorator} is the one this package ships: it
 * persists a configured step's completion and then throws {@link SimulatedCrashError}, simulating a
 * process that dies between an event write and the engine's next move, so recovery can be
 * exercised.
 */
@NullMarked
package com.scalar.db.saga.testing;

import org.jspecify.annotations.NullMarked;
