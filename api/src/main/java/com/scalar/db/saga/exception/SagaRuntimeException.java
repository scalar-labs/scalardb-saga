package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Base type for the unchecked saga exceptions.
 *
 * <p>The saga-level exceptions ({@link SagaNotFoundException}, {@link SagaAlreadyExistsException},
 * {@link SagaPersistenceException}, {@link SagaTimeoutException}, etc.) all extend {@code
 * SagaRuntimeException}, so callers can {@code catch (SagaRuntimeException e)} to handle any saga
 * failure uniformly. It is also thrown directly for failures that have no more specific type — e.g.
 * a remote client receiving an {@code INTERNAL}/unknown gRPC status.
 *
 * <p>The api may declare exceptions that only a remote implementation throws (e.g. {@link
 * SagaUnavailableException}); the {@code SagaOrchestrator} contract spans both the embedded and
 * remote implementations. (The step-level exceptions are a separate, partly-checked hierarchy and
 * do not extend this type.)
 */
public class SagaRuntimeException extends RuntimeException {

  public SagaRuntimeException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }

  public SagaRuntimeException(String message, Throwable cause) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
