package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a saga-level request deadline expires client-side (the wait for a saga to reach a
 * terminal state, or a gRPC {@code DEADLINE_EXCEEDED} from the daemon).
 *
 * <p>This is an unchecked exception in a separate hierarchy from {@link StepTimeoutException}
 * because saga-level and step-level timeouts are semantically different.
 */
public class SagaTimeoutException extends SagaRuntimeException {

  public SagaTimeoutException() {
    super(SagaErrorCode.REQUEST_TIMEOUT, ErrorMetadata.of());
  }

  public SagaTimeoutException(Throwable cause) {
    super(
        SagaErrorCode.REQUEST_TIMEOUT,
        ErrorMetadata.of(),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
