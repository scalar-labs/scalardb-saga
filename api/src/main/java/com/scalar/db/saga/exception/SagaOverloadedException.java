package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a start is refused because the engine is already running as many sagas as its
 * admission cap allows. Always carries {@link SagaErrorCode#ENGINE_OVERLOADED}.
 *
 * <p><b>Nothing was persisted.</b> The refusal happens after the request is validated but before
 * the saga is created, so the saga does not exist, its ID is still free, and retrying the same
 * request — including with the same caller-supplied ID — is safe and will not collide. That is what
 * makes the advice to retry honest rather than hopeful.
 *
 * <p>Distinct from {@link SagaErrorCode#RATE_LIMIT_EXCEEDED}, and the difference is worth keeping
 * straight when deciding how to back off: a rate limit is about how often <i>this caller</i> may
 * ask, and is answered per principal; this is about how much work the server is doing for
 * <i>everyone</i>. A caller well inside its request rate can still meet this, and no amount of
 * slowing down on its own part guarantees admission if another tenant is occupying the budget.
 *
 * <p>Retry with bounded attempts, exponential backoff and jitter. Unbounded immediate retries feed
 * the overload they are reacting to.
 */
public class SagaOverloadedException extends SagaRuntimeException {

  public SagaOverloadedException() {
    super(SagaErrorCode.ENGINE_OVERLOADED, ErrorMetadata.of());
  }

  public SagaOverloadedException(Throwable cause) {
    super(
        SagaErrorCode.ENGINE_OVERLOADED,
        ErrorMetadata.of(),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
