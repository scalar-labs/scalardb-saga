package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/**
 * Thrown when the saga store layer encounters a failure (e.g., a database write error, a
 * serialization or parse failure).
 *
 * <p>Carries one of three codes chosen by the static factory used:
 *
 * <ul>
 *   <li>{@link SagaErrorCode#PERSISTENCE_STORE_UNAVAILABLE} — a transient store failure or a
 *       retry-exhausted transaction; construct via {@link #storeUnavailable(Throwable)}.
 *   <li>{@link SagaErrorCode#PERSISTENCE_SERIALIZATION_FAILED} — a permanent JSON serialization
 *       failure; construct via {@link #serializationFailed(Throwable)}.
 *   <li>{@link SagaErrorCode#PERSISTENCE_DESERIALIZATION_FAILED} — a permanent JSON or event-stream
 *       parse failure; construct via {@link #deserializationFailed(Throwable)}.
 * </ul>
 *
 * <p>{@link #isRetryable()} derives from the code's {@link SagaErrorCode.Category}: {@code true}
 * for {@link SagaErrorCode.Category#RETRYABLE_SERVER_ERROR}, {@code false} otherwise. The daemon's
 * error mappers read the flag to pick a retryable ({@code UNAVAILABLE} / {@code 503}) versus a
 * non-retryable ({@code INTERNAL} / {@code 500}) wire code, so a permanent failure is not retried
 * futilely and is not misreported to the caller as a transient outage.
 */
public class SagaPersistenceException extends SagaRuntimeException {

  private final boolean retryable;

  private SagaPersistenceException(SagaErrorCode code, Throwable cause) {
    super(code, ErrorMetadata.of(), Objects.requireNonNull(cause, "cause must not be null"));
    this.retryable = code.category() == SagaErrorCode.Category.RETRYABLE_SERVER_ERROR;
  }

  /** Cause-free construction, for {@link #fromWire} only — see its contract. */
  private SagaPersistenceException(SagaErrorCode code) {
    super(code, ErrorMetadata.of());
    this.retryable = code.category() == SagaErrorCode.Category.RETRYABLE_SERVER_ERROR;
  }

  /**
   * Reconstructs the exception from a wire-received code, so a remote caller sees the same type,
   * code, and {@link #isRetryable()} verdict an embedded caller would. The three static factories
   * all demand a cause, but no cause crosses the wire: the server-side chain can carry internal
   * specifics, and the daemon deliberately does not transmit it. The wire layer attaches the
   * transport status as the cause instead.
   *
   * @throws IllegalArgumentException if {@code code} is not one of this type's three codes
   */
  public static SagaPersistenceException fromWire(
      SagaErrorCode code, Map<String, String> metadata) {
    Objects.requireNonNull(code, "code must not be null");
    switch (code) {
      case PERSISTENCE_STORE_UNAVAILABLE:
      case PERSISTENCE_SERIALIZATION_FAILED:
      case PERSISTENCE_DESERIALIZATION_FAILED:
        return new SagaPersistenceException(code);
      default:
        throw new IllegalArgumentException("SagaPersistenceException does not carry code " + code);
    }
  }

  /**
   * A transient store failure — a store outage, an unresolvable transaction commit, or a
   * retry-exhausted operation — that may succeed if the operation is retried.
   */
  public static SagaPersistenceException storeUnavailable(Throwable cause) {
    return new SagaPersistenceException(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE, cause);
  }

  /**
   * A permanent JSON-serialization failure — the payload cannot be encoded, so retrying always
   * fails the same way.
   */
  public static SagaPersistenceException serializationFailed(Throwable cause) {
    return new SagaPersistenceException(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED, cause);
  }

  /**
   * A permanent JSON-deserialization or event-stream parse failure — the stored data cannot be
   * decoded, so retrying always fails the same way.
   */
  public static SagaPersistenceException deserializationFailed(Throwable cause) {
    return new SagaPersistenceException(SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED, cause);
  }

  /**
   * Whether retrying the failed operation may succeed (a transient failure) rather than fail
   * identically (a permanent failure). Derived from the code's {@link SagaErrorCode.Category}.
   */
  public boolean isRetryable() {
    return retryable;
  }
}
