package com.scalar.db.saga.exception;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when a step's compensation ({@code compensate} or {@code cancel}) fails.
 *
 * <p>A compensation that throws is retried in-process up to the step's compensation retry policy;
 * once those attempts are spent, the saga stays {@code COMPENSATING} and periodic recovery retries
 * the compensation, escalating it only if it stays stuck past the grace period.
 *
 * <p>Deliberately extends {@link RuntimeException} directly, not {@link SagaRuntimeException} — the
 * step-level exceptions are a separate hierarchy that user code throws (users implementing {@code
 * compensate} should not have to reason about the saga error-code taxonomy). The engine, when it
 * wraps a compensation failure with structured step info, attaches {@link
 * SagaErrorCode#COMPENSATION_FAILED} via {@link #getErrorCode()}; user-thrown instances leave
 * {@link #getErrorCode()} null. A user-thrown failure reaches a remote caller only through saga
 * state, as a step-event payload carrying the exception type, message, and commit flag — no error
 * code travels with it today; a coded form is deferred work (see {@code SagaErrorCode}'s reserved
 * step codes).
 */
public class StepCompensationException extends RuntimeException {

  /** The name of the step whose compensation failed; null for a user-thrown instance. */
  private final @Nullable String stepName;

  /** The step's zero-based index in the definition; -1 for a user-thrown instance. */
  private final int stepIndex;

  /** The engine-attached error code; null for a user-thrown instance. */
  private final @Nullable SagaErrorCode errorCode;

  /** The error code's metadata; empty for a user-thrown instance. */
  private final Map<String, String> metadata;

  /**
   * User-thrown form: a compensation that failed for a reason the implementation can state.
   *
   * @param message what went wrong
   */
  public StepCompensationException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
    this.stepName = null;
    this.stepIndex = -1;
    this.errorCode = null;
    this.metadata = Collections.emptyMap();
  }

  /**
   * User-thrown form: a compensation that failed because of {@code cause}.
   *
   * @param cause the failure that prevented the compensation
   */
  public StepCompensationException(Throwable cause) {
    super(Objects.requireNonNull(cause, "cause must not be null"));
    this.stepName = null;
    this.stepIndex = -1;
    this.errorCode = null;
    this.metadata = Collections.emptyMap();
  }

  /**
   * Engine-produced form: wraps a step's compensation failure with structured step info and
   * attaches {@link SagaErrorCode#COMPENSATION_FAILED}. The message is derived from the code so
   * logs, docs, and wire reconstructions read identically.
   *
   * @param stepName the name of the step whose compensation failed
   * @param stepIndex the step's zero-based index in the definition
   * @param cause the failure the compensation threw
   * @throws IllegalArgumentException if {@code stepIndex} is negative
   */
  public StepCompensationException(String stepName, int stepIndex, Throwable cause) {
    super(
        SagaErrorCode.COMPENSATION_FAILED.buildMessage(
            buildMetadata(
                Objects.requireNonNull(stepName, "stepName must not be null"),
                validateStepIndex(stepIndex))),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.stepName = stepName;
    this.stepIndex = stepIndex;
    this.errorCode = SagaErrorCode.COMPENSATION_FAILED;
    // Defensive copy in the ctor so SpotBugs's EI_EXPOSE_REP is satisfied seeing the copy in the
    // ctor's bytecode; also lets the getter return the field directly.
    this.metadata =
        Collections.unmodifiableMap(new LinkedHashMap<>(buildMetadata(stepName, stepIndex)));
  }

  private static Map<String, String> buildMetadata(String stepName, int stepIndex) {
    return ErrorMetadata.of("step_name", stepName, "step_index", String.valueOf(stepIndex));
  }

  private static int validateStepIndex(int stepIndex) {
    if (stepIndex < 0) {
      throw new IllegalArgumentException("stepIndex must not be negative: " + stepIndex);
    }
    return stepIndex;
  }

  /**
   * The name of the step whose compensation failed.
   *
   * @return the step name, or {@code null} for a user-thrown instance
   */
  public @Nullable String getStepName() {
    return stepName;
  }

  /**
   * The zero-based index, within the definition, of the step whose compensation failed.
   *
   * @return the step index, or {@code -1} for a user-thrown instance
   */
  public int getStepIndex() {
    return stepIndex;
  }

  /**
   * The engine-attached error code, or {@code null} for user-thrown instances. Non-null only for
   * the engine-produced (stepName, stepIndex, cause) form.
   *
   * @return {@link SagaErrorCode#COMPENSATION_FAILED}, or {@code null} for a user-thrown instance
   */
  public @Nullable SagaErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * The metadata associated with the error code, in schema-declared order. Always non-null; empty
   * for user-thrown instances (which have no code).
   *
   * @return the metadata map, never {@code null}
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }
}
