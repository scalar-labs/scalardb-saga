package com.scalar.db.saga.exception;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when a step's compensation ({@code compensate} or {@code cancel}) fails.
 *
 * <p>Compensation failures are never retryable inline — the saga is escalated and periodic recovery
 * retries compensation later.
 *
 * <p>Deliberately extends {@link RuntimeException} directly, not {@link SagaRuntimeException} — the
 * step-level exceptions are a separate hierarchy that user code throws (users implementing {@code
 * compensate} should not have to reason about the saga error-code taxonomy). The engine, when it
 * wraps a compensation failure with structured step info, attaches {@link
 * SagaErrorCode#COMPENSATION_FAILED} via {@link #getErrorCode()}; user-thrown instances leave
 * {@link #getErrorCode()} null and the client SDK maps such wire arrivals to the sentinel {@code
 * STEP_USER_FAILURE} code (see the error-code design).
 */
public class StepCompensationException extends RuntimeException {

  private final @Nullable String stepName;
  private final int stepIndex;
  private final @Nullable SagaErrorCode errorCode;
  private final Map<String, String> metadata;

  public StepCompensationException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
    this.stepName = null;
    this.stepIndex = -1;
    this.errorCode = null;
    this.metadata = Collections.emptyMap();
  }

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

  public @Nullable String getStepName() {
    return stepName;
  }

  public int getStepIndex() {
    return stepIndex;
  }

  /**
   * The engine-attached error code, or {@code null} for user-thrown instances. Non-null only for
   * the engine-produced (stepName, stepIndex, cause) form.
   */
  public @Nullable SagaErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * The metadata associated with the error code, in schema-declared order. Always non-null; empty
   * for user-thrown instances (which have no code).
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }
}
