package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/**
 * Thrown when an async-step callback arrives before the saga has recorded that it is waiting on
 * that step. Always carries {@link SagaErrorCode#SAGA_STEP_NOT_PARKED}.
 *
 * <p>The engine parks a saga only after the participant has accepted the call, so a participant
 * that calls back at once can arrive first. Nothing was recorded and the callback was not applied,
 * so retrying it is safe: once the park is recorded, the same callback completes the step. This is
 * distinct from a callback for a step that already completed, which is answered as a duplicate
 * rather than refused.
 */
public class SagaStepNotParkedException extends SagaRuntimeException {

  /** The saga the callback named. */
  private final String sagaId;

  /** The step the callback tried to complete. */
  private final String stepName;

  /**
   * A callback for {@code stepName} arrived before saga {@code sagaId} recorded its park. Carries
   * {@link SagaErrorCode#SAGA_STEP_NOT_PARKED} with both names in its metadata.
   *
   * @param sagaId the saga the callback named
   * @param stepName the step the callback tried to complete
   */
  public SagaStepNotParkedException(String sagaId, String stepName) {
    super(
        SagaErrorCode.SAGA_STEP_NOT_PARKED,
        ErrorMetadata.of(
            "saga_id",
            Objects.requireNonNull(sagaId, "sagaId must not be null"),
            "step_name",
            Objects.requireNonNull(stepName, "stepName must not be null")));
    this.sagaId = sagaId;
    this.stepName = stepName;
  }

  /** Reconstructs the exception from a wire-received metadata map. */
  static SagaStepNotParkedException fromWire(Map<String, String> metadata) {
    return new SagaStepNotParkedException(
        Objects.requireNonNull(metadata.get("saga_id"), "sagaId must not be null"),
        Objects.requireNonNull(metadata.get("step_name"), "stepName must not be null"));
  }

  /**
   * Returns the saga the callback named.
   *
   * @return the saga ID
   */
  public String getSagaId() {
    return sagaId;
  }

  /**
   * Returns the step the callback tried to complete.
   *
   * @return the step name
   */
  public String getStepName() {
    return stepName;
  }
}
