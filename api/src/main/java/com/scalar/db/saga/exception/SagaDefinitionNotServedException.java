package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a saga cannot be started because this daemon does not serve it, even though it is
 * registered in the store.
 *
 * <p>A daemon serves the sagas its own definition files describe. The store is append-only, so a
 * definition stays registered long after its file is gone — which is what makes retirement
 * possible: removing the file stops new starts while sagas already running finish normally, and
 * recovery on them stays available.
 *
 * <p>Two situations produce this, and a single replica cannot tell them apart: the saga was retired
 * by removing its definition file, or it was just added and this replica's configuration has not
 * caught up with a fleet-mate that registered it first. The message names both, because acting on
 * the wrong one is the mistake worth preventing. It is a stable precondition failure rather than a
 * retryable conflict: the first situation never resolves by retrying, and treating it as though it
 * might would have clients retry a retired saga indefinitely.
 *
 * <p>Distinct from {@link SagaDefinitionNotFoundException}: that means no such saga was ever
 * registered, which is a different problem with a different fix.
 */
public class SagaDefinitionNotServedException extends SagaRuntimeException {

  private final String sagaName;

  /**
   * The saga named {@code sagaName} is registered but not served here.
   *
   * @param sagaName the saga whose start was refused
   */
  public static SagaDefinitionNotServedException of(String sagaName) {
    return new SagaDefinitionNotServedException(sagaName);
  }

  private SagaDefinitionNotServedException(String sagaName) {
    super(
        SagaErrorCode.SAGA_DEFINITION_NOT_SERVED,
        ErrorMetadata.of(
            "saga_name", Objects.requireNonNull(sagaName, "sagaName must not be null")));
    this.sagaName = sagaName;
  }

  public String getSagaName() {
    return sagaName;
  }
}
