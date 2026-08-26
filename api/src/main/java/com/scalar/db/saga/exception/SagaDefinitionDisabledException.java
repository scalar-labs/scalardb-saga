package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a saga cannot be started because its latest registered version is disabled.
 *
 * <p>Disabling retires a saga: new starts of the name are refused, sagas already running finish
 * normally, and recovery on them stays available. Retirement is a property of a version rather than
 * of a name — registered content is immutable, so retiring means registering a version that says so
 * — and a name is retired when its <b>latest</b> version is. Both start paths are refused: a start
 * by name, and a start pinned to an explicit version, including one pinned to a version that is
 * itself enabled but no longer the latest.
 *
 * <p>{@link #getVersion()} therefore reports the disabled <b>latest</b> version, which is not
 * necessarily the version the caller asked for. That is the version an operator has to look at to
 * understand the refusal, and the one a later, non-disabled version must be registered after.
 */
public class SagaDefinitionDisabledException extends SagaRuntimeException {

  private final String sagaName;
  private final String version;

  /**
   * The saga named {@code sagaName} is retired as of {@code version}, its latest.
   *
   * @param sagaName the saga whose start was refused
   * @param version the disabled latest version, not necessarily the one requested
   */
  public static SagaDefinitionDisabledException of(String sagaName, String version) {
    return new SagaDefinitionDisabledException(sagaName, version);
  }

  private SagaDefinitionDisabledException(String sagaName, String version) {
    super(
        SagaErrorCode.SAGA_DEFINITION_DISABLED,
        ErrorMetadata.of(
            "saga_name", Objects.requireNonNull(sagaName, "sagaName must not be null"),
            "version", Objects.requireNonNull(version, "version must not be null")));
    this.sagaName = sagaName;
    this.version = version;
  }

  public String getSagaName() {
    return sagaName;
  }

  /** The disabled latest version, which may differ from the version the caller requested. */
  public String getVersion() {
    return version;
  }
}
