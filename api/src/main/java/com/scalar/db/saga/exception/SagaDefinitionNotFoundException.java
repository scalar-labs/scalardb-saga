package com.scalar.db.saga.exception;

import com.scalar.db.saga.api.SagaDefinitionId;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when a saga definition cannot be found by name or by name and version.
 *
 * <p>Carries one of two codes: {@link SagaErrorCode#SAGA_DEFINITION_NOT_FOUND} (name only) or
 * {@link SagaErrorCode#SAGA_DEFINITION_VERSION_NOT_FOUND} (name + version). {@link #getErrorCode()}
 * reflects which was constructed.
 */
public class SagaDefinitionNotFoundException extends SagaRuntimeException {

  private final String sagaName;
  private final @Nullable String version;

  public SagaDefinitionNotFoundException(String sagaName) {
    super(
        SagaErrorCode.SAGA_DEFINITION_NOT_FOUND,
        ErrorMetadata.of(
            "saga_name", Objects.requireNonNull(sagaName, "sagaName must not be null")));
    this.sagaName = sagaName;
    this.version = null;
  }

  public SagaDefinitionNotFoundException(String sagaName, String version) {
    super(
        SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND,
        ErrorMetadata.of(
            "saga_name", Objects.requireNonNull(sagaName, "sagaName must not be null"),
            "version", Objects.requireNonNull(version, "version must not be null")));
    this.sagaName = sagaName;
    this.version = version;
  }

  public SagaDefinitionNotFoundException(SagaDefinitionId id) {
    this(id.name(), id.version());
  }

  public String getSagaName() {
    return sagaName;
  }

  public @Nullable String getVersion() {
    return version;
  }
}
