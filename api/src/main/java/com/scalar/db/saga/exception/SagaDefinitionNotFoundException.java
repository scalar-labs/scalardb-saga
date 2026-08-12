package com.scalar.db.saga.exception;

import com.scalar.db.saga.api.SagaDefinitionId;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when a saga definition cannot be found by name or by name and version.
 *
 * <p>Carries one of two codes: {@link SagaErrorCode#SAGA_DEFINITION_NOT_FOUND} (name only) or
 * {@link SagaErrorCode#SAGA_DEFINITION_VERSION_NOT_FOUND} (name + version). Construction goes
 * through the named factories so the throw site says which code it means, as the other multi-code
 * exceptions do; {@link #getErrorCode()} reflects the choice.
 */
public class SagaDefinitionNotFoundException extends SagaRuntimeException {

  private final String sagaName;
  private final @Nullable String version;

  /** No definition is registered under {@code sagaName} at all. */
  public static SagaDefinitionNotFoundException byName(String sagaName) {
    return new SagaDefinitionNotFoundException(sagaName);
  }

  /** The definition exists, but not at the requested {@code version}. */
  public static SagaDefinitionNotFoundException byNameAndVersion(String sagaName, String version) {
    return new SagaDefinitionNotFoundException(sagaName, version);
  }

  /** As {@link #byNameAndVersion(String, String)}, from a {@link SagaDefinitionId}. */
  public static SagaDefinitionNotFoundException byId(SagaDefinitionId id) {
    return new SagaDefinitionNotFoundException(id.name(), id.version());
  }

  private SagaDefinitionNotFoundException(String sagaName) {
    super(
        SagaErrorCode.SAGA_DEFINITION_NOT_FOUND,
        ErrorMetadata.of(
            "saga_name", Objects.requireNonNull(sagaName, "sagaName must not be null")));
    this.sagaName = sagaName;
    this.version = null;
  }

  private SagaDefinitionNotFoundException(String sagaName, String version) {
    super(
        SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND,
        ErrorMetadata.of(
            "saga_name", Objects.requireNonNull(sagaName, "sagaName must not be null"),
            "version", Objects.requireNonNull(version, "version must not be null")));
    this.sagaName = sagaName;
    this.version = version;
  }

  public String getSagaName() {
    return sagaName;
  }

  public @Nullable String getVersion() {
    return version;
  }
}
