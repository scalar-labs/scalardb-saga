package com.scalar.db.saga.exception;

import com.scalar.db.saga.api.SagaDefinitionId;
import java.util.Map;
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

  /** The saga name that was looked up. */
  private final String sagaName;

  /** The version that was looked up; null when the lookup was by name alone. */
  private final @Nullable String version;

  /**
   * No definition is registered under {@code sagaName} at all.
   *
   * @param sagaName the name no definition is registered under
   * @return the exception to throw, carrying {@link SagaErrorCode#SAGA_DEFINITION_NOT_FOUND}
   */
  public static SagaDefinitionNotFoundException byName(String sagaName) {
    return new SagaDefinitionNotFoundException(sagaName);
  }

  /**
   * The definition exists, but not at the requested {@code version}.
   *
   * @param sagaName the name the definition is registered under
   * @param version the version that is not registered
   * @return the exception to throw, carrying {@link
   *     SagaErrorCode#SAGA_DEFINITION_VERSION_NOT_FOUND}
   */
  public static SagaDefinitionNotFoundException byNameAndVersion(String sagaName, String version) {
    return new SagaDefinitionNotFoundException(sagaName, version);
  }

  /**
   * As {@link #byNameAndVersion(String, String)}, from a {@link SagaDefinitionId}.
   *
   * @param id the name and version that were looked up
   * @return the exception to throw, carrying {@link
   *     SagaErrorCode#SAGA_DEFINITION_VERSION_NOT_FOUND}
   */
  public static SagaDefinitionNotFoundException byId(SagaDefinitionId id) {
    return new SagaDefinitionNotFoundException(id.name(), id.version());
  }

  /**
   * Reconstructs the exception from a wire-received metadata map, under the code the wire named.
   *
   * <p>Package-private: {@link ExceptionRegistry} is the only caller, so a code this type does not
   * represent is a registry wiring bug rather than caller error, and throws {@link
   * IllegalStateException}. That is deliberately outside the {@code IllegalArgumentException |
   * NullPointerException} the registry catches for genuine wire-metadata drift, so a wiring bug
   * surfaces as itself instead of as {@code UNRECOGNIZED_SERVER_ERROR}.
   */
  static SagaDefinitionNotFoundException fromWire(
      SagaErrorCode code, Map<String, String> metadata) {
    switch (code) {
      case SAGA_DEFINITION_NOT_FOUND:
        return byName(
            Objects.requireNonNull(metadata.get("saga_name"), "sagaName must not be null"));
      case SAGA_DEFINITION_VERSION_NOT_FOUND:
        return byNameAndVersion(
            Objects.requireNonNull(metadata.get("saga_name"), "sagaName must not be null"),
            Objects.requireNonNull(metadata.get("version"), "version must not be null"));
      default:
        throw new IllegalStateException(
            "SagaDefinitionNotFoundException does not carry code " + code);
    }
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

  /**
   * The saga name that was looked up.
   *
   * @return the name, never {@code null}
   */
  public String getSagaName() {
    return sagaName;
  }

  /**
   * The version that was looked up.
   *
   * @return the version, or {@code null} when the lookup was by name alone
   */
  public @Nullable String getVersion() {
    return version;
  }
}
