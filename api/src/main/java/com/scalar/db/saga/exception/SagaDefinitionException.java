package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/**
 * Thrown when a saga definition is invalid — from build-time validation, JSON/YAML parsing,
 * call-spec decoding, and resolver / registry lookups that surface at definition-load time.
 *
 * <p>One factory per {@link SagaErrorCode}: throw sites construct the {@code detail} string
 * themselves. Follows the K8s / AWS / Spring pattern of per-code (not per-rule) factories.
 *
 * <p>{@link #declarativeStepInvalid} carries {@link SagaErrorCode#INVALID_STEP_DEFINITION}, the
 * step-scoped sibling of {@link SagaErrorCode#INVALID_DEFINITION}: its throw sites (the parser and
 * call-spec codec paths) know which step failed but not which saga definition encloses it, so the
 * schema asks for the step name rather than a saga name those sites would have to fake.
 */
public class SagaDefinitionException extends SagaRuntimeException {

  private SagaDefinitionException(SagaErrorCode code, Map<String, String> metadata) {
    super(code, metadata);
  }

  private SagaDefinitionException(
      SagaErrorCode code, Map<String, String> metadata, Throwable cause) {
    super(code, metadata, cause);
  }

  public static SagaDefinitionException definitionInvalid(String sagaName, String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_DEFINITION,
        ErrorMetadata.of("saga_name", sagaName, "detail", detail));
  }

  public static SagaDefinitionException declarativeStepInvalid(String stepName, String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_STEP_DEFINITION,
        ErrorMetadata.of("step_name", stepName, "detail", detail));
  }

  public static SagaDefinitionException definitionMalformed(String format, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.MALFORMED_DEFINITION,
        ErrorMetadata.of(
            "source", "inline " + format, "detail", "failed to parse " + format + " definition"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  /** As {@link #definitionMalformed(String, Throwable)}, for a file or resource source. */
  public static SagaDefinitionException definitionMalformed(
      String format, String source, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.MALFORMED_DEFINITION,
        ErrorMetadata.of("source", source, "detail", "failed to parse " + format + " definition"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  public static SagaDefinitionException sourceUnreadable(String source) {
    return new SagaDefinitionException(
        SagaErrorCode.UNREADABLE_DEFINITION_SOURCE, ErrorMetadata.of("source", source));
  }

  public static SagaDefinitionException sourceUnreadable(String source, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.UNREADABLE_DEFINITION_SOURCE,
        ErrorMetadata.of("source", source),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  public static SagaDefinitionException versionContentConflict(String name, String version) {
    return new SagaDefinitionException(
        SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT,
        ErrorMetadata.of("saga_name", name, "version", version));
  }

  public static SagaDefinitionException stepClassInvalid(String stepClass, String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_STEP_CLASS,
        ErrorMetadata.of("step_class", stepClass, "detail", detail));
  }

  public static SagaDefinitionException stepClassInvalid(
      String stepClass, String detail, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_STEP_CLASS,
        ErrorMetadata.of("step_class", stepClass, "detail", detail),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  public static SagaDefinitionException stepClassNotSupportedOnServer(
      String sagaName, String stepName) {
    return new SagaDefinitionException(
        SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER,
        ErrorMetadata.of("saga_name", sagaName, "step_name", stepName));
  }

  public static SagaDefinitionException httpEndpointLookupFailed(String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED, ErrorMetadata.of("detail", detail));
  }

  /**
   * Reconstructs the exception from a wire-received {@link SagaErrorCode} and metadata, when the
   * client SDK decodes an {@code ErrorInfo} from the daemon. The code must be one this exception
   * represents (any of the definition, step, or endpoint codes routed here).
   *
   * <p>Package-private: {@link ExceptionRegistry} is the only caller, so a code this type does not
   * represent is a registry wiring bug rather than caller error, and throws {@link
   * IllegalStateException}. That is deliberately outside the {@code IllegalArgumentException |
   * NullPointerException} the registry catches for genuine wire-metadata drift, so a wiring bug
   * surfaces as itself instead of as {@code UNRECOGNIZED_SERVER_ERROR}.
   */
  static SagaDefinitionException fromWire(SagaErrorCode code, Map<String, String> metadata) {
    switch (code) {
      case INVALID_DEFINITION:
      case INVALID_STEP_DEFINITION:
      case MALFORMED_DEFINITION:
      case UNREADABLE_DEFINITION_SOURCE:
      case SAGA_DEFINITION_VERSION_CONTENT_CONFLICT:
      case INVALID_STEP_CLASS:
      case STEP_CLASS_NOT_SUPPORTED_ON_SERVER:
      case HTTP_ENDPOINT_LOOKUP_FAILED:
        return new SagaDefinitionException(code, metadata);
      default:
        throw new IllegalStateException("SagaDefinitionException does not carry code " + code);
    }
  }
}
