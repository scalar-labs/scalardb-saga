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
 * <p>{@link #declarativeStepInvalid} is a formatting shortcut over {@link #definitionInvalid} that
 * prefixes the step context; it's kept as a public factory because it's reused across the parser
 * and call-spec codec paths.
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

  /**
   * Convenience over {@link #definitionInvalid} that prefixes the declarative step context so every
   * declarative-step failure reads {@code "declarative service step '<name>': <detail>"}.
   */
  public static SagaDefinitionException declarativeStepInvalid(String stepName, String detail) {
    return definitionInvalid("", "declarative service step '" + stepName + "': " + detail);
  }

  public static SagaDefinitionException definitionMalformed(String format, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.MALFORMED_DEFINITION,
        ErrorMetadata.of(
            "source", "inline " + format, "detail", "failed to parse " + format + " definition"),
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
        SagaErrorCode.DEFINITION_VERSION_CONTENT_CONFLICT,
        ErrorMetadata.of("name", name, "version", version));
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

  public static SagaDefinitionException stepClassNotSupportedOnDaemon(
      String sagaName, String stepName) {
    return new SagaDefinitionException(
        SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_DAEMON,
        ErrorMetadata.of("saga_name", sagaName, "step_name", stepName));
  }

  public static SagaDefinitionException httpEndpointLookupFailed(String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED, ErrorMetadata.of("detail", detail));
  }
}
