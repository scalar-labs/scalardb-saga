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

  /**
   * The definition of {@code sagaName} violates a validation rule — a duplicate step name, a bad
   * pivot placement, a malformed field value. Carries {@link SagaErrorCode#INVALID_DEFINITION}.
   *
   * @param sagaName the name of the definition that failed validation
   * @param detail which rule failed, and where
   * @return the exception to throw
   */
  public static SagaDefinitionException definitionInvalid(String sagaName, String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_DEFINITION,
        ErrorMetadata.of("saga_name", sagaName, "detail", detail));
  }

  /**
   * A declarative service step violates a validation rule — a missing or malformed call field, a
   * bad phase combination. Carries {@link SagaErrorCode#INVALID_STEP_DEFINITION}; the throw sites
   * know the step but not the saga definition enclosing it, so the step name is what travels.
   *
   * @param stepName the name of the step that failed validation
   * @param detail which rule failed, and where
   * @return the exception to throw
   */
  public static SagaDefinitionException declarativeStepInvalid(String stepName, String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_STEP_DEFINITION,
        ErrorMetadata.of("step_name", stepName, "detail", detail));
  }

  /**
   * The {@code detail} stays generic and the parser's diagnostic rides only the cause,
   * deliberately: every thrower today is operator-local (inline strings, server-boot file loading),
   * where the cause chain is visible, and echoing parser internals onto the wire is a leakage
   * decision to make only if a remote registration route ever ships.
   *
   * @param format the definition format being parsed, named as it should read in the message (for
   *     example {@code JSON})
   * @param cause the parser's diagnostic
   * @return the exception to throw, carrying {@link SagaErrorCode#MALFORMED_DEFINITION}
   */
  public static SagaDefinitionException definitionMalformed(String format, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.MALFORMED_DEFINITION,
        ErrorMetadata.of(
            "source", "inline " + format, "detail", "failed to parse " + format + " definition"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  /**
   * As {@link #definitionMalformed(String, Throwable)}, for a file or resource source.
   *
   * @param format the definition format being parsed, named as it should read in the message (for
   *     example {@code JSON})
   * @param source the file path or resource name the definition was read from
   * @param cause the parser's diagnostic
   * @return the exception to throw, carrying {@link SagaErrorCode#MALFORMED_DEFINITION}
   */
  public static SagaDefinitionException definitionMalformed(
      String format, String source, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.MALFORMED_DEFINITION,
        ErrorMetadata.of("source", source, "detail", "failed to parse " + format + " definition"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  /**
   * The file, classpath resource, or extension at {@code source} could not be resolved to a
   * readable definition. Carries {@link SagaErrorCode#UNREADABLE_DEFINITION_SOURCE}.
   *
   * @param source the path or resource that could not be read
   * @return the exception to throw
   */
  public static SagaDefinitionException sourceUnreadable(String source) {
    return new SagaDefinitionException(
        SagaErrorCode.UNREADABLE_DEFINITION_SOURCE, ErrorMetadata.of("source", source));
  }

  /**
   * As {@link #sourceUnreadable(String)}, with the failure that prevented the read as the cause.
   *
   * @param source the path or resource that could not be read
   * @param cause the failure that prevented the read
   * @return the exception to throw
   */
  public static SagaDefinitionException sourceUnreadable(String source, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.UNREADABLE_DEFINITION_SOURCE,
        ErrorMetadata.of("source", source),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  /**
   * A definition is already registered under {@code name} and {@code version} with different
   * content. Carries {@link SagaErrorCode#SAGA_DEFINITION_VERSION_CONTENT_CONFLICT}; the fix is to
   * bump the version rather than re-register under the same one.
   *
   * @param name the saga name of the definition
   * @param version the version already taken by different content
   * @return the exception to throw
   */
  public static SagaDefinitionException versionContentConflict(String name, String version) {
    return new SagaDefinitionException(
        SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT,
        ErrorMetadata.of("saga_name", name, "version", version));
  }

  /**
   * A class-based step could not be resolved or instantiated: the class was not found, is not a
   * {@code Step} or {@code TccStep}, has the wrong constructor shape, or its constructor threw.
   * Carries {@link SagaErrorCode#INVALID_STEP_CLASS}.
   *
   * @param stepClass the fully qualified name of the step class
   * @param detail which part of the resolution failed
   * @return the exception to throw
   */
  public static SagaDefinitionException stepClassInvalid(String stepClass, String detail) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_STEP_CLASS,
        ErrorMetadata.of("step_class", stepClass, "detail", detail));
  }

  /**
   * As {@link #stepClassInvalid(String, String)}, with the reflective failure as the cause.
   *
   * @param stepClass the fully qualified name of the step class
   * @param detail which part of the resolution failed
   * @param cause the reflective failure
   * @return the exception to throw
   */
  public static SagaDefinitionException stepClassInvalid(
      String stepClass, String detail, Throwable cause) {
    return new SagaDefinitionException(
        SagaErrorCode.INVALID_STEP_CLASS,
        ErrorMetadata.of("step_class", stepClass, "detail", detail),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  /**
   * The definition names a class-based step, which a saga server cannot run: the server executes
   * declarative definitions only. Carries {@link SagaErrorCode#STEP_CLASS_NOT_SUPPORTED_ON_SERVER}.
   *
   * @param sagaName the saga whose definition names the step
   * @param stepName the class-based step
   * @return the exception to throw
   */
  public static SagaDefinitionException stepClassNotSupportedOnServer(
      String sagaName, String stepName) {
    return new SagaDefinitionException(
        SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER,
        ErrorMetadata.of("saga_name", sagaName, "step_name", stepName));
  }

  /**
   * A step needed an HTTP endpoint, but the orchestrator has no matching registration: none
   * registered, the name not found, or several registered without a qualifier to choose by. Carries
   * {@link SagaErrorCode#HTTP_ENDPOINT_LOOKUP_FAILED}.
   *
   * @param detail what was looked up and why it did not resolve
   * @return the exception to throw
   */
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
