package com.scalar.db.saga.exception;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base type for the unchecked saga exceptions.
 *
 * <p>The saga-level exceptions ({@link SagaNotFoundException}, {@link SagaAlreadyExistsException},
 * {@link SagaPersistenceException}, {@link SagaTimeoutException}, etc.) all extend {@code
 * SagaRuntimeException}, so callers can {@code catch (SagaRuntimeException e)} to handle any saga
 * failure uniformly. It is also thrown directly for failures that have no natural typed home,
 * notably the client SDK's {@link SagaErrorCode#UNRECOGNIZED_SERVER_ERROR} fallback.
 *
 * <p>The api may declare exceptions that only a remote implementation throws (e.g. {@link
 * SagaUnavailableException}); the {@code SagaOrchestrator} contract spans both the embedded and
 * remote implementations. (The step-level exceptions are a separate, partly-checked hierarchy and
 * do not extend this type.)
 *
 * <p>Every instance carries a non-null {@link SagaErrorCode} and its schema-validated metadata map.
 * {@link ErrorMetadataSchema#validate} runs at construction, so a missing/extra/null-valued key
 * fails fast rather than shipping to logs and the wire.
 */
public class SagaRuntimeException extends RuntimeException {

  private final SagaErrorCode errorCode;
  private final Map<String, String> metadata;

  /**
   * Constructs an exception carrying a {@link SagaErrorCode} and the metadata that renders in its
   * message.
   *
   * <p>Typed subclasses (e.g. {@link SagaNotFoundException}) are the usual construction path; raw
   * {@code SagaRuntimeException} with a code is reserved for cases with no natural typed home,
   * notably the client SDK's {@link SagaErrorCode#UNRECOGNIZED_SERVER_ERROR} fallback.
   *
   * @throws NullPointerException if {@code code} or {@code metadata} is null
   * @throws IllegalArgumentException if {@code metadata} does not satisfy the code's schema
   */
  public SagaRuntimeException(SagaErrorCode code, Map<String, String> metadata) {
    super(build(code, metadata));
    this.errorCode = code;
    // Defensive copy in the ctor so SpotBugs's EI_EXPOSE_REP is satisfied seeing the copy in the
    // ctor's bytecode; also lets the getter return the field directly.
    this.metadata = Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
  }

  /**
   * Constructs an exception carrying a {@link SagaErrorCode}, its metadata, and an underlying
   * cause. Validation is the same as the two-argument constructor.
   *
   * @throws NullPointerException if any argument is null
   * @throws IllegalArgumentException if {@code metadata} does not satisfy the code's schema
   */
  public SagaRuntimeException(SagaErrorCode code, Map<String, String> metadata, Throwable cause) {
    super(build(code, metadata), Objects.requireNonNull(cause, "cause must not be null"));
    this.errorCode = code;
    this.metadata = Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
  }

  /** The exception's error code — always non-null. */
  public SagaErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * Never {@code null}, unlike the {@link Throwable} contract: every constructor builds the message
   * from the code and metadata, and the wire mappers reuse it as the response message, so the
   * non-null return lets them do that without a check.
   */
  @Override
  public String getMessage() {
    return Objects.requireNonNull(super.getMessage());
  }

  /**
   * The metadata attached to the error code, in schema-declared order. Always non-null; empty when
   * the code has a schemaless (no-key) shape.
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  private static String build(SagaErrorCode code, Map<String, String> metadata) {
    Objects.requireNonNull(code, "code must not be null");
    Objects.requireNonNull(metadata, "metadata must not be null");
    code.schema().validate(code, metadata);
    return code.buildMessage(metadata);
  }
}
