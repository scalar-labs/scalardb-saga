package com.scalar.db.saga.exception;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Structured error codes for every failure the saga engine and its clients report. Each constant
 * owns everything a caller, an operator, or the docs generator needs — the wire code string,
 * category, fixed human-readable message, metadata schema, cause description, and recommended
 * action. Wire mappers ({@code GrpcErrorMapper}, {@code ErrorMapper}) and the docs generator read
 * this enum as their single source of truth; exception constructors are typed adapters that
 * populate the metadata map once.
 *
 * <p>Format: {@code DB-SAGA-<CATEGORY><4-DIGIT-ID>}, e.g. {@code DB-SAGA-11000}. See {@link
 * Category}.
 *
 * <p>Codes stay coarse: one per distinct failure class the client meaningfully differentiates, not
 * one per rule violation. A code owns the shape (schema + fixed template + docs page); per-case
 * specifics ride in the {@code detail} metadata field, following the K8s {@code Status.Reason} +
 * {@code Details.Causes[].message} and AWS {@code ValidationException} pattern.
 */
// Suppress Error Prone's ImmutableEnumChecker: Schema is effectively immutable (its List field is
// Collections.unmodifiableList of a defensive copy), but Error Prone only trusts its own
// @com.google.errorprone.annotations.Immutable which api/ does not depend on. The claim is real.
@SuppressWarnings("ImmutableEnumChecker")
public enum SagaErrorCode {

  // ── USER_ERROR — Saga definition (100xx) ────────────────────────────
  DEFINITION_INVALID(
      "DB-SAGA-10003",
      Category.USER_ERROR,
      "Saga definition is invalid",
      Schema.of("saga_name", "detail"),
      "The definition violates a validation rule (e.g. duplicate step name, bad pivot placement, malformed field value). The detail identifies the specific violation.",
      "Fix the definition per the detail and re-register."),

  DEFINITION_MALFORMED(
      "DB-SAGA-10004",
      Category.USER_ERROR,
      "Saga definition source is not parseable",
      Schema.of("source", "detail"),
      "The definition source (JSON or YAML) has a syntactic error. The cause carries the parser's diagnostic.",
      "Fix the JSON/YAML syntax error indicated by the parser cause."),

  DEFINITION_SOURCE_UNREADABLE(
      "DB-SAGA-10005",
      Category.USER_ERROR,
      "Saga definition source cannot be read",
      Schema.of("source"),
      "The file, classpath resource, or extension could not be resolved to a readable definition source.",
      "Verify the path/resource exists, is readable, and has a supported extension (.json, .yaml, .yml)."),

  DEFINITION_VERSION_CONTENT_CONFLICT(
      "DB-SAGA-10006",
      Category.USER_ERROR,
      "Definition version is already registered with different content",
      Schema.of("name", "version"),
      "A definition with this (name, version) already exists but its content differs from what was submitted.",
      "Bump the version instead of re-registering under the same one."),

  STEP_CLASS_INVALID(
      "DB-SAGA-10007",
      Category.USER_ERROR,
      "Step class cannot be resolved or instantiated",
      Schema.of("step_class", "detail"),
      "Reflective resolution of the class-based step failed (not found, not a Step/TccStep, wrong constructor shape, unresolvable parameter, or constructor threw). The detail identifies the specific failure.",
      "Fix the step class per the detail and ensure it is on the runtime classpath."),

  STEP_CLASS_NOT_SUPPORTED_ON_DAEMON(
      "DB-SAGA-10008",
      Category.USER_ERROR,
      "Class-based step is not supported on the daemon",
      Schema.of("saga_name", "step_name"),
      "The daemon runs declarative definitions only; class steps cannot be executed remotely.",
      "Convert the step to a declarative service step or embed the engine instead."),

  HTTP_ENDPOINT_LOOKUP_FAILED(
      "DB-SAGA-10009",
      Category.USER_ERROR,
      "HTTP endpoint lookup failed",
      Schema.of("detail"),
      "A step needed an HTTP endpoint, but the orchestrator has no matching registration (none registered, name not found, or multiple registered without a qualifier).",
      "Register the endpoint on the orchestrator's builder, fix the lookup name, or annotate the SagaHttpClient parameter with @Named to select one."),

  // ── USER_ERROR — Auth (104xx) ───────────────────────────────────────
  AUTH_UNAUTHENTICATED(
      "DB-SAGA-10400",
      Category.USER_ERROR,
      "Authentication required",
      Schema.none(),
      "The request did not present a valid credential.",
      "Attach a valid credential (API key, bearer token) and retry."),

  AUTH_PERMISSION_DENIED(
      "DB-SAGA-10401",
      Category.USER_ERROR,
      "Permission denied",
      Schema.none(),
      "The authenticated principal lacks the role required for this operation.",
      "Request the appropriate role from an administrator."),

  // ── USER_ERROR — Not-found (110xx) ──────────────────────────────────
  SAGA_NOT_FOUND(
      "DB-SAGA-11000",
      Category.USER_ERROR,
      "Saga not found",
      Schema.of("saga_id"),
      "No saga instance exists with the given ID.",
      "Verify the saga ID and that the saga has been started."),

  SAGA_DEFINITION_NOT_FOUND(
      "DB-SAGA-11001",
      Category.USER_ERROR,
      "Saga definition not found",
      Schema.of("saga_name"),
      "No saga definition is registered with the given name.",
      "Register the definition before starting a saga."),

  SAGA_DEFINITION_VERSION_NOT_FOUND(
      "DB-SAGA-11002",
      Category.USER_ERROR,
      "Saga definition version not found",
      Schema.of("saga_name", "version"),
      "The specified version of the saga definition is not registered.",
      "Check available versions or register this version."),

  // ── USER_ERROR — Already-exists (111xx) ─────────────────────────────
  SAGA_ALREADY_EXISTS(
      "DB-SAGA-11100",
      Category.USER_ERROR,
      "Saga already exists",
      Schema.of("saga_id"),
      "A saga with the caller-supplied ID has already been started.",
      "Use a different saga ID, or omit the ID to let the engine generate one."),

  // ── USER_ERROR — Wrong-state (112xx) ────────────────────────────────
  SAGA_WRONG_STATE(
      "DB-SAGA-11200",
      Category.USER_ERROR,
      "Operation not allowed in the saga's current state",
      Schema.of("saga_id", "current_state", "requested_operation"),
      "The requested operation is not valid for a saga in this state.",
      "Wait for the saga to reach a compatible state, or issue a different operation."),

  SAGA_PARKED(
      "DB-SAGA-11201",
      Category.USER_ERROR,
      "Saga is parked and cannot be resumed automatically",
      Schema.of("saga_id"),
      "The saga has been parked (typically after repeated compensation failures) and requires manual intervention.",
      "Investigate the parked saga, then unpark or terminate it explicitly."),

  // ── RETRYABLE_SERVER_ERROR (2xxxx) ──────────────────────────────────
  SAGA_CONCURRENT_MODIFICATION(
      "DB-SAGA-20001",
      Category.RETRYABLE_SERVER_ERROR,
      "Another writer modified the saga first",
      Schema.of("saga_id"),
      "Optimistic locking detected a concurrent modification by another engine replica or operator.",
      "Retry the operation from a fresh snapshot. This is typically transient."),

  PERSISTENCE_STORE_UNAVAILABLE(
      "DB-SAGA-20010",
      Category.RETRYABLE_SERVER_ERROR,
      "Underlying store is temporarily unavailable",
      Schema.none(),
      "A transient failure occurred while accessing the underlying store.",
      "Retry the operation. If failures persist, check the store's health."),

  SERVICE_UNAVAILABLE(
      "DB-SAGA-20020",
      Category.RETRYABLE_SERVER_ERROR,
      "Saga service temporarily unavailable",
      Schema.none(),
      "The saga service could not fulfill the request; the failure is transient.",
      "Retry the operation with backoff."),

  RATE_LIMIT_EXCEEDED(
      "DB-SAGA-20030",
      Category.RETRYABLE_SERVER_ERROR,
      "Rate limit exceeded",
      Schema.none(),
      "The request exceeded the server's configured rate limit for this operation.",
      "Back off and retry after a short delay."),

  // ── NON_RETRYABLE_SERVER_ERROR (3xxxx) ──────────────────────────────
  PERSISTENCE_SERIALIZATION_FAILED(
      "DB-SAGA-30001",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Failed to serialize event payload",
      Schema.none(),
      "JSON serialization of the event payload failed.",
      "Inspect the payload structure; this is typically an engine bug."),

  PERSISTENCE_DESERIALIZATION_FAILED(
      "DB-SAGA-30002",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Failed to deserialize event payload or definition",
      Schema.none(),
      "The stored data could not be deserialized, possibly due to schema drift.",
      "Check for schema-version mismatch between the writer and reader."),

  STEP_TIMEOUT(
      "DB-SAGA-30010",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Step timed out",
      Schema.of("step_name", "step_index"),
      "The step did not complete before its configured deadline; the engine's retry policy is exhausted.",
      "Investigate the step for hangs, or raise its deadline / retry budget."),

  STEP_USER_FAILURE(
      "DB-SAGA-30011",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Step failed with a user-thrown exception",
      Schema.none(),
      "A saga step threw an exception the engine did not classify further; the wrapped cause carries the details.",
      "Inspect the step implementation and the exception cause chain."),

  COMPENSATION_FAILED(
      "DB-SAGA-30020",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Compensation of step failed",
      Schema.of("step_name", "step_index"),
      "The step's compensation action threw an exception.",
      "Investigate the compensation implementation; the saga may be parked pending manual intervention."),

  INTERNAL_ERROR(
      "DB-SAGA-30099",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Internal error",
      Schema.none(),
      "The engine encountered an unexpected internal error.",
      "Contact your administrator with the error details."),

  // ── CLIENT_ERROR — produced only by the client SDK (4xxxx) ──────────
  SERVER_UNREACHABLE(
      "DB-SAGA-40001",
      Category.CLIENT_ERROR,
      "The saga server could not be reached",
      Schema.none(),
      "The request did not reach the server, or the server returned no structured error.",
      "Verify the server endpoint and network connectivity, then retry."),

  REQUEST_TIMEOUT(
      "DB-SAGA-40002",
      Category.CLIENT_ERROR,
      "The request to the saga server timed out",
      Schema.none(),
      "The request did not complete before its deadline.",
      "Retry the request or increase the deadline."),

  UNRECOGNIZED_SERVER_ERROR(
      "DB-SAGA-40003",
      Category.CLIENT_ERROR,
      "The server returned an error code this client does not recognize",
      Schema.of("server_error_code"),
      "The server is likely newer than this client SDK.",
      "Upgrade the client SDK to a version compatible with the server."),
  ;

  private final String code;
  private final Category category;
  private final String message;
  private final Schema schema;
  private final String cause;
  private final String action;

  SagaErrorCode(
      String code, Category category, String message, Schema schema, String cause, String action) {
    this.code = code;
    this.category = category;
    this.message = message;
    this.schema = schema;
    this.cause = cause;
    this.action = action;
  }

  public String code() {
    return code;
  }

  public Category category() {
    return category;
  }

  public String message() {
    return message;
  }

  public Schema schema() {
    return schema;
  }

  public String cause() {
    return cause;
  }

  public String action() {
    return action;
  }

  /**
   * Assembles the runtime message: {@code "<code>: <message>"} for a schemaless code, or {@code
   * "<code>: <message> [k1=v1, k2=v2]"} for one with metadata. Keys iterate in schema-declared
   * order (not map order) for stable log lines. The base exception constructor pre-validates {@code
   * metadata} via {@link Schema#validate}; passing anything else in bypasses that check.
   */
  public String buildMessage(Map<String, String> metadata) {
    if (schema.requiredKeys().isEmpty()) {
      return code + ": " + message;
    }
    StringBuilder sb = new StringBuilder(code).append(": ").append(message).append(" [");
    boolean first = true;
    for (String key : schema.requiredKeys()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(key).append('=').append(metadata.get(key));
      first = false;
    }
    return sb.append(']').toString();
  }

  private static final Map<String, SagaErrorCode> BY_CODE;

  static {
    Map<String, SagaErrorCode> m = new HashMap<>();
    for (SagaErrorCode e : values()) {
      m.put(e.code, e);
    }
    BY_CODE = Collections.unmodifiableMap(m);
  }

  /** Reverse lookup: the enum constant with the given wire code, or empty if unknown. */
  public static Optional<SagaErrorCode> fromCode(String code) {
    return Optional.ofNullable(BY_CODE.get(code));
  }

  /**
   * Caller-reaction axis. The wire status (HTTP/gRPC) is a separate axis mapped per exception TYPE
   * — see the design doc's "Wire Serialization" section.
   */
  public enum Category {
    /** Fix the request or the target state; retrying unchanged will fail identically. */
    USER_ERROR("1"),
    /** Retry the same operation; the failure is transient and safe to re-execute. */
    RETRYABLE_SERVER_ERROR("2"),
    /** Escalate to an operator; retrying will not help. */
    NON_RETRYABLE_SERVER_ERROR("3"),
    /**
     * Transport failed on the client before a structured server error was received (network
     * failure, deadline, unrecognized code).
     */
    CLIENT_ERROR("4");

    private final String id;

    Category(String id) {
      this.id = id;
    }

    public String id() {
      return id;
    }
  }
}
