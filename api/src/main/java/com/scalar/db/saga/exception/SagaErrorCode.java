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
 * <p>Format: {@code DB-SAGA-<CATEGORY><4-DIGIT-ID>}, e.g. {@code DB-SAGA-10201}. The category-digit
 * is the {@link Category}'s id (1=USER_ERROR, 2=RETRYABLE_SERVER_ERROR,
 * 3=NON_RETRYABLE_SERVER_ERROR, 4=CLIENT_ERROR).
 *
 * <p><b>Numbering.</b> Within USER_ERROR, codes cluster into 100-slot sub-ranges by client-facing
 * consequence (HTTP-status family): {@code 100xx}=bad-input (400), {@code 101xx}=auth (401/403),
 * {@code 102xx}=not-found (404), {@code 103xx}=conflict (409), {@code 104xx}=precondition (422).
 * Other categories are single flat blocks — codes number contiguously from {@code XX001}. {@code
 * X9999} is reserved as an always-last sentinel only for categories with a real catch-all code
 * path: {@code 39999} for {@link #INTERNAL_ERROR} (unmapped server fault) and {@code 49999} for
 * {@link #UNRECOGNIZED_SERVER_ERROR} (unknown-code fallback in the client SDK).
 *
 * <p>Codes stay coarse: one per distinct failure class the client meaningfully differentiates, not
 * one per rule violation. A code owns the shape (schema + fixed template + docs page); per-case
 * specifics ride in the {@code detail} metadata field, following the K8s {@code Status.Reason} +
 * {@code Details.Causes[].message} and AWS {@code ValidationException} pattern.
 *
 * <p><b>Once a code is released, its number is frozen.</b>
 */
// Suppress Error Prone's ImmutableEnumChecker: Schema is effectively immutable (its List field is
// Collections.unmodifiableList of a defensive copy), but Error Prone only trusts its own
// @com.google.errorprone.annotations.Immutable which api/ does not depend on. The claim is real.
@SuppressWarnings("ImmutableEnumChecker")
public enum SagaErrorCode {

  // ── USER_ERROR (1xxxx) ────────────────────────────────────────────────

  // ── Bad request (100xx) ──────────────────────────────────────────────
  INVALID_REQUEST(
      "DB-SAGA-10001",
      Category.USER_ERROR,
      "Request is invalid",
      Schema.of("detail"),
      "The request message itself failed validation at the daemon edge: a missing or malformed field, an unparseable body, or an unrecognized query parameter. Only a remote caller can produce this; the embedded engine has no request to validate. A caller value the engine rejected is INVALID_ARGUMENT instead.",
      "Fix the request per the detail and retry."),

  INVALID_ARGUMENT(
      "DB-SAGA-10002",
      Category.USER_ERROR,
      "Argument is invalid",
      Schema.of("detail"),
      "A value the caller passed failed validation — a malformed page token, a blank reason, an out-of-range timestamp. Raised by the engine and by client-side pre-checks, so it reaches callers in both embedded and daemon mode.",
      "Fix the argument per the detail and retry."),

  INVALID_DEFINITION(
      "DB-SAGA-10003",
      Category.USER_ERROR,
      "Saga definition is invalid",
      Schema.of("saga_name", "detail"),
      "The definition violates a validation rule (e.g. duplicate step name, bad pivot placement, malformed field value). The detail identifies the specific violation.",
      "Fix the definition per the detail and re-register."),

  MALFORMED_DEFINITION(
      "DB-SAGA-10004",
      Category.USER_ERROR,
      "Saga definition source is not parseable",
      Schema.of("source", "detail"),
      "The definition source (JSON or YAML) has a syntactic error. The cause carries the parser's diagnostic.",
      "Fix the JSON/YAML syntax error indicated by the parser cause."),

  UNREADABLE_DEFINITION_SOURCE(
      "DB-SAGA-10005",
      Category.USER_ERROR,
      "Saga definition source cannot be read",
      Schema.of("source"),
      "The file, classpath resource, or extension could not be resolved to a readable definition source.",
      "Verify the path/resource exists, is readable, and has a supported extension (.json, .yaml, .yml)."),

  INVALID_STEP_CLASS(
      "DB-SAGA-10006",
      Category.USER_ERROR,
      "Step class cannot be resolved or instantiated",
      Schema.of("step_class", "detail"),
      "Reflective resolution of the class-based step failed (not found, not a Step/TccStep, wrong constructor shape, unresolvable parameter, or constructor threw). The detail identifies the specific failure.",
      "Fix the step class per the detail and ensure it is on the runtime classpath."),

  STEP_CLASS_NOT_SUPPORTED_ON_DAEMON(
      "DB-SAGA-10007",
      Category.USER_ERROR,
      "Class-based step is not supported on the daemon",
      Schema.of("saga_name", "step_name"),
      "The daemon runs declarative definitions only; class steps cannot be executed remotely.",
      "Convert the step to a declarative service step or embed the engine instead."),

  HTTP_ENDPOINT_LOOKUP_FAILED(
      "DB-SAGA-10008",
      Category.USER_ERROR,
      "HTTP endpoint lookup failed",
      Schema.of("detail"),
      "A step needed an HTTP endpoint, but the orchestrator has no matching registration (none registered, name not found, or multiple registered without a qualifier).",
      "Register the endpoint on the orchestrator's builder, fix the lookup name, or annotate the SagaHttpClient parameter with @Named to select one."),

  // ── Auth (101xx) ─────────────────────────────────────────────────────
  UNAUTHENTICATED(
      "DB-SAGA-10101",
      Category.USER_ERROR,
      "Authentication required",
      Schema.none(),
      "The request did not present a valid credential.",
      "Attach a valid credential (API key, bearer token) and retry."),

  PERMISSION_DENIED(
      "DB-SAGA-10102",
      Category.USER_ERROR,
      "Permission denied",
      Schema.none(),
      "The authenticated principal lacks the role required for this operation.",
      "Request the appropriate role from an administrator."),

  // ── Not-found (102xx) ────────────────────────────────────────────────
  SAGA_NOT_FOUND(
      "DB-SAGA-10201",
      Category.USER_ERROR,
      "Saga not found",
      Schema.of("saga_id"),
      "No saga instance exists with the given ID.",
      "Verify the saga ID; the saga may have been purged or never existed."),

  SAGA_DEFINITION_NOT_FOUND(
      "DB-SAGA-10202",
      Category.USER_ERROR,
      "Saga definition not found",
      Schema.of("saga_name"),
      "No saga definition is registered under the given name.",
      "Register the saga definition or fix the name."),

  SAGA_DEFINITION_VERSION_NOT_FOUND(
      "DB-SAGA-10203",
      Category.USER_ERROR,
      "Saga definition version not found",
      Schema.of("saga_name", "version"),
      "The saga definition exists but not at the requested version.",
      "Register that version or start the saga at an existing version."),

  // ── Conflict (103xx) ─────────────────────────────────────────────────
  SAGA_ALREADY_EXISTS(
      "DB-SAGA-10301",
      Category.USER_ERROR,
      "Saga already exists",
      Schema.of("saga_id"),
      "A saga with the given client-supplied ID already exists.",
      "Use a different ID, or fetch the existing saga's state."),

  DEFINITION_VERSION_CONTENT_CONFLICT(
      "DB-SAGA-10302",
      Category.USER_ERROR,
      "Definition version is already registered with different content",
      Schema.of("name", "version"),
      "A definition with this (name, version) already exists but its content differs from what was submitted.",
      "Bump the version instead of re-registering under the same one."),

  // ── Precondition failed (104xx) ──────────────────────────────────────
  SAGA_WRONG_STATE(
      "DB-SAGA-10401",
      Category.USER_ERROR,
      "Operation not allowed in the saga's current state",
      Schema.of("saga_id", "current_state", "requested_operation"),
      "The saga is in a status the operation does not accept (e.g. force-completing a non-escalated saga, resuming an escalated one).",
      "GET the saga for its current state; only certain transitions are allowed per status."),

  SAGA_PARKED(
      "DB-SAGA-10402",
      Category.USER_ERROR,
      "Saga is parked and cannot be resumed automatically",
      Schema.of("saga_id"),
      "The saga is WAITING on an async callback and resolves via the callback or its timeout — not via admin action.",
      "Wait for the callback or the timeout; do not attempt to drive the saga manually."),

  // ── RETRYABLE_SERVER_ERROR (2xxxx) ───────────────────────────────────
  SAGA_CONCURRENT_MODIFICATION(
      "DB-SAGA-20001",
      Category.RETRYABLE_SERVER_ERROR,
      "Another writer modified the saga first",
      Schema.of("saga_id"),
      "Optimistic locking detected a concurrent modification by another engine replica or operator.",
      "Retry the operation from a fresh snapshot. This is typically transient."),

  PERSISTENCE_STORE_UNAVAILABLE(
      "DB-SAGA-20002",
      Category.RETRYABLE_SERVER_ERROR,
      "Underlying store is temporarily unavailable",
      Schema.none(),
      "A transient failure occurred while accessing the underlying store.",
      "Retry the operation. If failures persist, check the store's health."),

  SERVICE_UNAVAILABLE(
      "DB-SAGA-20003",
      Category.RETRYABLE_SERVER_ERROR,
      "Service temporarily unavailable",
      Schema.none(),
      "The saga service could not fulfill the request; the failure is transient. Applies to the saga service itself or one of its upstream dependencies (e.g. the identity provider).",
      "Retry the operation with backoff."),

  RATE_LIMIT_EXCEEDED(
      "DB-SAGA-20004",
      Category.RETRYABLE_SERVER_ERROR,
      "Rate limit exceeded",
      Schema.none(),
      "The request exceeded the server's configured rate limit for this operation.",
      "Back off and retry after a short delay."),

  // ── NON_RETRYABLE_SERVER_ERROR (3xxxx) ───────────────────────────────
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

  // RESERVED, not yet produced. A step failure is recorded into saga state as an event whose
  // payload carries only type, message, and knownNotCommitted, with no room for a code; the engine
  // never attaches either of these two. They are declared so the classification is agreed and
  // numbered, and the reader surfaces them once the engine records one. See todos/032.
  STEP_TIMEOUT(
      "DB-SAGA-30003",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Step timed out",
      Schema.of("step_name", "step_index"),
      "The step exceeded its configured timeout before returning a result.",
      "Increase the step's timeout or optimize the step; the saga is escalated pending intervention."),

  STEP_USER_FAILURE(
      "DB-SAGA-30004",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Step reported a user failure",
      Schema.of("step_name", "step_index"),
      "The step threw a non-retryable failure; a business-rule rejection, or a 4xx from a declarative service step.",
      "Inspect the step's failure detail; the saga compensates and settles."),

  COMPENSATION_FAILED(
      "DB-SAGA-30005",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Compensation of step failed",
      Schema.of("step_name", "step_index"),
      "The step's compensation action threw an exception.",
      "Investigate the compensation implementation; the saga may be parked pending manual intervention."),

  INTERNAL_ERROR(
      "DB-SAGA-39999",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Internal error",
      Schema.none(),
      "The engine encountered an unexpected internal error.",
      "Contact your administrator with the error details."),

  // ── CLIENT_ERROR (4xxxx — client SDK only) ────────────────────────────
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

  REQUEST_ABORTED(
      "DB-SAGA-40003",
      Category.CLIENT_ERROR,
      "The request was aborted before it could complete",
      Schema.none(),
      "The caller cancelled the operation (thread interrupt, future cancellation, or client shutdown) before the RPC completed.",
      "If the abort was unintentional, avoid interrupting or cancelling the calling thread and retry."),

  UNRECOGNIZED_SERVER_ERROR(
      "DB-SAGA-49999",
      Category.CLIENT_ERROR,
      "The server returned a value this client does not recognize",
      Schema.of("server_value"),
      "The server sent an error-code token or wire enum value the client SDK has no mapping for. Usually a version skew: the server is newer than this client SDK.",
      "Upgrade the client SDK to a version compatible with the server."),
  ;

  /**
   * The {@code google.rpc.ErrorInfo} domain scoping every {@code DB-SAGA-*} reason — per the
   * ErrorInfo convention, the service name styled on the vendor's own domain. The daemon stamps it
   * on every {@code ErrorInfo} it emits, and the client SDK ignores an {@code ErrorInfo} carrying
   * any other domain: an intermediary (mesh sidecar, gateway) may attach its own, whose reason
   * means nothing in this vocabulary. Frozen at first release — clients match it exactly, so
   * renaming it would make every older client treat a newer daemon's {@code ErrorInfo} as foreign.
   */
  public static final String WIRE_DOMAIN = "scalardb-saga.scalar-labs.com";

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
