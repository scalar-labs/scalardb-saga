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
 * consequence (HTTP-status family): {@code 100xx}=bad-input (the request-shape statuses: 400 and
 * the framework-generated 405/413/415), {@code 101xx}=auth (401/403), {@code 102xx}=not-found
 * (404), {@code 103xx}=conflict (409), {@code 104xx}=precondition (422). Other categories are
 * single flat blocks — codes number contiguously from {@code XX001}. {@code X9999} is reserved as
 * an always-last sentinel only for categories with a real catch-all code path: {@code 29999} for
 * {@link #UNRECOGNIZED_RETRYABLE_SERVER_ERROR} (the client SDK's unknown-but-retryable fallback),
 * {@code 39999} for {@link #INTERNAL_ERROR} (unmapped server fault), and {@code 49999} for {@link
 * #UNRECOGNIZED_SERVER_ERROR} (unknown-code fallback in the client SDK).
 *
 * <p><b>Retry authority.</b> The {@link Category} of the body's {@code errorCode} — readable from a
 * code string's first digit via {@link Category#fromWireCode} — decides whether to retry; the
 * transport status (HTTP or gRPC) is only the coarse family, never the authority. The two codes
 * sharing HTTP {@code 409} show why: {@link #SAGA_ALREADY_EXISTS} (1xxxx — fix the request) and
 * {@link #SAGA_CONCURRENT_MODIFICATION} (2xxxx — retry from a fresh snapshot) demand opposite
 * reactions that only the code's digit distinguishes.
 *
 * <p>Codes stay coarse: one per distinct failure class the client meaningfully differentiates, not
 * one per rule violation. A code owns the shape (schema + fixed template + docs page); per-case
 * specifics ride in the {@code detail} metadata field, following the K8s {@code Status.Reason} +
 * {@code Details.Causes[].message} and AWS {@code ValidationException} pattern.
 *
 * <p><b>Once a code is released, its number is frozen.</b> Its metadata schema may <b>gain</b>
 * keys, but may never lose or rename one: the client SDK drops unknown keys during reconstruction
 * (so an older client keeps typed reconstruction against an enriched newer server), while a missing
 * declared key is protocol drift and degrades. The asymmetry is deliberate — a client newer than
 * its server still degrades when a newly declared key is absent — so roll servers forward first.
 */
// Suppress Error Prone's ImmutableEnumChecker: ErrorMetadataSchema is effectively immutable — its
// list field is an unmodifiable defensive copy — but Error Prone only trusts its own Immutable
// annotation, which api/ does not depend on. The claim is real.
@SuppressWarnings("ImmutableEnumChecker")
public enum SagaErrorCode {

  // ── USER_ERROR (1xxxx) ────────────────────────────────────────────────

  // ── Bad request (100xx) ──────────────────────────────────────────────
  /**
   * The request message itself failed validation at the server edge: a missing or malformed field,
   * an unparseable body, or an invalid query-parameter value. Only a remote caller can produce it,
   * since the embedded engine has no request to validate; a caller value the engine rejected is
   * {@link #INVALID_ARGUMENT} instead. HTTP 400.
   */
  INVALID_REQUEST(
      "DB-SAGA-10001",
      Category.USER_ERROR,
      "Request is invalid",
      ErrorMetadataSchema.of("detail"),
      "The request message itself failed validation at the server edge: a missing or malformed field, an unparseable body, or an invalid query-parameter value. Only a remote caller can produce this; the embedded engine has no request to validate. A caller value the engine rejected is INVALID_ARGUMENT instead.",
      "Fix the request per the detail and retry."),

  /**
   * A value the caller passed failed validation, such as a malformed page token, a blank reason, or
   * an out-of-range timestamp. Raised by the engine and by client-side checks, so it reaches
   * callers in both embedded and server mode. HTTP 400.
   */
  INVALID_ARGUMENT(
      "DB-SAGA-10002",
      Category.USER_ERROR,
      "Argument is invalid",
      ErrorMetadataSchema.of("detail"),
      "A value the caller passed failed validation — a malformed page token, a blank reason, an out-of-range timestamp. Raised by the engine and by client-side pre-checks, so it reaches callers in both embedded and server mode.",
      "Fix the argument per the detail and retry."),

  /**
   * A saga definition violates a validation rule, such as a duplicate step name, a bad pivot
   * placement, or a malformed field value; the {@code detail} metadata names the violation. HTTP
   * 400.
   */
  INVALID_DEFINITION(
      "DB-SAGA-10003",
      Category.USER_ERROR,
      "Saga definition is invalid",
      ErrorMetadataSchema.of("saga_name", "detail"),
      "The definition violates a validation rule (e.g. duplicate step name, bad pivot placement, malformed field value). The detail identifies the specific violation.",
      "Fix the definition per the detail and re-register."),

  /**
   * A saga definition source (JSON or YAML) has a syntax error; the cause carries the parser's
   * diagnostic. HTTP 400.
   */
  MALFORMED_DEFINITION(
      "DB-SAGA-10004",
      Category.USER_ERROR,
      "Saga definition source is not parseable",
      ErrorMetadataSchema.of("source", "detail"),
      "The definition source (JSON or YAML) has a syntactic error. The cause carries the parser's diagnostic.",
      "Fix the JSON/YAML syntax error indicated by the parser cause."),

  /**
   * A definition source (file, classpath resource, or extension) could not be resolved to a
   * readable definition. HTTP 400.
   */
  UNREADABLE_DEFINITION_SOURCE(
      "DB-SAGA-10005",
      Category.USER_ERROR,
      "Saga definition source cannot be read",
      ErrorMetadataSchema.of("source"),
      "The file, classpath resource, or extension could not be resolved to a readable definition source.",
      "Verify the path/resource exists, is readable, and has a supported extension (.json, .yaml, .yml)."),

  /**
   * A class-based step could not be resolved or instantiated: not found, not a {@code Step} or
   * {@code TccStep}, wrong constructor shape, unresolvable parameter, or a constructor that threw.
   * The {@code detail} metadata names the failure. HTTP 400.
   */
  INVALID_STEP_CLASS(
      "DB-SAGA-10006",
      Category.USER_ERROR,
      "Step class cannot be resolved or instantiated",
      ErrorMetadataSchema.of("step_class", "detail"),
      "Reflective resolution of the class-based step failed (not found, not a Step/TccStep, wrong constructor shape, unresolvable parameter, or constructor threw). The detail identifies the specific failure.",
      "Fix the step class per the detail and ensure it is on the runtime classpath."),

  /**
   * A definition names a class-based step, which a saga server cannot run: it executes declarative
   * definitions only. HTTP 400.
   */
  STEP_CLASS_NOT_SUPPORTED_ON_SERVER(
      "DB-SAGA-10007",
      Category.USER_ERROR,
      "Class-based step is not supported on the server",
      ErrorMetadataSchema.of("saga_name", "step_name"),
      "The server runs declarative definitions only; class steps cannot be executed remotely.",
      "Convert the step to a declarative service step or embed the engine instead."),

  /**
   * A step needed an HTTP endpoint, but the orchestrator has no matching registration: none
   * registered, the name not found, or several registered without a qualifier. HTTP 400.
   */
  HTTP_ENDPOINT_LOOKUP_FAILED(
      "DB-SAGA-10008",
      Category.USER_ERROR,
      "HTTP endpoint lookup failed",
      ErrorMetadataSchema.of("detail"),
      "A step needed an HTTP endpoint, but the orchestrator has no matching registration (none registered, name not found, or multiple registered without a qualifier).",
      "Register the endpoint on the orchestrator's builder, fix the lookup name, or annotate the SagaHttpClient parameter with @Named to select one."),

  /**
   * One declarative service step violates a validation rule, such as a missing or malformed call
   * field or a bad phase combination. The step-scoped sibling of {@link #INVALID_DEFINITION},
   * raised where the failing step is known but the enclosing definition is not. HTTP 400.
   */
  INVALID_STEP_DEFINITION(
      "DB-SAGA-10009",
      Category.USER_ERROR,
      "Declarative step definition is invalid",
      ErrorMetadataSchema.of("step_name", "detail"),
      "One declarative service step violates a validation rule (e.g. a missing or malformed call field, a bad phase combination). The step-scoped sibling of INVALID_DEFINITION: the step-level validation paths know which step failed but not which saga definition encloses it.",
      "Fix the step per the detail and re-register the definition."),

  // ── Auth (101xx) ─────────────────────────────────────────────────────
  /** The request presented no valid credential. HTTP 401. */
  UNAUTHENTICATED(
      "DB-SAGA-10101",
      Category.USER_ERROR,
      "Authentication required",
      ErrorMetadataSchema.none(),
      "The request did not present a valid credential.",
      "Attach a valid credential (API key, bearer token) and retry."),

  /**
   * The authenticated principal lacks the role the operation requires. HTTP 403.
   *
   * <p>The required role is deliberately absent from the schema: metadata is exact-match (no
   * optional keys), and this code is also constructed where no role is known, as the client's
   * transport fallback for a bare {@code PERMISSION_DENIED} status. The role is static policy per
   * operation, documented in the API reference; the server-side audit log carries it for operators.
   */
  PERMISSION_DENIED(
      "DB-SAGA-10102",
      Category.USER_ERROR,
      "Permission denied",
      ErrorMetadataSchema.none(),
      "The authenticated principal lacks the role required for this operation. The role itself is deliberately not sent on the wire; each operation's required role is static policy, documented in the API reference, and the server's audit log records the principal and the missing role.",
      "Look up the operation's required role in the API reference and request it from an administrator."),

  // ── Not-found (102xx) ────────────────────────────────────────────────
  /** No saga instance exists with the given id. HTTP 404. */
  SAGA_NOT_FOUND(
      "DB-SAGA-10201",
      Category.USER_ERROR,
      "Saga not found",
      ErrorMetadataSchema.of("saga_id"),
      "No saga instance exists with the given ID.",
      "Verify the saga ID; the saga may have been purged or never existed."),

  /** No saga definition is registered under the given name. HTTP 404. */
  SAGA_DEFINITION_NOT_FOUND(
      "DB-SAGA-10202",
      Category.USER_ERROR,
      "Saga definition not found",
      ErrorMetadataSchema.of("saga_name"),
      "No saga definition is registered under the given name.",
      "Register the saga definition or fix the name."),

  /** The saga definition exists, but not at the requested version. HTTP 404. */
  SAGA_DEFINITION_VERSION_NOT_FOUND(
      "DB-SAGA-10203",
      Category.USER_ERROR,
      "Saga definition version not found",
      ErrorMetadataSchema.of("saga_name", "version"),
      "The saga definition exists but not at the requested version.",
      "Register that version or start the saga at an existing version."),

  /**
   * The request path or method matches no registered REST route. Only a remote caller can produce
   * it; the embedded engine has no routes. HTTP 404.
   */
  ENDPOINT_NOT_FOUND(
      "DB-SAGA-10204",
      Category.USER_ERROR,
      "No such endpoint",
      ErrorMetadataSchema.of("detail"),
      "The request path (or method) matches no registered REST route. Only a remote caller can produce this; the embedded engine has no routes.",
      "Check the request method and path against the API reference."),

  // ── Conflict (103xx) ─────────────────────────────────────────────────
  /**
   * A saga with the given client-supplied id already exists. HTTP 409, a status it shares with
   * {@link #SAGA_CONCURRENT_MODIFICATION}; only the category digit tells a request to fix from a
   * request to retry.
   */
  SAGA_ALREADY_EXISTS(
      "DB-SAGA-10301",
      Category.USER_ERROR,
      "Saga already exists",
      ErrorMetadataSchema.of("saga_id"),
      "A saga with the given client-supplied ID already exists.",
      "Use a different ID, or fetch the existing saga's state."),

  /**
   * A definition is already registered under this name and version with different content; bump the
   * version instead of re-registering under the same one. HTTP 409.
   */
  SAGA_DEFINITION_VERSION_CONTENT_CONFLICT(
      "DB-SAGA-10302",
      Category.USER_ERROR,
      "Definition version is already registered with different content",
      ErrorMetadataSchema.of("saga_name", "version"),
      "A definition with this (name, version) already exists but its content differs from what was submitted.",
      "Bump the version instead of re-registering under the same one."),

  // ── Precondition failed (104xx) ──────────────────────────────────────
  /**
   * The saga is in a status the requested operation does not accept, such as force-completing a
   * saga that is not escalated or resuming one that is. HTTP 422.
   */
  SAGA_WRONG_STATE(
      "DB-SAGA-10401",
      Category.USER_ERROR,
      "Operation not allowed in the saga's current state",
      ErrorMetadataSchema.of("saga_id", "current_state", "requested_operation"),
      "The saga is in a status the operation does not accept (e.g. force-completing a non-escalated saga, resuming an escalated one).",
      "GET the saga for its current state; only certain transitions are allowed per status."),

  /**
   * The saga is waiting on an asynchronous callback and resolves through that callback or its
   * timeout, not through admin action. HTTP 422.
   */
  SAGA_PARKED(
      "DB-SAGA-10402",
      Category.USER_ERROR,
      "Saga is parked and cannot be resumed automatically",
      ErrorMetadataSchema.of("saga_id"),
      "The saga is WAITING on an async callback and resolves via the callback or its timeout — not via admin action.",
      "Wait for the callback or the timeout; do not attempt to drive the saga manually."),

  /**
   * The saga definition is registered in the store but is not part of this daemon's configuration:
   * either it was retired by removing its definition file, or this replica has not yet caught up
   * with a newly added one. Sagas already running under it finish normally. HTTP 422.
   */
  SAGA_DEFINITION_NOT_SERVED(
      "DB-SAGA-10403",
      Category.USER_ERROR,
      "Saga definition is registered but not served by this daemon",
      ErrorMetadataSchema.of("saga_name"),
      "The saga is registered in the store but is not part of this daemon's configuration: either it was retired by removing its definition file, or this replica's configuration has not caught up with a newly added one. Sagas already running under it finish normally.",
      "If the saga was retired, stop starting it. If it was just added, retry — another replica may already serve it, and this one will once its configuration syncs."),

  // ── RETRYABLE_SERVER_ERROR (2xxxx) ───────────────────────────────────
  /**
   * Optimistic locking detected that another engine replica or an operator modified the saga first;
   * retry from a fresh snapshot. HTTP 409, the status it shares with {@link #SAGA_ALREADY_EXISTS},
   * which demands the opposite reaction.
   */
  SAGA_CONCURRENT_MODIFICATION(
      "DB-SAGA-20001",
      Category.RETRYABLE_SERVER_ERROR,
      "Another writer modified the saga first",
      ErrorMetadataSchema.of("saga_id"),
      "Optimistic locking detected a concurrent modification by another engine replica or operator.",
      "Retry the operation from a fresh snapshot. This is typically transient."),

  /** A transient failure occurred while accessing the underlying store. */
  PERSISTENCE_STORE_UNAVAILABLE(
      "DB-SAGA-20002",
      Category.RETRYABLE_SERVER_ERROR,
      "Underlying store is temporarily unavailable",
      ErrorMetadataSchema.none(),
      "A transient failure occurred while accessing the underlying store.",
      "Retry the operation. If failures persist, check the store's health."),

  /**
   * The saga service, or one of its upstream dependencies such as the identity provider, could not
   * fulfil the request; the failure is transient.
   */
  SERVICE_UNAVAILABLE(
      "DB-SAGA-20003",
      Category.RETRYABLE_SERVER_ERROR,
      "Service temporarily unavailable",
      ErrorMetadataSchema.none(),
      "The saga service could not fulfill the request; the failure is transient. Applies to the saga service itself or one of its upstream dependencies (e.g. the identity provider).",
      "Retry the operation with backoff."),

  /** The request exceeded the server's configured rate limit for this operation. */
  RATE_LIMIT_EXCEEDED(
      "DB-SAGA-20004",
      Category.RETRYABLE_SERVER_ERROR,
      "Rate limit exceeded",
      ErrorMetadataSchema.none(),
      "The request exceeded the server's configured rate limit for this operation.",
      "Back off and retry after a short delay."),

  /**
   * The server abandoned the operation mid-flight, typically interrupted by its own shutdown,
   * rather than a dependency failing. Distinct from {@link #REQUEST_ABORTED}, where the caller
   * cancelled its own request.
   */
  OPERATION_ABORTED(
      "DB-SAGA-20005",
      Category.RETRYABLE_SERVER_ERROR,
      "Operation aborted by the server",
      ErrorMetadataSchema.none(),
      "The server abandoned the operation mid-flight — typically an interrupt during shutdown — rather than a dependency failing. Distinct from REQUEST_ABORTED, where the caller cancelled its own request.",
      "Retry the operation; it will land on another replica, or on this server after it restarts."),

  /**
   * The server already runs as many sagas as its admission cap allows and refused to start another.
   * Nothing was persisted: the saga does not exist and its id is still free. Distinct from {@link
   * #RATE_LIMIT_EXCEEDED}, which bounds how often one caller may ask; this bounds how much work the
   * server does for everyone.
   */
  ENGINE_OVERLOADED(
      "DB-SAGA-20006",
      Category.RETRYABLE_SERVER_ERROR,
      "The engine is at its concurrent saga limit",
      ErrorMetadataSchema.none(),
      "The server is already running as many sagas as its admission cap allows, so it refused to start another. Nothing was persisted: the saga does not exist and its ID is still free. Distinct from RATE_LIMIT_EXCEEDED, which is about how often this caller may ask; this is about how much work the server is doing for everyone.",
      "Retry with backoff and jitter, honouring Retry-After when present. Sustained rejection means the cap, the request rate limit, or downstream capacity needs revisiting."),

  /**
   * The server sent a retryable (2xxxx) code this client SDK has no mapping for, usually because
   * the server is newer. The category digit is a frozen wire contract, so the failure is known to
   * be transient even though the specific code is not.
   */
  UNRECOGNIZED_RETRYABLE_SERVER_ERROR(
      "DB-SAGA-29999",
      Category.RETRYABLE_SERVER_ERROR,
      "The server reported a retryable error this client does not recognize",
      ErrorMetadataSchema.of("server_value"),
      "The server sent a RETRYABLE_SERVER_ERROR (2xxxx) code this client SDK has no mapping for; usually a version skew where the server is newer. The category digit is a frozen wire contract, so the failure is known to be transient even though the specific code is not.",
      "Retry with backoff; upgrade the client SDK to interpret the specific code."),

  // ── NON_RETRYABLE_SERVER_ERROR (3xxxx) ───────────────────────────────
  /** JSON serialization of an event payload failed; typically an engine bug. */
  PERSISTENCE_SERIALIZATION_FAILED(
      "DB-SAGA-30001",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Failed to serialize event payload",
      ErrorMetadataSchema.none(),
      "JSON serialization of the event payload failed.",
      "Inspect the payload structure; this is typically an engine bug."),

  /**
   * Stored data could not be deserialized, possibly because the schema drifted between the writer
   * and the reader.
   */
  PERSISTENCE_DESERIALIZATION_FAILED(
      "DB-SAGA-30002",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Failed to deserialize event payload or definition",
      ErrorMetadataSchema.none(),
      "The stored data could not be deserialized, possibly due to schema drift.",
      "Check for schema-version mismatch between the writer and reader."),

  /**
   * Reserved, not yet produced: a step exceeded its configured timeout before returning a result.
   *
   * <p>A step failure is recorded into saga state as an event whose payload carries only its type,
   * message, and whether the step is known not to have committed, with no room for a code, so the
   * engine never attaches this code or {@link #STEP_USER_FAILURE} today. Both are declared so the
   * classification is agreed and numbered, and readers surface them once the engine records one.
   */
  STEP_TIMEOUT(
      "DB-SAGA-30003",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Step timed out",
      ErrorMetadataSchema.of("step_name", "step_index"),
      "The step exceeded its configured timeout before returning a result.",
      "Increase the step's timeout or optimize the step; the saga compensates and settles."),

  /**
   * Reserved, not yet produced, for the same reason as {@link #STEP_TIMEOUT}: a step threw a non-
   * retryable failure, such as a business-rule rejection or a 4xx from a declarative service step.
   */
  STEP_USER_FAILURE(
      "DB-SAGA-30004",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Step reported a user failure",
      ErrorMetadataSchema.of("step_name", "step_index"),
      "The step threw a non-retryable failure; a business-rule rejection, or a 4xx from a declarative service step.",
      "Inspect the step's failure detail; the saga compensates and settles."),

  /**
   * A step's compensation action threw. The saga stays {@code COMPENSATING} and recovery retries
   * the compensation; one stuck past the compensation grace period is escalated for manual
   * intervention.
   */
  COMPENSATION_FAILED(
      "DB-SAGA-30005",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Compensation of step failed",
      ErrorMetadataSchema.of("step_name", "step_index"),
      "The step's compensation action threw an exception.",
      "Investigate the compensation implementation; recovery retries the compensation and escalates the saga if it stays stuck past the grace period."),

  /**
   * The engine hit an unexpected internal error; the catch-all for a server fault no other code
   * describes.
   */
  INTERNAL_ERROR(
      "DB-SAGA-39999",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Internal error",
      ErrorMetadataSchema.none(),
      "The engine encountered an unexpected internal error.",
      "Contact your administrator with the error details."),

  // ── CLIENT_ERROR (4xxxx — client SDK only) ────────────────────────────

  /**
   * Reserved, not yet produced: the request did not reach the server, or the server returned no
   * structured error.
   *
   * <p>The client maps every {@code UNAVAILABLE} without an {@code ErrorInfo} to {@link
   * #SERVICE_UNAVAILABLE} today, because the transport layer cannot reliably distinguish a request
   * that never reached the server from a server that said it is unavailable. Declared so the
   * classification is agreed and numbered at the head of the CLIENT_ERROR block.
   */
  SERVER_UNREACHABLE(
      "DB-SAGA-40001",
      Category.CLIENT_ERROR,
      "The saga server could not be reached",
      ErrorMetadataSchema.none(),
      "The request did not reach the server, or the server returned no structured error.",
      "Verify the server endpoint and network connectivity, then retry."),

  /** The request did not complete before its deadline. */
  REQUEST_TIMEOUT(
      "DB-SAGA-40002",
      Category.CLIENT_ERROR,
      "The request to the saga server timed out",
      ErrorMetadataSchema.none(),
      "The request did not complete before its deadline.",
      "Retry the request or increase the deadline."),

  /**
   * The caller cancelled the operation (a thread interrupt, a future cancellation, or client
   * shutdown) before the RPC completed.
   */
  REQUEST_ABORTED(
      "DB-SAGA-40003",
      Category.CLIENT_ERROR,
      "The request was aborted before it could complete",
      ErrorMetadataSchema.none(),
      "The caller cancelled the operation (thread interrupt, future cancellation, or client shutdown) before the RPC completed.",
      "If the abort was unintentional, avoid interrupting or cancelling the calling thread and retry."),

  /**
   * The response carried a bare transport status and no saga error body. Servers attach a code to
   * every response, so this usually comes from the transport runtime itself (a message-size
   * rejection, say) or an intermediary, not from the saga application; upgrading the client SDK
   * will not change it.
   */
  UNMAPPED_SERVER_STATUS(
      "DB-SAGA-40004",
      Category.CLIENT_ERROR,
      "The server reported only a transport status with no error body",
      ErrorMetadataSchema.of("server_value"),
      "The response carried a bare transport status and no saga error body. Servers attach a code to every response, so this typically comes from the transport runtime itself (a message-size rejection, say) or an intermediary, not from the saga application.",
      "Inspect server_value (the transport status) and the cause. This is not a version skew; upgrading the client SDK will not change it."),

  /**
   * The saga did not reach a terminal state within the client-side wait bound. Usually it is still
   * running and will settle on its own, but the budget can also expire while the first start
   * request is still being retried, in which case whether anything was persisted is unknown: poll
   * the saga under its id rather than re-sending under a fresh one. Distinct from {@link
   * #REQUEST_TIMEOUT}, where the request itself did not complete.
   */
  SAGA_AWAIT_TIMEOUT(
      "DB-SAGA-40005",
      Category.CLIENT_ERROR,
      "The wait for the saga to finish timed out",
      ErrorMetadataSchema.of("saga_id"),
      "The saga did not reach a terminal state within the client-side wait bound: the caller's wait budget expired. Usually the saga is running and will settle on its own, but the budget can also expire while the first start request is still being retried, where whether anything was persisted is unknown. Distinct from REQUEST_TIMEOUT, where the request itself did not complete.",
      "Poll saga_id (getStateSnapshot or awaitSaga) to learn where the saga stands: either it is running and will settle on its own, or it is not found, meaning the start had not landed when the poll ran. Do not re-send under a fresh ID. saga_id is also the idempotency key the start was sent under, and an attempt still in flight can land after a not-found poll, so any re-send must reuse saga_id. A caller that started the saga without supplying an ID has no other copy of it."),

  /**
   * The server sent an error-code token or wire enum value this client SDK has no mapping for;
   * usually the server is newer than the client.
   */
  UNRECOGNIZED_SERVER_ERROR(
      "DB-SAGA-49999",
      Category.CLIENT_ERROR,
      "The server returned a value this client does not recognize",
      ErrorMetadataSchema.of("server_value"),
      "The server sent an error-code token or wire enum value the client SDK has no mapping for. Usually a version skew: the server is newer than this client SDK.",
      "Upgrade the client SDK to a version compatible with the server."),
  ;

  /**
   * The globally unique identifier scoping this code vocabulary — the service name styled on the
   * vendor's own domain. Wherever a {@code DB-SAGA-*} reason travels beside other producers' codes,
   * this says whose vocabulary the reason belongs to. Its one carrier today is gRPC's {@code
   * google.rpc.ErrorInfo.domain}: the server stamps it on every {@code ErrorInfo} it emits, and the
   * client SDK ignores an {@code ErrorInfo} carrying any other domain, since an intermediary (mesh
   * sidecar, gateway) may attach its own. Frozen at first release — clients match it verbatim, so
   * renaming it would make every older client treat a newer server's {@code ErrorInfo} as foreign.
   */
  public static final String WIRE_DOMAIN = "scalardb-saga.scalar-labs.com";

  private final String code;
  private final Category category;
  private final String message;
  private final ErrorMetadataSchema schema;
  private final String cause;
  private final String action;

  SagaErrorCode(
      String code,
      Category category,
      String message,
      ErrorMetadataSchema schema,
      String cause,
      String action) {
    this.code = code;
    this.category = category;
    this.message = message;
    this.schema = schema;
    this.cause = cause;
    this.action = action;
  }

  /**
   * The wire code string, {@code DB-SAGA-<CATEGORY><4-DIGIT-ID>}, frozen once released.
   *
   * @return the code, such as {@code DB-SAGA-10201}
   */
  public String code() {
    return code;
  }

  /**
   * The caller-reaction category, which decides whether the operation is worth retrying.
   *
   * @return the category
   */
  public Category category() {
    return category;
  }

  /**
   * The fixed human-readable message every instance of this code renders.
   *
   * @return the message
   */
  public String message() {
    return message;
  }

  /**
   * The metadata keys an instance of this code must carry.
   *
   * @return the schema
   */
  public ErrorMetadataSchema schema() {
    return schema;
  }

  /**
   * A description of what produces this code, written for the generated error reference.
   *
   * @return the cause description
   */
  public String cause() {
    return cause;
  }

  /**
   * The recommended caller reaction, written for the generated error reference.
   *
   * @return the recommended action
   */
  public String action() {
    return action;
  }

  /**
   * Assembles the runtime message: {@code "<code>: <message>"} for a schemaless code, or {@code
   * "<code>: <message> [k1=v1, k2=v2]"} for one with metadata. Keys iterate in schema-declared
   * order (not map order) for stable log lines. The base exception constructor pre-validates {@code
   * metadata} via {@link ErrorMetadataSchema#validate}; passing anything else in bypasses that
   * check.
   *
   * @param metadata the metadata map, with exactly the keys {@link #schema()} declares
   * @return the assembled message
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

  /**
   * Reverse lookup: the enum constant with the given wire code, or empty if unknown.
   *
   * @param code the wire code string, such as {@code DB-SAGA-10201}
   * @return the constant carrying that code, or empty if none does
   */
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

    /**
     * The category's digit, which is the first of the five digits in a wire code.
     *
     * @return the digit, as a one-character string
     */
    public String id() {
      return id;
    }

    /**
     * Parses the category digit of a well-formed wire code ({@code DB-SAGA-NNNNN}) into its
     * category, or empty for anything else. The digit is a frozen wire contract, so a client can
     * classify a code it does not otherwise recognize — the retryability signal survives a version
     * skew where the server is newer than the client.
     *
     * @param wireCode the wire code string to classify
     * @return the category the first digit names, or empty when the string is not a well-formed
     *     code
     */
    public static Optional<Category> fromWireCode(String wireCode) {
      String prefix = "DB-SAGA-";
      if (wireCode.length() != prefix.length() + 5 || !wireCode.startsWith(prefix)) {
        return Optional.empty();
      }
      for (int i = prefix.length(); i < wireCode.length(); i++) {
        char c = wireCode.charAt(i);
        if (c < '0' || c > '9') {
          return Optional.empty();
        }
      }
      char digit = wireCode.charAt(prefix.length());
      for (Category category : values()) {
        if (category.id.charAt(0) == digit) {
          return Optional.of(category);
        }
      }
      return Optional.empty();
    }
  }
}
