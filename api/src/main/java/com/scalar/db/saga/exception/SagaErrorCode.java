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
      "The request is malformed: a required field is missing or malformed, the body cannot be parsed, or a query parameter has an invalid value. The `detail` metadata says which.",
      "Fix the request as the detail describes, then send it again."),

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
      "A value in the request is not valid, such as a malformed page token, a blank reason, or an out-of-range timestamp. The `detail` metadata says which value.",
      "Fix the value as the detail describes, then send the request again."),

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
      "A saga definition breaks a validation rule, such as a duplicate step name or a misplaced pivot step. The `detail` metadata names the rule.",
      "Fix the definition as the detail describes, then load it again."),

  /**
   * A saga definition source (JSON or YAML) has a syntax error; the cause carries the parser's
   * diagnostic. HTTP 400.
   */
  MALFORMED_DEFINITION(
      "DB-SAGA-10004",
      Category.USER_ERROR,
      "Saga definition source is not parseable",
      ErrorMetadataSchema.of("source", "detail"),
      "A definition file is not valid JSON or YAML.",
      "Fix the syntax error that the parser reports in the exception's cause, then load the definition again."),

  /**
   * A definition source (file, classpath resource, or extension) could not be resolved to a
   * readable definition. HTTP 400.
   */
  UNREADABLE_DEFINITION_SOURCE(
      "DB-SAGA-10005",
      Category.USER_ERROR,
      "Saga definition source cannot be read",
      ErrorMetadataSchema.of("source"),
      "A definition file or classpath resource cannot be found or read, or its name does not end in `.json`, `.yaml`, or `.yml`.",
      "Check that the file or resource exists, can be read, and has a supported extension."),

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
      "A class-based step cannot be loaded or created: the class is not on the classpath, does not implement `Step` or `TccStep`, or its constructor cannot be called or fails. The `detail` metadata says which.",
      "Fix the step class as the detail describes, and make sure it is on the classpath."),

  /**
   * A definition names a class-based step, which a saga server cannot run: it executes declarative
   * definitions only. HTTP 400.
   */
  STEP_CLASS_NOT_SUPPORTED_ON_SERVER(
      "DB-SAGA-10007",
      Category.USER_ERROR,
      "Class-based step is not supported on the server",
      ErrorMetadataSchema.of("saga_name", "step_name"),
      "A definition uses a class-based step. The server runs only declarative service steps.",
      "Replace the step with a declarative service step, or run the engine embedded in your application."),

  /**
   * A step needed an HTTP endpoint, but the orchestrator has no matching registration: none
   * registered, the name not found, or several registered without a qualifier. HTTP 400.
   */
  HTTP_ENDPOINT_LOOKUP_FAILED(
      "DB-SAGA-10008",
      Category.USER_ERROR,
      "HTTP endpoint lookup failed",
      ErrorMetadataSchema.of("detail"),
      "A step needs an HTTP endpoint, but none matches: no endpoint is registered, none has the requested name, or several are registered and the step does not name one.",
      "Register the endpoint, fix its name, or select one by annotating the `SagaHttpClient` parameter with `@Named`."),

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
      "A declarative service step breaks a validation rule, such as a missing or malformed call field. The `detail` metadata names the rule.",
      "Fix the step as the detail describes, then load the definition again."),

  // ── Auth (101xx) ─────────────────────────────────────────────────────
  /** The request presented no valid credential. HTTP 401. */
  UNAUTHENTICATED(
      "DB-SAGA-10101",
      Category.USER_ERROR,
      "Authentication required",
      ErrorMetadataSchema.none(),
      "The request has no valid credential, or the credential has expired.",
      "Send a valid API key or bearer token, then send the request again."),

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
      "The caller is authenticated but does not have the role the operation requires. The response does not name the role; the server's audit log does.",
      "Ask an administrator for the role that the operation requires."),

  // ── Not-found (102xx) ────────────────────────────────────────────────
  /** No saga instance exists with the given id. HTTP 404. */
  SAGA_NOT_FOUND(
      "DB-SAGA-10201",
      Category.USER_ERROR,
      "Saga not found",
      ErrorMetadataSchema.of("saga_id"),
      "No saga exists with the given ID.",
      "Check the saga ID. The saga may never have been started, or retention may have removed it."),

  /** No saga definition is registered under the given name. HTTP 404. */
  SAGA_DEFINITION_NOT_FOUND(
      "DB-SAGA-10202",
      Category.USER_ERROR,
      "Saga definition not found",
      ErrorMetadataSchema.of("saga_name"),
      "No saga definition has the given name.",
      "Check the definition name, or add the definition to the server."),

  /** The saga definition exists, but not at the requested version. HTTP 404. */
  SAGA_DEFINITION_VERSION_NOT_FOUND(
      "DB-SAGA-10203",
      Category.USER_ERROR,
      "Saga definition version not found",
      ErrorMetadataSchema.of("saga_name", "version"),
      "The saga definition exists, but not at the requested version.",
      "Use a version that exists, or add the requested version."),

  /**
   * The request path or method matches no registered REST route. Only a remote caller can produce
   * it; the embedded engine has no routes. HTTP 404.
   */
  ENDPOINT_NOT_FOUND(
      "DB-SAGA-10204",
      Category.USER_ERROR,
      "No such endpoint",
      ErrorMetadataSchema.of("detail"),
      "No REST route matches the request's method and path.",
      "Check the request's method and path."),

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
      "A saga with the given ID already exists.",
      "Use a different ID, or get the existing saga's state."),

  /**
   * A definition is already registered under this name and version with different content; bump the
   * version instead of re-registering under the same one. HTTP 409.
   */
  SAGA_DEFINITION_VERSION_CONTENT_CONFLICT(
      "DB-SAGA-10302",
      Category.USER_ERROR,
      "Definition version is already registered with different content",
      ErrorMetadataSchema.of("saga_name", "version"),
      "A definition with this name and version is already registered, with different content.",
      "Give the changed definition a new version."),

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
      "The saga's status does not allow the operation, such as force-completing a saga that is not escalated.",
      "Get the saga's current status, and use an operation that status allows."),

  /**
   * The saga is waiting on an asynchronous callback and resolves through that callback or its
   * timeout, not through admin action. HTTP 422.
   */
  SAGA_PARKED(
      "DB-SAGA-10402",
      Category.USER_ERROR,
      "Saga is parked and cannot be resumed automatically",
      ErrorMetadataSchema.of("saga_id"),
      "The saga is waiting for an asynchronous step. It continues when the participant reports the step's result or the step times out, not through an operator action.",
      "Wait for the participant's report or the step's timeout."),

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
      "The definition is registered, but this server does not serve it: either it was retired by removing its file, or this server has not yet loaded a newly added one. Sagas already running under it finish normally.",
      "If the definition was retired, stop starting it. If it was just added, retry; the server serves it once it loads the new configuration."),

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
      "Another server or an operator changed the saga at the same time.",
      "Retry the operation. This is usually transient."),

  /** A transient failure occurred while accessing the underlying store. */
  PERSISTENCE_STORE_UNAVAILABLE(
      "DB-SAGA-20002",
      Category.RETRYABLE_SERVER_ERROR,
      "Underlying store is temporarily unavailable",
      ErrorMetadataSchema.none(),
      "The database is temporarily unavailable.",
      "Retry the operation. If it keeps failing, check the database."),

  /**
   * The saga service, or one of its upstream dependencies such as the identity provider, could not
   * fulfil the request; the failure is transient.
   */
  SERVICE_UNAVAILABLE(
      "DB-SAGA-20003",
      Category.RETRYABLE_SERVER_ERROR,
      "Service temporarily unavailable",
      ErrorMetadataSchema.none(),
      "The server, or a service it depends on such as the identity provider, is temporarily unavailable.",
      "Retry with backoff."),

  /** The request exceeded the server's configured rate limit for this operation. */
  RATE_LIMIT_EXCEEDED(
      "DB-SAGA-20004",
      Category.RETRYABLE_SERVER_ERROR,
      "Rate limit exceeded",
      ErrorMetadataSchema.none(),
      "The caller has started more sagas than the server's rate limit allows.",
      "Retry with backoff, waiting at least as long as the server advises when it does: `Retry-After` on REST, `RetryInfo` on gRPC."),

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
      "The server stopped the operation partway through, usually because it is shutting down.",
      "Retry the operation. Another server, or this one after it restarts, handles it."),

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
      "The server is running as many sagas as its limit allows, so it refused to start another. Nothing was stored, so the saga ID is still free. Unlike `DB-SAGA-20004`, this limit is shared by all callers.",
      "Retry with backoff and jitter, waiting at least as long as the server advises when it does: `Retry-After` on REST, `RetryInfo` on gRPC. If refusals continue, review the server's limits and the capacity of the services it calls."),

  /**
   * An async-step callback arrived before the saga recorded the park it answers, so acking it now
   * would lose it. Retryable: the park is about to be recorded, and the same callback then
   * completes the step. HTTP 503 with {@code Retry-After}.
   */
  SAGA_STEP_NOT_PARKED(
      "DB-SAGA-20007",
      Category.RETRYABLE_SERVER_ERROR,
      "The step has not finished parking yet",
      ErrorMetadataSchema.of("saga_id", "step_name"),
      "A participant reported an asynchronous step's result before the server recorded that the saga is waiting for it, which can happen when the participant reports immediately. The report was not applied.",
      "Send the same report again with backoff, waiting at least as long as `Retry-After` says when it is present."),

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
      "The server returned a retryable error code that this version of the client SDK does not know, usually because the server is newer.",
      "Retry with backoff, and upgrade the client SDK."),

  // ── NON_RETRYABLE_SERVER_ERROR (3xxxx) ───────────────────────────────
  /** JSON serialization of an event payload failed; typically an engine bug. */
  PERSISTENCE_SERIALIZATION_FAILED(
      "DB-SAGA-30001",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Failed to serialize event payload",
      ErrorMetadataSchema.none(),
      "The server could not convert saga data to JSON to store it.",
      "Contact your administrator with the error details. This usually indicates a bug."),

  /**
   * Stored data could not be deserialized, possibly because the schema drifted between the writer
   * and the reader.
   */
  PERSISTENCE_DESERIALIZATION_FAILED(
      "DB-SAGA-30002",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Failed to deserialize event payload or definition",
      ErrorMetadataSchema.none(),
      "Stored saga data or a stored definition could not be read, possibly because an incompatible version wrote it.",
      "Check that every server runs a compatible version, and contact your administrator with the error details."),

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
      "The step exceeded its timeout before returning a result.",
      "Increase the step's timeout, or make the step faster."),

  /**
   * Reserved, not yet produced, for the same reason as {@link #STEP_TIMEOUT}: a step threw a non-
   * retryable failure, such as a business-rule rejection or a 4xx from a declarative service step.
   */
  STEP_USER_FAILURE(
      "DB-SAGA-30004",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Step reported a user failure",
      ErrorMetadataSchema.of("step_name", "step_index"),
      "The step failed permanently, such as with a business-rule rejection or a 4xx response from a service step.",
      "Check the step's failure detail."),

  /**
   * A step's compensation action threw. The saga stays {@code COMPENSATING} and recovery retries
   * the compensation; one stuck past the escalation grace period is escalated for manual
   * intervention.
   */
  COMPENSATION_FAILED(
      "DB-SAGA-30005",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Compensation of step failed",
      ErrorMetadataSchema.of("step_name", "step_index"),
      "A step's compensation failed.",
      "Check the participant's compensation. Recovery retries it, and escalates the saga if it keeps failing past the escalation grace period."),

  /**
   * The engine hit an unexpected internal error; the catch-all for a server fault no other code
   * describes.
   */
  INTERNAL_ERROR(
      "DB-SAGA-39999",
      Category.NON_RETRYABLE_SERVER_ERROR,
      "Internal error",
      ErrorMetadataSchema.none(),
      "The server hit an unexpected error.",
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
      "The client could not reach the server.",
      "Check the server address and the network, then retry."),

  /** The request did not complete before its deadline. */
  REQUEST_TIMEOUT(
      "DB-SAGA-40002",
      Category.CLIENT_ERROR,
      "The request to the saga server timed out",
      ErrorMetadataSchema.none(),
      "The request did not finish before its deadline.",
      "Retry the request, or set a longer deadline."),

  /**
   * The caller cancelled the operation (a thread interrupt, a future cancellation, or client
   * shutdown) before the RPC completed.
   */
  REQUEST_ABORTED(
      "DB-SAGA-40003",
      Category.CLIENT_ERROR,
      "The request was aborted before it could complete",
      ErrorMetadataSchema.none(),
      "The application cancelled the request before it finished, by interrupting the calling thread, cancelling its future, or closing the client.",
      "If the cancellation was not intended, avoid interrupting or cancelling the call, then retry."),

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
      "The response carried a gRPC status but no ScalarDB Saga error. It usually comes from the gRPC runtime itself, such as a message-size limit, or from a proxy between the client and the server.",
      "Check the status in the `server_value` metadata and the exception's cause. Upgrading the client SDK does not help."),

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
      "The saga did not finish within the client's wait limit. Usually it is still running and finishes on its own; but if the limit expired while the start was still being retried, the saga may not exist.",
      "Check the saga by the ID in the `saga_id` metadata. If it is not found, start it again with that same ID, never a new one, because an earlier attempt may still arrive."),

  /**
   * The server sent an error-code token or wire enum value this client SDK has no mapping for;
   * usually the server is newer than the client.
   */
  UNRECOGNIZED_SERVER_ERROR(
      "DB-SAGA-49999",
      Category.CLIENT_ERROR,
      "The server returned a value this client does not recognize",
      ErrorMetadataSchema.of("server_value"),
      "The server returned an error code or value that this version of the client SDK does not know, usually because the server is newer.",
      "Upgrade the client SDK to a version that matches the server."),
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
