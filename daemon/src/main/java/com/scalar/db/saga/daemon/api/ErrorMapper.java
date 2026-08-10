package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.daemon.security.SagaAuthUnavailableException;
import com.scalar.db.saga.daemon.security.SagaAuthenticationException;
import com.scalar.db.saga.daemon.security.SagaAuthorizationException;
import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.NotFoundResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps exceptions to HTTP responses with a consistent JSON error body.
 *
 * <p>Body shape:
 *
 * <pre>{@code
 * { "errorCode": "DB-SAGA-10201",
 *   "message":   "DB-SAGA-10201: Saga not found [saga_id=abc-123]",
 *   "metadata":  { "saga_id": "abc-123" } }
 * }</pre>
 *
 * <p><b>{@link SagaErrorCode} is the sole wire-facing source of truth.</b> Every response is
 * composed from the exception's {@link SagaErrorCode} + its schema-validated metadata via one
 * body-composition path. Log messages also derive from the enum ({@code e.getMessage()}), plus the
 * server-side-only {@code internalDetail} where the exception carries one — so log and wire never
 * drift.
 *
 * <p><b>Per-type explicit registration.</b> Every wire-facing exception has its own {@code
 * app.exception} handler; this file reads as a complete per-type dispatch table. The generic {@link
 * SagaRuntimeException} handler catches any future subclass that lacks a dedicated entry so the
 * fallback route is safe.
 *
 * <p><b>Route-level 404.</b> A request no route matches surfaces as Javalin's internal {@link
 * NotFoundResponse}, which Javalin's own handler would render with a default body — the one REST
 * response without an {@code errorCode}. Registering that exact type routes it through the same
 * body composition as everything else; handler-produced 404s (saga or definition not found) carry
 * their own typed exceptions and are unaffected.
 *
 * <p><b>Logging rule:</b> log per-type only when the operator would learn something the wire body
 * doesn't already show — i.e. the exception carries a server-side-only field ({@code
 * internalDetail}, principal, required role, or a cause chain with internal specifics). Everything
 * else — where the wire body already tells the whole story — is not logged: the client already saw
 * the error and a duplicate server-side log adds nothing. Severity: 5xx → {@code ERROR} (operator
 * must investigate); authorization denials → {@code INFO} (security audit); everything else that
 * logs → {@code DEBUG} (usually high-volume probing traffic).
 */
public final class ErrorMapper {

  private static final Logger logger = LoggerFactory.getLogger(ErrorMapper.class);

  private ErrorMapper() {}

  /**
   * Registers exception handlers on the given app.
   *
   * @param app the Javalin app
   */
  public static void register(Javalin app) {
    // ── Not found (404) ──────────────────────────────────────────────────
    app.exception(SagaNotFoundException.class, (e, ctx) -> respond(ctx, 404, e));
    app.exception(SagaDefinitionNotFoundException.class, (e, ctx) -> respond(ctx, 404, e));

    // ── Conflict (409) ───────────────────────────────────────────────────
    app.exception(SagaAlreadyExistsException.class, (e, ctx) -> respond(ctx, 409, e));
    app.exception(SagaConcurrentModificationException.class, (e, ctx) -> respond(ctx, 409, e));

    // ── Precondition failed (422) ────────────────────────────────────────
    app.exception(SagaStatePreconditionException.class, (e, ctx) -> respond(ctx, 422, e));

    // ── Bad request (400) ────────────────────────────────────────────────
    // One definition code is not a bad request: DEFINITION_VERSION_CONTENT_CONFLICT sits in the
    // conflict (103xx) sub-range, and the sub-range is a wire contract — the status must say 409
    // where the code says conflict. The other six definition codes are genuinely bad requests.
    app.exception(
        SagaDefinitionException.class,
        (e, ctx) ->
            respond(
                ctx,
                e.getErrorCode() == SagaErrorCode.DEFINITION_VERSION_CONTENT_CONFLICT ? 409 : 400,
                e));
    app.exception(SagaInvalidRequestException.class, (e, ctx) -> respond(ctx, 400, e));
    app.exception(SagaIllegalArgumentException.class, (e, ctx) -> respond(ctx, 400, e));
    // A client-supplied value the engine rejects surfaces as a stdlib IllegalArgumentException from
    // the engine sites not yet migrated to SagaIllegalArgumentException. Wrap it in the latter with
    // a fixed daemon-owned detail (do not echo the engine's wording) so it flows through the same
    // code path as every other exception. INVALID_ARGUMENT, not INVALID_REQUEST: the request
    // message was well-formed; a value inside it was rejected.
    app.exception(
        IllegalArgumentException.class,
        (e, ctx) ->
            respond(ctx, 400, new SagaIllegalArgumentException("invalid request parameter")));

    // ── Auth (401 / 403) ─────────────────────────────────────────────────
    app.exception(
        CallbackAuthException.class,
        (e, ctx) -> {
          // DEBUG: probing traffic can make this frequent; log the internal reason for triage.
          logger.debug(
              "{} on {} {}: {}", e.getMessage(), ctx.method(), ctx.path(), e.getInternalDetail());
          respond(ctx, 401, e);
        });
    app.exception(
        SagaAuthenticationException.class,
        (e, ctx) -> {
          // DEBUG: probing traffic can make this frequent.
          logger.debug(
              "{} on {} {}: {}", e.getMessage(), ctx.method(), ctx.path(), e.getInternalDetail());
          respond(ctx, 401, e);
        });
    app.exception(
        SagaAuthorizationException.class,
        (e, ctx) -> {
          // INFO: audit trail with principal + required role.
          logger.info(
              "{} on {} {}: caller '{}' lacks role {}",
              e.getMessage(),
              ctx.method(),
              ctx.path(),
              e.getPrincipal(),
              e.getRequiredRole().wireName());
          respond(ctx, 403, e);
        });

    // ── Rate limit (429) ─────────────────────────────────────────────────
    app.exception(
        RateLimitExceededException.class,
        (e, ctx) -> {
          logger.debug(
              "{} on {} {}: {}", e.getMessage(), ctx.method(), ctx.path(), e.getInternalDetail());
          respond(ctx, 429, e);
        });

    // ── Server errors (500 / 503) ────────────────────────────────────────
    // Identity-provider transient outage; log ERROR with the specific upstream failure.
    app.exception(
        SagaAuthUnavailableException.class,
        (e, ctx) -> {
          logger.error(
              "{} on {} {}: {}",
              e.getMessage(),
              ctx.method(),
              ctx.path(),
              e.getInternalDetail(),
              e);
          respond(ctx, 503, e);
        });
    // Transient store failure → 503 (retryable); permanent → 500 (do not retry).
    app.exception(
        SagaPersistenceException.class,
        (e, ctx) -> {
          int status = e.isRetryable() ? 503 : 500;
          logger.error("{} on {} {}", e.getMessage(), ctx.method(), ctx.path(), e);
          respond(ctx, status, e);
        });

    // ── Fallbacks ────────────────────────────────────────────────────────
    // Any SagaRuntimeException subclass that isn't listed above — sane default via category.
    app.exception(
        SagaRuntimeException.class,
        (e, ctx) -> {
          int status = statusForCategory(e.getErrorCode().category());
          if (status >= 500) {
            logger.error("{} on {} {}", e.getMessage(), ctx.method(), ctx.path(), e);
          }
          respond(ctx, status, e);
        });
    // Non-Saga catch-all: log and surface the enum's generic INTERNAL_ERROR.
    app.exception(
        Exception.class,
        (e, ctx) -> {
          logger.error("Unhandled error on {} {}", ctx.method(), ctx.path(), e);
          ctx.status(500).json(body(SagaErrorCode.INTERNAL_ERROR, ErrorMetadata.of()));
        });

    // ── Unmatched route (404) ────────────────────────────────────────────
    // Javalin reports a path (or method) no route matches by throwing NotFoundResponse internally,
    // so mapping that type gives the response a structured body like every other; without this
    // entry it would ship Javalin's default body, the one REST response without an errorCode.
    // INVALID_REQUEST, because only a remote caller can produce a request the daemon edge cannot
    // route. Handler-produced 404s (saga or definition not found) carry their own typed exceptions
    // and never reach this entry. Registering the exact type outranks Javalin's built-in
    // HttpResponseException handler, which resolves by nearest class.
    app.exception(
        NotFoundResponse.class,
        (e, ctx) ->
            ctx.status(404)
                .json(
                    body(
                        SagaErrorCode.INVALID_REQUEST,
                        ErrorMetadata.of(
                            "detail", "no such endpoint: " + ctx.method() + " " + ctx.path()))));
  }

  /** Writes the wire body for a {@link SagaRuntimeException} at the given HTTP status. */
  private static void respond(Context ctx, int status, SagaRuntimeException e) {
    ctx.status(status).json(body(e.getErrorCode(), e.getMetadata()));
  }

  /**
   * Category-based HTTP status — used only by the {@link SagaRuntimeException} fallback for an
   * unmapped subclass, so a newly-added exception still lands in a sensible bucket.
   */
  private static int statusForCategory(SagaErrorCode.Category c) {
    return switch (c) {
      case USER_ERROR -> 400;
      case RETRYABLE_SERVER_ERROR -> 503;
      case NON_RETRYABLE_SERVER_ERROR, CLIENT_ERROR -> 500;
    };
  }

  /** The one body-composition path: everything wire-facing goes through the enum. */
  private static Map<String, Object> body(SagaErrorCode code, Map<String, String> metadata) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("errorCode", code.code());
    body.put("message", code.buildMessage(metadata));
    body.put("metadata", metadata);
    return body;
  }
}
