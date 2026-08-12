package com.scalar.db.saga.server.api;

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
import com.scalar.db.saga.server.security.SagaAuthUnavailableException;
import com.scalar.db.saga.server.security.SagaAuthenticationException;
import com.scalar.db.saga.server.security.SagaAuthorizationException;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpResponseException;
import io.javalin.http.NotFoundResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps exceptions to HTTP responses with a consistent JSON error body — the REST analogue of the
 * gRPC transport's {@code GrpcErrorMapper}. <b>Keep in sync with {@code GrpcErrorMapper}</b> — both
 * translate the same exception hierarchy through the same {@link SagaErrorCode} vocabulary, and
 * each mapper's golden-table test pins its half of the agreement.
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
 * must investigate); authorization denials → {@code INFO} (security audit); the replaced bare
 * {@code IllegalArgumentException} → {@code WARN} (a misattributed server bug or a migration
 * candidate — evidence that must print at the production default); everything else that logs →
 * {@code DEBUG} (usually high-volume probing traffic).
 */
public final class ErrorMapper {

  private static final Logger logger = LoggerFactory.getLogger(ErrorMapper.class);

  /** Cap on the request line echoed by the unmatched-route 404 body. */
  private static final int MAX_ECHOED_REQUEST_LINE = 200;

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
    // One definition code is not a bad request: SAGA_DEFINITION_VERSION_CONTENT_CONFLICT sits in
    // the
    // conflict (103xx) sub-range, and the sub-range is a wire contract — the status must say 409
    // where the code says conflict. The other six definition codes are genuinely bad requests.
    app.exception(
        SagaDefinitionException.class,
        (e, ctx) ->
            respond(
                ctx,
                e.getErrorCode() == SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT
                    ? 409
                    : 400,
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
        (e, ctx) -> {
          // The engine's wording and cause are replaced on the wire, so log them: a server-side
          // bug surfacing as IllegalArgumentException (NumberFormatException, say) would otherwise
          // be reported to the caller as their fault with no evidence left anywhere. WARN, visible
          // at the production default: every hit is either a misattributed server bug or an
          // unmigrated caller-input site — the branch's shrink-to-zero to-do list.
          logger.warn(
              "Replacing a bare IllegalArgumentException on {} {}", ctx.method(), ctx.path(), e);
          respond(ctx, 400, new SagaIllegalArgumentException("invalid request parameter"));
        });

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
          // Whole seconds per RFC 9110, rounded up so a compliant client never retries before the
          // window actually resets.
          ctx.header("Retry-After", Long.toString((e.getRetryAfterMillis() + 999) / 1000));
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
    // ENDPOINT_NOT_FOUND (102xx): the sub-range is a wire contract, so a 404 status must carry a
    // not-found-family code. Handler-produced 404s (saga or definition not found) carry their own
    // typed exceptions and never reach this entry. Registering the exact type outranks Javalin's
    // built-in HttpResponseException handler, which resolves by nearest class.
    app.exception(
        NotFoundResponse.class,
        (e, ctx) ->
            ctx.status(404)
                .json(
                    body(
                        SagaErrorCode.ENDPOINT_NOT_FOUND,
                        ErrorMetadata.of(
                            "detail", "no such endpoint: " + boundedRequestLine(ctx)))));

    // ── Javalin-generated errors (the framework's own gatekeepers) ───────
    // Javalin produces responses of its own before our routes run — the request-size cap (413,
    // ContentTooLargeResponse, on by default at about 1 MB) is triggerable by any caller — and its
    // default bodies carry no errorCode. Routing the whole family through the standard
    // composition keeps the framework's status and adds ours: INVALID_REQUEST for the 4xx
    // request-shape statuses, INTERNAL_ERROR for 5xx. NotFoundResponse stays on its dedicated
    // ENDPOINT_NOT_FOUND handler above; Javalin resolves the nearest registered type.
    app.exception(
        HttpResponseException.class,
        (e, ctx) -> {
          int status = e.getStatus();
          if (status >= 500) {
            logger.error("Javalin-generated {} on {} {}", status, ctx.method(), ctx.path(), e);
            ctx.status(status).json(body(SagaErrorCode.INTERNAL_ERROR, ErrorMetadata.of()));
          } else {
            String message = e.getMessage();
            ctx.status(status)
                .json(
                    body(
                        SagaErrorCode.INVALID_REQUEST,
                        ErrorMetadata.of(
                            "detail",
                            message == null || message.isBlank()
                                ? "request rejected at the server edge"
                                : message)));
          }
        });
  }

  /**
   * The request line echoed by the unmatched-route 404, capped: this is the one response an
   * anonymous caller can shape without matching any route (auth and rate limiting are beforeMatched
   * handlers, which never run for it), so the echo is bounded here rather than only by Jetty's
   * request-line limit.
   */
  private static String boundedRequestLine(Context ctx) {
    String line = ctx.method() + " " + ctx.path();
    return line.length() <= MAX_ECHOED_REQUEST_LINE
        ? line
        : line.substring(0, MAX_ECHOED_REQUEST_LINE) + "...";
  }

  /** Writes the wire body for a {@link SagaRuntimeException} at the given HTTP status. */
  private static void respond(Context ctx, int status, SagaRuntimeException e) {
    // The exception constructor already built the wire message; reuse it rather than rebuilding.
    ctx.status(status).json(body(e.getErrorCode(), e.getMetadata(), e.getMessage()));
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
    return body(code, metadata, code.buildMessage(metadata));
  }

  private static Map<String, Object> body(
      SagaErrorCode code, Map<String, String> metadata, String message) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("errorCode", code.code());
    body.put("message", message);
    body.put("metadata", metadata);
    return body;
  }
}
