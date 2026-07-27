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
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPermissionDeniedException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import io.javalin.Javalin;
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
 * <p><b>{@link SagaErrorCode} is the sole wire-facing source of truth.</b> Every response — engine
 * exceptions, daemon-only exceptions, catch-all — is composed from a {@link SagaErrorCode} + its
 * schema-validated metadata via one path. The mapper carries no hand-authored code tokens or
 * message strings; changing a wire message is a single-line edit on the enum.
 *
 * <p>The per-type handlers exist only to (a) pick the HTTP status per exception type and (b) log at
 * the right severity with the right per-type context; the body composition is uniform.
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
    // The daemon's own edge validation — its message is daemon-authored, so it rides in `detail`.
    app.exception(
        InvalidRequestException.class,
        (e, ctx) ->
            ctx.status(400)
                .json(
                    body(
                        SagaErrorCode.INVALID_REQUEST,
                        ErrorMetadata.of("detail", nullToEmpty(e.getMessage())))));
    // A client-supplied value the engine rejects. Do not echo the engine's wording (may contain
    // internal detail); use a fixed daemon-owned literal in `detail`.
    app.exception(
        IllegalArgumentException.class,
        (e, ctx) ->
            ctx.status(400)
                .json(
                    body(
                        SagaErrorCode.INVALID_REQUEST,
                        ErrorMetadata.of("detail", "invalid request parameter"))));
    // Authentication failure: log at debug (probing traffic can be frequent); the response never
    // echoes why the credential was rejected — the enum's fixed message conveys "authenticate."
    app.exception(
        SagaAuthenticationException.class,
        (e, ctx) -> {
          logger.debug(
              "Authentication failed on {} {}: {}", ctx.method(), ctx.path(), e.getMessage());
          ctx.status(401).json(body(SagaErrorCode.UNAUTHENTICATED, ErrorMetadata.of()));
        });
    // Authorization failure: log the principal + required role for the audit trail; the response is
    // the enum's generic "permission denied."
    app.exception(
        SagaAuthorizationException.class,
        (e, ctx) -> {
          logger.info(
              "Authorization denied on {} {}: caller '{}' lacks role {}",
              ctx.method(),
              ctx.path(),
              e.getPrincipal(),
              e.getRequiredRole().wireName());
          ctx.status(403).json(body(SagaErrorCode.PERMISSION_DENIED, ErrorMetadata.of()));
        });
    // Authentication could not be completed because the provider is unavailable — a transient
    // upstream outage. Log it and return the enum's retryable "service unavailable."
    app.exception(
        SagaAuthUnavailableException.class,
        (e, ctx) -> {
          logger.error("Authentication provider unavailable on {} {}", ctx.method(), ctx.path(), e);
          ctx.status(503).json(body(SagaErrorCode.SERVICE_UNAVAILABLE, ErrorMetadata.of()));
        });
    app.exception(
        RateLimitExceededException.class,
        (e, ctx) -> {
          logger.debug(
              "Rate limit exceeded on {} {}: {}", ctx.method(), ctx.path(), e.getMessage());
          ctx.status(429).json(body(SagaErrorCode.RATE_LIMIT_EXCEEDED, ErrorMetadata.of()));
        });
    // Async-callback token missing/invalid. Semantically an auth failure; the response is
    // deliberately generic (does not distinguish missing from invalid) so it is not an oracle.
    app.exception(
        CallbackAuthException.class,
        (e, ctx) -> ctx.status(401).json(body(SagaErrorCode.UNAUTHENTICATED, ErrorMetadata.of())));

    // Every SagaRuntimeException — generic path. Javalin resolves the closest handler up the
    // hierarchy, so subclasses fall here unless they need a special-case handler (they don't).
    app.exception(
        SagaRuntimeException.class,
        (e, ctx) -> {
          int status = httpStatusForType(e);
          if (status >= 500) {
            logger.error("Server-side error on {} {}", ctx.method(), ctx.path(), e);
          }
          ctx.status(status).json(body(e.getErrorCode(), e.getMetadata()));
        });

    // Catch-all: never leak an unmapped exception; log it and surface the enum's generic INTERNAL.
    app.exception(
        Exception.class,
        (e, ctx) -> {
          logger.error("Unhandled error on {} {}", ctx.method(), ctx.path(), e);
          ctx.status(500).json(body(SagaErrorCode.INTERNAL_ERROR, ErrorMetadata.of()));
        });
  }

  /**
   * The per-type HTTP status. Types not listed fall back to the {@link SagaErrorCode.Category}
   * mapping so a newly-added subclass with a code still lands in a sensible bucket.
   */
  private static int httpStatusForType(SagaRuntimeException e) {
    if (e instanceof SagaNotFoundException || e instanceof SagaDefinitionNotFoundException) {
      return 404;
    }
    if (e instanceof SagaAlreadyExistsException) {
      return 409;
    }
    if (e instanceof SagaConcurrentModificationException) {
      return 409;
    }
    if (e instanceof SagaStatePreconditionException) {
      return 422;
    }
    if (e instanceof SagaDefinitionException) {
      return 400;
    }
    if (e instanceof SagaTimeoutException) {
      return 504;
    }
    if (e instanceof SagaUnauthenticatedException) {
      return 401;
    }
    if (e instanceof SagaPermissionDeniedException) {
      return 403;
    }
    if (e instanceof SagaUnavailableException) {
      return 503;
    }
    if (e instanceof SagaPersistenceException pe) {
      return pe.isRetryable() ? 503 : 500;
    }
    return switch (e.getErrorCode().category()) {
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

  private static String nullToEmpty(@org.jspecify.annotations.Nullable String s) {
    return s == null ? "" : s;
  }
}
