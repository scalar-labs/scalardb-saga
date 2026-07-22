package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.daemon.security.SagaAuthUnavailableException;
import com.scalar.db.saga.daemon.security.SagaAuthenticationException;
import com.scalar.db.saga.daemon.security.SagaAuthorizationException;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import io.javalin.Javalin;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps exceptions to HTTP responses with a consistent JSON error body.
 *
 * <p>Client-facing exceptions (not-found, already-exists, invalid request) are mapped to specific
 * 4xx codes with daemon-owned messages. A transient persistence failure or an unavailable auth
 * provider maps to {@code 503}; a permanent persistence failure maps to {@code 500}, like the
 * catch-all. Everything else falls through to a generic {@code 500}: the real exception is logged
 * server-side and the response carries no internal detail — only the daemon's own messages are ever
 * returned to a caller.
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
    app.exception(
        SagaNotFoundException.class,
        (e, ctx) ->
            ctx.status(404)
                .json(error("SAGA_NOT_FOUND", "No saga found with id '" + e.getSagaId() + "'")));
    app.exception(
        SagaDefinitionNotFoundException.class,
        (e, ctx) ->
            ctx.status(404).json(error("SAGA_DEFINITION_NOT_FOUND", definitionNotFoundMessage(e))));
    app.exception(
        InvalidRequestException.class,
        (e, ctx) -> ctx.status(400).json(error("BAD_REQUEST", e.getMessage())));
    // Authentication failure: the credential is missing/invalid. Log at debug — probing traffic can
    // make this frequent — and return a generic 401 without echoing why the credential was
    // rejected.
    app.exception(
        SagaAuthenticationException.class,
        (e, ctx) -> {
          logger.debug(
              "Authentication failed on {} {}: {}", ctx.method(), ctx.path(), e.getMessage());
          ctx.status(401).json(error("UNAUTHENTICATED", "Authentication required"));
        });
    // Authorization failure: a known caller lacks the required role. Log the principal + required
    // role for the audit trail, and return a generic 403.
    app.exception(
        SagaAuthorizationException.class,
        (e, ctx) -> {
          logger.info(
              "Authorization denied on {} {}: caller '{}' lacks role {}",
              ctx.method(),
              ctx.path(),
              e.getPrincipal(),
              e.getRequiredRole().wireName());
          ctx.status(403).json(error("FORBIDDEN", "Insufficient permissions"));
        });
    // Authentication could not be completed because the provider is unavailable (e.g. the JWKS
    // endpoint is unreachable) — a transient upstream outage, not a bad credential. Log it and
    // return a retryable 503, mirroring a persistence failure.
    app.exception(
        SagaAuthUnavailableException.class,
        (e, ctx) -> {
          logger.error("Authentication provider unavailable on {} {}", ctx.method(), ctx.path(), e);
          ctx.status(503).json(error("UNAVAILABLE", "Service temporarily unavailable"));
        });
    app.exception(
        RateLimitExceededException.class,
        (e, ctx) -> {
          logger.debug(
              "Rate limit exceeded on {} {}: {}", ctx.method(), ctx.path(), e.getMessage());
          ctx.status(429).json(error("TOO_MANY_REQUESTS", "Rate limit exceeded"));
        });
    // A client-supplied value the engine rejects (e.g. an invalid saga id or an unsupported input
    // value type) surfaces as IllegalArgumentException — a client error. Map it to 400 with a
    // generic, daemon-owned message rather than echoing the engine's wording.
    app.exception(
        IllegalArgumentException.class,
        (e, ctx) -> ctx.status(400).json(error("BAD_REQUEST", "Invalid request parameter")));
    // An async-callback request with a missing or invalid HMAC token. The response is deliberately
    // generic (does not distinguish missing from invalid) so it is not an oracle.
    app.exception(
        CallbackAuthException.class,
        (e, ctx) ->
            ctx.status(401).json(error("UNAUTHORIZED", "Invalid or missing callback token")));
    app.exception(
        SagaAlreadyExistsException.class,
        (e, ctx) -> {
          Map<String, Object> body = new LinkedHashMap<>();
          body.put("error", "SAGA_ALREADY_EXISTS");
          body.put("message", "A saga already exists with id '" + e.getSagaId() + "'");
          body.put("sagaId", e.getSagaId());
          body.put("existing", SagaSnapshotResponse.from(e.getExisting()));
          ctx.status(409).json(body);
        });
    // An admin mutation on a saga in the wrong state (e.g. recovering an escalated saga, resetting
    // a
    // non-escalated one, or acting on a parked one). A precondition failure, not a transient one,
    // so
    // 422 rather than 409 — retrying the identical request will fail identically. The machine-
    // readable code distinguishes the reason without parsing the message.
    app.exception(
        SagaStatePreconditionException.class,
        (e, ctx) -> {
          // The machine-readable code (SAGA_WRONG_STATE / SAGA_PARKED) is the contract; the message
          // is daemon-owned rather than the core exception's, and the caller can GET the saga for
          // its actual state.
          Map<String, Object> body =
              error(e.getCode().name(), "The saga is not in a state that allows this operation");
          body.put("sagaId", e.getSagaId());
          ctx.status(422).json(body);
        });
    // An admin mutation (or a concurrent recovery) lost the compare-and-set on the saga's state — a
    // transient race, so 409, and a retry may now succeed against the new state.
    app.exception(
        SagaConcurrentModificationException.class,
        (e, ctx) ->
            ctx.status(409)
                .json(error("SAGA_CONFLICT", "The saga was concurrently modified; retry")));
    // A transient store failure is retryable (503); a permanent one (e.g. a serialization or parse
    // error) is not — surface it as 500 so the client does not retry it futilely.
    app.exception(
        SagaPersistenceException.class,
        (e, ctx) -> {
          if (e.isRetryable()) {
            logger.error("Transient persistence error on {} {}", ctx.method(), ctx.path(), e);
            ctx.status(503).json(error("UNAVAILABLE", "Service temporarily unavailable"));
          } else {
            logger.error("Permanent persistence error on {} {}", ctx.method(), ctx.path(), e);
            ctx.status(500).json(error("INTERNAL", "Internal server error"));
          }
        });
    // Catch-all: never leak an unmapped exception's message; log it and return a generic 500.
    app.exception(
        Exception.class,
        (e, ctx) -> {
          logger.error("Unhandled error on {} {}", ctx.method(), ctx.path(), e);
          ctx.status(500).json(error("INTERNAL", "Internal server error"));
        });
  }

  private static String definitionNotFoundMessage(SagaDefinitionNotFoundException e) {
    String message = "No saga definition registered with name '" + e.getSagaName() + "'";
    String version = e.getVersion();
    return version == null ? message : message + " (version '" + version + "')";
  }

  private static Map<String, Object> error(String code, @Nullable String message) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("error", code);
    body.put("message", message == null ? "" : message);
    return body;
  }
}
