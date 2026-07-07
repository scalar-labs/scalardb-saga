package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.daemon.security.SagaAuthenticationException;
import com.scalar.db.saga.daemon.security.SagaAuthorizationException;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
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
 * 4xx codes with daemon-owned messages. A persistence failure maps to {@code 503}. Everything else
 * falls through to a generic {@code 500}: the real exception is logged server-side and the response
 * carries no internal detail — only the daemon's own messages are ever returned to a caller.
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
    app.exception(
        SagaPersistenceException.class,
        (e, ctx) -> {
          logger.error("Persistence error on {} {}", ctx.method(), ctx.path(), e);
          ctx.status(503).json(error("UNAVAILABLE", "Service temporarily unavailable"));
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
