package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.time.Clock;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Registers the async-callback completion route:
 *
 * <ul>
 *   <li>{@code POST /sagas/{id}/steps/{stepName}/complete?token=&iat=} — a participant reports the
 *       result of an async (parked) step. The daemon verifies the per-step HMAC callback token,
 *       then resumes the saga from the next step via {@link DefaultSagaOrchestrator#completeStep}.
 * </ul>
 *
 * <p><b>Auth.</b> This route is authenticated by the HMAC callback token, <em>not</em> by the
 * caller-facing auth (e.g. JWT) that guards the other endpoints — a participant service holds a
 * signed callback URL, not a user credential. {@link #PATH} is exposed as the single contract by
 * which a future auth layer exempts this route from that auth. An optional {@code iat} TTL
 * (configured via {@code register}) additionally rejects a token older than a maximum age, so a
 * leaked callback URL is not a non-expiring credential.
 *
 * <p><b>Idempotency.</b> A callback for a saga that is no longer {@code WAITING} — a duplicate
 * callback (a prior one already resumed it) or a late one (the timeout sweep already resolved it) —
 * is a no-op that returns the saga's current state rather than an error, so a participant that
 * retries its callback sees a consistent {@code 200}.
 */
public final class CallbackResource {

  /**
   * The callback route path (a Javalin path-parameter pattern). Exposed as a constant so a future
   * auth layer can exempt this HMAC-authenticated route from caller-facing auth.
   */
  public static final String PATH = "/sagas/{id}/steps/{stepName}/complete";

  /**
   * A callback token is the lowercase-hex HMAC-SHA256 output: exactly 64 chars of {@code [0-9a-f]}.
   */
  private static final Pattern TOKEN_PATTERN = Pattern.compile("[0-9a-f]{64}");

  private CallbackResource() {}

  /**
   * Registers the callback route on the given app.
   *
   * @param app the Javalin app
   * @param orchestrator the orchestrator whose {@link DefaultSagaOrchestrator#completeStep} resumes
   *     the parked saga
   * @param callbackSecret the HMAC secret the callback token is verified against
   * @param maxAgeSeconds the {@code iat} TTL in seconds; a token older than this is rejected as
   *     expired. {@code 0} disables the check
   * @param clock the clock used to age the token's {@code iat} against {@code maxAgeSeconds}
   */
  public static void register(
      Javalin app,
      DefaultSagaOrchestrator orchestrator,
      String callbackSecret,
      long maxAgeSeconds,
      Clock clock) {
    app.post(
        PATH,
        ctx -> {
          String sagaId = ctx.pathParam("id");
          String stepName = ctx.pathParam("stepName");
          verifyToken(ctx, callbackSecret, maxAgeSeconds, clock, sagaId, stepName);
          Map<String, Object> output = parseOutput(ctx);
          SagaStateSnapshot snapshot = complete(orchestrator, sagaId, stepName, output);
          ctx.status(200).json(SagaSnapshotResponse.from(snapshot));
        });
  }

  /**
   * Verifies the HMAC token over the data produced by {@link HmacUtils#callbackSignedData}. Both
   * {@code token} and {@code iat} query parameters are required (the token is computed over {@code
   * iat}, so it cannot be checked without it). The token must match the lowercase-hex HMAC-SHA256
   * shape before the signature is checked, so a malformed value is rejected without computing an
   * HMAC. When {@code maxAgeSeconds} is positive, an authenticated token older than that is
   * rejected as expired. Any missing, malformed, non-matching, or expired value throws {@link
   * CallbackAuthException} → {@code 401}.
   */
  private static void verifyToken(
      Context ctx,
      String callbackSecret,
      long maxAgeSeconds,
      Clock clock,
      String sagaId,
      String stepName) {
    String token = ctx.queryParam("token");
    String iat = ctx.queryParam("iat");
    if (token == null || token.isBlank() || iat == null || iat.isBlank()) {
      throw new CallbackAuthException("missing callback token or iat");
    }
    if (!TOKEN_PATTERN.matcher(token).matches()) {
      throw new CallbackAuthException("malformed callback token");
    }
    String data = HmacUtils.callbackSignedData(sagaId, stepName, iat);
    if (!HmacUtils.verify(callbackSecret, data, token)) {
      throw new CallbackAuthException("invalid callback token");
    }
    if (maxAgeSeconds > 0) {
      requireFresh(iat, maxAgeSeconds, clock);
    }
  }

  /**
   * Enforces the {@code iat} TTL: rejects an already-authenticated token whose issue time is more
   * than {@code maxAgeSeconds} old. Because the signature has been verified, {@code iat} is the
   * value the daemon minted; a value that does not parse as epoch seconds is treated as malformed.
   */
  private static void requireFresh(String iat, long maxAgeSeconds, Clock clock) {
    long issuedAtEpochSecond;
    try {
      issuedAtEpochSecond = Long.parseLong(iat);
    } catch (NumberFormatException e) {
      throw new CallbackAuthException("malformed callback iat");
    }
    if (clock.instant().getEpochSecond() - issuedAtEpochSecond > maxAgeSeconds) {
      throw new CallbackAuthException("expired callback token");
    }
  }

  /**
   * Parses the JSON request body into the step's output map. An empty body is a step that completed
   * with no output; a malformed body maps to {@code 400}.
   */
  private static Map<String, Object> parseOutput(Context ctx) {
    String body = ctx.body();
    if (body.isBlank()) {
      return Map.of();
    }
    Map<String, Object> output;
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> parsed = ctx.bodyAsClass(Map.class);
      output = parsed;
    } catch (Exception e) {
      throw new InvalidRequestException("malformed callback body");
    }
    // A body of the JSON null literal deserializes to null without throwing; treat it as no output.
    return output == null ? Map.of() : output;
  }

  /**
   * Resumes the parked step, mapping the "no longer WAITING" cases to an idempotent snapshot read:
   * a duplicate callback ({@link IllegalStateException} — already resumed) or a callback that lost
   * the race to the timeout sweep ({@link SagaConcurrentModificationException}) returns the saga's
   * current state instead of failing. A wrong step name ({@link IllegalArgumentException}) and an
   * unknown saga ({@code SagaNotFoundException}) propagate to the error mapper (400 / 404).
   */
  private static SagaStateSnapshot complete(
      DefaultSagaOrchestrator orchestrator,
      String sagaId,
      String stepName,
      Map<String, Object> output) {
    try {
      return orchestrator.completeStep(sagaId, stepName, output);
    } catch (IllegalStateException | SagaConcurrentModificationException e) {
      return orchestrator.getStateSnapshot(sagaId);
    }
  }
}
