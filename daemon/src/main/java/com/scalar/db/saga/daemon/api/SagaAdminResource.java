package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaOperation;
import com.scalar.db.saga.daemon.security.SagaSecurityHandler;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.OperatorContext;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Registers the Admin API REST endpoints — the operational control plane over the daemon's saga
 * engine:
 *
 * <ul>
 *   <li>{@code GET /sagas} — list sagas, filtered by status and an {@code updatedAt} window, paged.
 *       Global enumeration across applications, so an operator (ADMIN) action, not an application
 *       read — a saga carries no application identity to scope a list to.
 *   <li>{@code POST /sagas/{id}/recover} — recover a stuck saga (the engine's pivot decides
 *       compensate vs resume; the operator never does)
 *   <li>{@code POST /sagas/{id}/force-complete} — force an escalated saga to {@code COMPLETED}
 *   <li>{@code POST /sagas/{id}/reset} — reset one escalated saga back into recovery
 *   <li>{@code POST /admin/reset-escalated} — bulk-reset escalated sagas in a window
 * </ul>
 *
 * <p>Inspecting one saga's detail and timeline is an application self-service read, not an operator
 * action, so it lives on {@code SagaResource} ({@code GET /sagas/{id}/detail}), not here.
 *
 * <p><b>Operator identity is server-injected.</b> Every mutation is attributed to the authenticated
 * caller — read from the request's {@link SagaIdentity}, never from the body — so a caller cannot
 * forge who acted. Listing needs no operator, so it uses the orchestrator's embedded admin view;
 * each mutation builds a per-request admin view bound to that request's identity (see {@link
 * DefaultSagaOrchestrator#adminService(OperatorContext, long)}).
 *
 * <p><b>Wrong-state and races.</b> A mutation on a saga in the wrong state (e.g. recovering an
 * escalated saga, or resetting a non-escalated one) surfaces as {@code 422}; losing the
 * compare-and-set to a concurrent admin or recovery action surfaces as {@code 409}. Both are mapped
 * in {@link ErrorMapper}.
 *
 * <p><b>Bounded drive.</b> A single-saga {@code recover}/{@code reset} drives the saga inline. The
 * drive is bounded by the daemon's request budget: past the bound the durable transition is already
 * recorded and the response carries the saga's current (possibly still-running) state, with the
 * recovery loop finishing the rest — the same "still processing" contract a bounded synchronous
 * start uses.
 */
public final class SagaAdminResource {

  private SagaAdminResource() {}

  /**
   * Registers the admin routes on the given app.
   *
   * @param app the Javalin app
   * @param orchestrator the orchestrator whose admin control plane the endpoints drive
   * @param driveDeadlineMillis the bound on a single-saga inline drive ({@code 0} = unbounded)
   */
  public static void register(
      Javalin app, DefaultSagaOrchestrator orchestrator, long driveDeadlineMillis) {
    app.get("/sagas", ctx -> list(ctx, orchestrator), SagaOperation.LIST_SAGAS);
    app.post(
        "/sagas/{id}/recover",
        ctx -> recover(ctx, orchestrator, driveDeadlineMillis),
        SagaOperation.RECOVER_SAGA);
    app.post(
        "/sagas/{id}/force-complete",
        ctx -> forceComplete(ctx, orchestrator, driveDeadlineMillis),
        SagaOperation.FORCE_COMPLETE);
    app.post(
        "/sagas/{id}/reset",
        ctx -> reset(ctx, orchestrator, driveDeadlineMillis),
        SagaOperation.RESET_ESCALATED);
    app.post(
        "/admin/reset-escalated",
        ctx -> bulkReset(ctx, orchestrator, driveDeadlineMillis),
        SagaOperation.RESET_ESCALATED);
  }

  // --- reads (no operator, no drive) -----------------------------------------

  private static void list(Context ctx, DefaultSagaOrchestrator orchestrator) {
    SagaQuery query = parseQuery(ctx);
    ctx.status(200).json(SagaListResponse.from(orchestrator.adminService().listSagas(query)));
  }

  // --- mutations (operator-attributed, bounded drive) ------------------------

  private static void recover(
      Context ctx, DefaultSagaOrchestrator orchestrator, long driveDeadlineMillis) {
    String reason = bodyReason(ctx);
    SagaAdminService admin = admin(ctx, orchestrator, driveDeadlineMillis);
    SagaStateSnapshot snapshot = admin.recoverSaga(ctx.pathParam("id"), reason);
    ctx.status(driveOutcomeStatus(snapshot)).json(SagaSnapshotResponse.from(snapshot));
  }

  private static void forceComplete(
      Context ctx, DefaultSagaOrchestrator orchestrator, long driveDeadlineMillis) {
    String reason = bodyReason(ctx);
    SagaAdminService admin = admin(ctx, orchestrator, driveDeadlineMillis);
    SagaStateSnapshot snapshot = admin.forceComplete(ctx.pathParam("id"), reason);
    // force-complete is terminal (ESCALATED -> COMPLETED), so always a settled 200.
    ctx.status(200).json(SagaSnapshotResponse.from(snapshot));
  }

  private static void reset(
      Context ctx, DefaultSagaOrchestrator orchestrator, long driveDeadlineMillis) {
    String reason = bodyReason(ctx);
    SagaAdminService admin = admin(ctx, orchestrator, driveDeadlineMillis);
    SagaStateSnapshot snapshot = admin.resetEscalated(ctx.pathParam("id"), reason);
    ctx.status(driveOutcomeStatus(snapshot)).json(SagaSnapshotResponse.from(snapshot));
  }

  private static void bulkReset(
      Context ctx, DefaultSagaOrchestrator orchestrator, long driveDeadlineMillis) {
    BulkResetRequest request = body(ctx, BulkResetRequest.class);
    String reason = request.requireReason();
    SagaAdminService admin = admin(ctx, orchestrator, driveDeadlineMillis);
    // The bulk sweep un-escalates each row and hands the drive to the recovery loop, so it returns
    // an itemized result rather than a single snapshot; always 200.
    ctx.status(200).json(ResetResultResponse.from(admin.resetEscalated(request.toQuery(), reason)));
  }

  // --- helpers ---------------------------------------------------------------

  /**
   * Builds the per-request admin view attributed to the authenticated caller. The admin routes are
   * role-gated, so the security handler has already resolved and stored the identity; a missing one
   * is a server wiring bug (the route was reached without authentication), surfaced as a {@code
   * 500}, not a client error.
   */
  private static SagaAdminService admin(
      Context ctx, DefaultSagaOrchestrator orchestrator, long driveDeadlineMillis) {
    SagaIdentity identity = ctx.attribute(SagaSecurityHandler.IDENTITY_ATTRIBUTE);
    if (identity == null) {
      throw new IllegalStateException(
          "no authenticated identity on an admin mutation; the route was reached unauthenticated");
    }
    OperatorContext operatorContext = identity::principal;
    return orchestrator.adminService(operatorContext, driveDeadlineMillis);
  }

  /** {@code 200} if the drive settled to a terminal state, else {@code 202} (still running). */
  private static int driveOutcomeStatus(SagaStateSnapshot snapshot) {
    return snapshot.getStatus().isTerminal() ? 200 : 202;
  }

  private static String bodyReason(Context ctx) {
    return body(ctx, InterventionRequest.class).requireReason();
  }

  private static <T> T body(Context ctx, Class<T> type) {
    T request;
    try {
      request = ctx.bodyAsClass(type);
    } catch (Exception e) {
      throw new InvalidRequestException("malformed request body");
    }
    // A body of the JSON null literal deserializes to null without throwing; reject it cleanly.
    if (request == null) {
      throw new InvalidRequestException("request body must not be null");
    }
    return request;
  }

  private static SagaQuery parseQuery(Context ctx) {
    SagaQuery.Builder builder = SagaQuery.newBuilder();
    String status = ctx.queryParam("status");
    if (status != null) {
      builder.status(parseStatus(status));
    }
    String updatedAfter = ctx.queryParam("updatedAfter");
    if (updatedAfter != null) {
      builder.updatedAfter(parseInstant(updatedAfter, "updatedAfter"));
    }
    String updatedBefore = ctx.queryParam("updatedBefore");
    if (updatedBefore != null) {
      builder.updatedBefore(parseInstant(updatedBefore, "updatedBefore"));
    }
    String pageSize = ctx.queryParam("pageSize");
    if (pageSize != null) {
      builder.pageSize(parsePageSize(pageSize)); // out-of-range -> IllegalArgumentException -> 400
    }
    String pageToken = ctx.queryParam("pageToken");
    if (pageToken != null) {
      builder.pageToken(pageToken);
    }
    return builder.build(); // an empty updatedAt window -> IllegalArgumentException -> 400
  }

  private static SagaStatus parseStatus(String value) {
    try {
      return SagaStatus.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new InvalidRequestException("'status' is not a valid saga status");
    }
  }

  private static Instant parseInstant(String value, String field) {
    try {
      return Instant.parse(value);
    } catch (DateTimeParseException e) {
      throw new InvalidRequestException("'" + field + "' is not a valid ISO-8601 instant");
    }
  }

  private static int parsePageSize(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new InvalidRequestException("'pageSize' is not an integer");
    }
  }
}
