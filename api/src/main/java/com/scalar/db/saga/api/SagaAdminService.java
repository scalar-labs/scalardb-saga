package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;

/**
 * Operational control plane for a saga engine: list and inspect sagas, and un-stick or resolve the
 * ones that need an operator. Implemented in-process by {@code DefaultSagaAdminService} and, in the
 * future, by a remote admin client, so the same surface works embedded or against a saga server.
 * (No remote client is provided yet; a remote caller uses the generated gRPC admin stub directly.)
 *
 * <p><b>Direction-agnostic mutations.</b> The operator never chooses "compensate" vs. "resume
 * forward" — the engine decides from the saga's pivot, exactly as automatic recovery does. The
 * operator only chooses <em>whether</em> to let the engine continue ({@link #recoverSaga}, {@link
 * #resetEscalated}) or to override a stuck saga to done ({@link #forceComplete}). A caller supplies
 * a {@code reason} for audit; the operator identity is injected by the server, never passed in.
 *
 * <p><b>Rejections.</b> A mutation on a saga in a state the operation does not accept throws {@link
 * SagaStatePreconditionException} (wrong state — HTTP 422). Losing an optimistic-concurrency race
 * to a concurrent writer (another operator, or automatic recovery) throws {@link
 * SagaConcurrentModificationException} (HTTP 409). A missing saga throws {@link
 * SagaNotFoundException} (HTTP 404). A mutation that must drive the saga needs its definition; if
 * the registry no longer holds the one the saga was started with, it throws {@link
 * SagaDefinitionNotFoundException} (HTTP 404). Re-register the definition to make the saga
 * recoverable.
 */
public interface SagaAdminService extends AutoCloseable {

  // ---------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------

  /**
   * Lists saga state snapshots matching {@code query}, one page at a time. Drive pagination by the
   * returned {@link SagaPage#getNextPageToken()} until it is {@code null}.
   *
   * @param query the status/time filter, page size, and continuation token
   * @return a page of matching snapshots and a token for the next page
   * @throws IllegalArgumentException if the page token is malformed (the daemon maps this to 400)
   */
  SagaPage<SagaStateSnapshot> listSagas(SagaQuery query);

  // Inspecting one saga's detail and timeline is an application self-service read (diagnosing your
  // own saga's failure), not an operator intervention, so it lives on
  // SagaOrchestrator#getSagaDetail
  // rather than here. An operator holds ADMIN, which implies READ, so they reach it too.

  // ---------------------------------------------------------------------------
  // Mutations (operator interventions)
  // ---------------------------------------------------------------------------

  /**
   * Un-sticks a non-escalated stuck saga by driving it in the direction the engine would take —
   * compensate a pre-pivot failure, resume a post-pivot one — immediately, rather than waiting for
   * the recovery grace period. Accepts a {@code RUNNING} or {@code COMPENSATING} saga.
   *
   * <p>The drive runs synchronously on the calling thread, so the call blocks until the saga
   * completes, compensates, or parks.
   *
   * @param sagaId the saga instance ID
   * @param reason why the operator is intervening (recorded for audit; must be non-blank)
   * @return the saga's snapshot after the drive — its resulting status (e.g. {@code COMPLETED},
   *     {@code COMPENSATED}, still {@code COMPENSATING}, or {@code WAITING})
   * @throws SagaNotFoundException if no such saga exists
   * @throws SagaStatePreconditionException if the saga is {@code ESCALATED} (use {@link
   *     #resetEscalated} or {@link #forceComplete}), {@code WAITING}, or terminal
   * @throws SagaDefinitionNotFoundException if the saga's definition is no longer registered, so
   *     the engine cannot drive it
   * @throws SagaConcurrentModificationException if a concurrent writer changed the saga first
   */
  SagaStateSnapshot recoverSaga(String sagaId, String reason);

  /**
   * Overrides an {@code ESCALATED} saga to {@code COMPLETED} — "this actually succeeded, accept
   * it." The override is recorded with a distinct event type so it is never mistaken for a genuine
   * completion.
   *
   * @param sagaId the saga instance ID
   * @param reason why the operator is force-completing (recorded for audit; must be non-blank)
   * @return the saga's {@code COMPLETED} snapshot
   * @throws SagaNotFoundException if no such saga exists
   * @throws SagaStatePreconditionException if the saga is not {@code ESCALATED}
   * @throws SagaConcurrentModificationException if a concurrent writer changed the saga first
   */
  SagaStateSnapshot forceComplete(String sagaId, String reason);

  /**
   * Un-escalates a single {@code ESCALATED} saga and drives it in the direction the engine would
   * take (compensate or resume forward). Use this to triage one escalated saga back into the active
   * flow; use {@link #resetEscalated(SagaQuery, String)} to sweep many at once.
   *
   * <p>The drive runs synchronously on the calling thread, so the call blocks until the saga
   * completes, compensates, or parks.
   *
   * @param sagaId the saga instance ID
   * @param reason why the operator is un-escalating (recorded for audit; must be non-blank)
   * @return the saga's snapshot after the drive — its resulting status (e.g. {@code COMPLETED},
   *     {@code COMPENSATED}, still {@code COMPENSATING}, or {@code WAITING})
   * @throws SagaNotFoundException if no such saga exists
   * @throws SagaStatePreconditionException if the saga is not {@code ESCALATED}
   * @throws SagaDefinitionNotFoundException if the saga's definition is no longer registered, so
   *     the engine cannot drive it — the single-saga counterpart of the bulk form's {@link
   *     ResetResult.SkipReason#DEFINITION_NOT_FOUND}
   * @throws SagaConcurrentModificationException if a concurrent writer changed the saga first
   */
  SagaStateSnapshot resetEscalated(String sagaId, String reason);

  /**
   * Un-escalates every {@code ESCALATED} saga in one page of {@code query}, handing each to the
   * recovery loop to drive in the direction the engine would take — so the call returns without
   * blocking on the drives themselves. The query's {@code status} filter is fixed to {@code
   * ESCALATED}; supplying a conflicting status is an error. Drive the sweep by {@link
   * ResetResult#getNextPageToken()} until it is {@code null}.
   *
   * <p>Each row is un-escalated under its own optimistic-concurrency guard. A row that lost a race,
   * whose definition could not be resolved, or whose stored event stream cannot be read back is
   * listed in {@link ResetResult#getSkipped()} with its reason, never force-driven — one unusable
   * saga never stops the sweep from reaching the rest. A failure of the store itself is not a
   * per-row skip: it aborts the call, since every remaining row would fail the same way.
   *
   * @param query the page of escalated sagas to sweep (status filter fixed to {@code ESCALATED})
   * @param reason why the operator is un-escalating (recorded per row for audit; must be non-blank)
   * @return the per-page counts and the token to continue the sweep
   * @throws IllegalArgumentException if {@code query} sets a status other than {@code ESCALATED},
   *     or its page token is malformed (the daemon maps this to 400)
   */
  ResetResult resetEscalated(SagaQuery query, String reason);

  /** Releases any resources held by this service. The embedded implementation is a no-op. */
  @Override
  default void close() {}
}
