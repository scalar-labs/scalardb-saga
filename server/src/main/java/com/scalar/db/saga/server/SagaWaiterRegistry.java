package com.scalar.db.saga.server;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.engine.SettlementListener;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import net.jcip.annotations.ThreadSafe;

/**
 * The in-flight requests on this process that are waiting for a saga to settle, keyed by saga id.
 *
 * <p>Installed on the engine as a {@link SettlementListener}, so any drive that settles a saga here
 * wakes whoever is waiting for it — including the drive that resumes a saga after an asynchronous
 * step, which carries no {@link com.scalar.db.saga.api.SagaCallback} and could otherwise notify
 * nobody.
 *
 * <p><b>Best-effort.</b> A saga resumed on another replica settles without reaching this registry,
 * so every waiter also polls and re-reads at its bound. This removes latency; it does not deliver a
 * guarantee. See {@link SettlementListener}.
 *
 * <p>More than one waiter per saga is legal — two clients may await the same id — so registrations
 * are a collection and all of them are completed.
 */
@ThreadSafe
public final class SagaWaiterRegistry implements SettlementListener {

  // Registrations are distinguished by identity, not by value: two waiters on one saga are distinct
  // even though their futures are indistinguishable. Giving each an id makes that identity explicit
  // and keeps removal on a well-defined equality — a future's own equals is Object's, which Future
  // does not specify, so keying on the future itself would rest on an undefined contract.
  private final AtomicLong nextRegistrationId = new AtomicLong();

  // The value map is only ever touched inside a compute function. ConcurrentHashMap runs those
  // holding the key's bin lock, so a plain LinkedHashMap is safe here and needs no synchronization
  // of its own.
  // onSagaSettled copies the futures out and completes them *outside* that lock: completing a
  // CompletableFuture can run dependent actions on the completing thread, and running those under a
  // map lock would let one waiter's continuation block every other saga hashing to the same bin.
  private final ConcurrentMap<String, Map<Long, CompletableFuture<SagaStateSnapshot>>> waiters =
      new ConcurrentHashMap<>();

  /**
   * Registers interest in a saga. Register <b>before</b> reading the saga's state: a completion
   * landing between the read and the registration would otherwise be missed, and the waiter would
   * sit until its bound for an outcome that had already happened.
   *
   * <p>The caller supplies the future rather than receiving one, so the registry never hands out a
   * reference to state it owns and the caller waits on an object it already holds.
   *
   * @param sagaId the saga to wait for
   * @param settled completed with the saga's terminal snapshot if it settles on this process
   * @return a handle to close in order to deregister
   */
  public Waiter register(String sagaId, CompletableFuture<SagaStateSnapshot> settled) {
    long registrationId = nextRegistrationId.incrementAndGet();
    waiters.compute(
        sagaId,
        (id, existing) -> {
          Map<Long, CompletableFuture<SagaStateSnapshot>> registered =
              existing != null ? existing : new LinkedHashMap<>();
          registered.put(registrationId, settled);
          return registered;
        });
    return new Waiter(sagaId, registrationId);
  }

  @Override
  public boolean isWatching(String sagaId) {
    return waiters.containsKey(sagaId);
  }

  @Override
  public void onSagaSettled(SagaStateSnapshot saga) {
    List<CompletableFuture<SagaStateSnapshot>> toComplete = new ArrayList<>();
    // Copy under the bin lock, complete outside it — see the field comment.
    waiters.computeIfPresent(
        saga.getSagaId(),
        (id, registered) -> {
          toComplete.addAll(registered.values());
          return registered;
        });
    for (CompletableFuture<SagaStateSnapshot> future : toComplete) {
      future.complete(saga);
    }
  }

  /** How many sagas currently have a waiter. Visible for testing leak-freedom. */
  int watchedSagaCount() {
    return waiters.size();
  }

  /**
   * A registration. {@link #close()} deregisters and is idempotent, so it is safe in a
   * try-with-resources whose body returns early or throws.
   */
  public final class Waiter implements AutoCloseable {

    private final String sagaId;
    private final long registrationId;

    // The future itself is not held here: the caller supplied it and waits on its own reference,
    // and the map holds the copy this registry completes. A handle needs only enough to deregister.
    private Waiter(String sagaId, long registrationId) {
      this.sagaId = sagaId;
      this.registrationId = registrationId;
    }

    /** Deregisters, dropping the saga's entry entirely once its last waiter leaves. */
    @Override
    public void close() {
      waiters.computeIfPresent(
          sagaId,
          (id, registered) -> {
            registered.remove(registrationId);
            // Returning null removes the mapping, which is what keeps isWatching cheap and the map
            // from growing without bound across a process's lifetime.
            return registered.isEmpty() ? null : registered;
          });
    }
  }
}
