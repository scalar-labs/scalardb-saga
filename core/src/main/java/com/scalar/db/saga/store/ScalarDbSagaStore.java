package com.scalar.db.saga.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScalarDB-backed implementation of {@link SagaStore}.
 *
 * <p>A saga lives in two tables (see {@link SagaSchema}): {@code saga_events}, whose newest event
 * that carries a state is the saga's current state, and {@code saga_sweep}, the bucket-partitioned
 * list the background sweeps and the admin listing read. Every write that changes a saga's state
 * appends its event and moves its sweep row in one transaction, so the two never disagree, and a
 * writer derives the sweep row's key from the state it holds instead of looking it up. Reading one
 * saga reads its own event partition; there is no secondary index. Definitions live in {@code
 * saga_definitions}. All state-mutating operations use ScalarDB transactions for atomicity.
 *
 * <p>All transaction operations are executed through {@link #runInTransaction}, which provides
 * unified retry logic for {@link TransactionException} (including {@link
 * UnknownTransactionStatusException}). A conflict is retried by default, whether it surfaces during
 * CRUD as {@link CrudConflictException} or at commit as {@link CommitConflictException}; retry can
 * be disabled per operation (e.g., {@code createSaga} treats a conflict as permanent).
 */
public final class ScalarDbSagaStore implements SagaStore {

  private static final Logger logger = LoggerFactory.getLogger(ScalarDbSagaStore.class);

  private static final Pattern SAGA_ID_PATTERN = Pattern.compile("[a-zA-Z0-9._-]{1,128}");
  private static final int[] RECOVERABLE_SLICES =
      java.util.Arrays.stream(SagaStatus.values())
          .filter(SagaStatus::isRecoverable)
          .mapToInt(status -> SagaSchema.sliceOf(status, false))
          .toArray();

  /** All slice codes in ascending order — the stable sweep order for an all-status listing. */
  private static final int[] ALL_SLICES =
      java.util.stream.IntStream.concat(
              java.util.Arrays.stream(SagaStatus.values()).mapToInt(SagaStatus::getStatusCode),
              java.util.stream.IntStream.of(SagaSchema.WAITING_WITH_TIMEOUT_SLICE))
          .sorted()
          .toArray();

  /**
   * The earliest instant the epoch-millisecond {@code sweep_at} key can hold: -2^53 ms, the low end
   * of the BIGINT range ScalarDB accepts on every backend. Cosmos DB rejects a key value beyond it,
   * a scan bound included, so the listing's bounds and open-ended sentinel stay inside it.
   */
  static final Instant MIN_KEY_INSTANT = Instant.ofEpochMilli(-(1L << 53));

  /** The latest instant the {@code sweep_at} key can hold: 2^53 ms, the high end of that range. */
  static final Instant MAX_KEY_INSTANT = Instant.ofEpochMilli(1L << 53);

  /** Format version prefix for the opaque list page token. */
  private static final String PAGE_TOKEN_VERSION = "1";

  // Circuit breaker for the retention sweep in findByStatusOlderThan: this many consecutive
  // bucket-scan failures are treated as the store being unavailable rather than poison buckets,
  // and the failure propagates. Mirrors the recovery manager's consecutive-failed-pages guard.
  private static final int MAX_CONSECUTIVE_FAILED_BUCKETS = 3;

  private final DistributedTransactionManager txManager;
  private final ObjectMapper objectMapper;
  private final SagaSchema schema;
  private final ScalarDbSagaStoreConfig config;
  private final SagaDefinitionSerializer definitionSerializer;
  private final Supplier<String> appendIdSupplier;
  private final Supplier<Instant> nowSupplier;

  // Where each bucket's stale scan of each slice stopped when it filled its limit. A claim leaves
  // the sweep row in place, so without this every pass would re-read the same oldest rows and the
  // sagas behind them would wait until those escalate.
  private final ConcurrentMap<BucketSlice, SweepKey> recoveryResume = new ConcurrentHashMap<>();

  /**
   * Creates a new store instance.
   *
   * @param txManager the ScalarDB transaction manager
   * @param objectMapper Jackson ObjectMapper for JSON serialization
   * @param schema the saga schema (provides bucket configuration)
   * @param config the store configuration
   */
  public ScalarDbSagaStore(
      DistributedTransactionManager txManager,
      ObjectMapper objectMapper,
      SagaSchema schema,
      ScalarDbSagaStoreConfig config) {
    this(txManager, objectMapper, schema, config, () -> UUID.randomUUID().toString(), Instant::now);
  }

  /**
   * Visible for testing: inject a deterministic {@code appendIdSupplier} so contention scenarios
   * around the per-append UUID verifier are reproducible.
   */
  ScalarDbSagaStore(
      DistributedTransactionManager txManager,
      ObjectMapper objectMapper,
      SagaSchema schema,
      ScalarDbSagaStoreConfig config,
      Supplier<String> appendIdSupplier) {
    this(txManager, objectMapper, schema, config, appendIdSupplier, Instant::now);
  }

  /**
   * Visible for testing: additionally inject the timestamp source so tests that pin the recovery
   * manager's clock keep row timestamps (e.g., the {@code claimForRecovery} stamp) on the same
   * timeline, avoiding phantom re-claims when a pinned clock runs ahead of wall time.
   */
  ScalarDbSagaStore(
      DistributedTransactionManager txManager,
      ObjectMapper objectMapper,
      SagaSchema schema,
      ScalarDbSagaStoreConfig config,
      Supplier<String> appendIdSupplier,
      Supplier<Instant> nowSupplier) {
    this.txManager = Objects.requireNonNull(txManager, "txManager must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.schema = Objects.requireNonNull(schema, "schema must not be null");
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.appendIdSupplier =
        Objects.requireNonNull(appendIdSupplier, "appendIdSupplier must not be null");
    this.nowSupplier = Objects.requireNonNull(nowSupplier, "nowSupplier must not be null");
    this.definitionSerializer = new SagaDefinitionSerializer(objectMapper);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void close() {
    try {
      txManager.close();
    } catch (Exception e) {
      throw SagaPersistenceException.storeUnavailable(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Saga lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot createSaga(
      @Nullable String sagaId,
      String sagaName,
      String ownerId,
      Map<String, Object> input,
      String definitionVersion) {
    if (sagaId == null) {
      sagaId = UUID.randomUUID().toString();
    } else {
      validateSagaId(sagaId);
    }
    String payload = toJson(input);
    validatePayloadSize(payload);
    StatusEvent startedEvent = StatusEvent.started(payload);
    String id = sagaId; // effectively final for lambda
    int bucket = schema.bucketOf(id);
    int slice = SagaSchema.sliceOf(SagaStatus.RUNNING, false);
    // Identifies this call's own seq-0 event, so a post-UnknownTransactionStatus verify can tell
    // our insert apart from a saga that already owns this id. A state read cannot: a state is
    // present either way, so it would confirm the commit and hand back the other caller's saga.
    String appendId = appendIdSupplier.get();

    try {
      return runInTransaction(
          tx -> {
            Instant now = nowSupplier.get();
            SagaStateSnapshot snapshot =
                new SagaStateSnapshot(
                    id, sagaName, SagaStatus.RUNNING, definitionVersion, now, now);
            long sweepAt = now.toEpochMilli();
            tx.insert(
                buildEventInsert(
                    id,
                    0,
                    startedEvent,
                    appendId,
                    now,
                    new StateColumns(slice, sweepAt, snapshot)));
            tx.insert(buildSweepInsert(bucket, slice, sweepAt, snapshot));
            return snapshot;
          },
          verifyOwnAppendCommitted(id, 0, appendId),
          "create saga " + id,
          false);
    } catch (SagaConcurrentModificationException e) {
      // A foreign append_id at sequence 0 means this saga id already belongs to someone else.
      // Above sequence 0 a foreign id is a transient racer and the conflict code is the right
      // answer; at sequence 0 it is a permanent duplicate, and telling the caller to retry an id
      // that can never be free would be wrong. Report it as the duplicate it is.
      throw existingSagaOr(id, e);
    } catch (SagaPersistenceException e) {
      throw existingSagaOr(id, e);
    }
  }

  /**
   * Reconciles a create that failed: when a saga already occupies {@code id}, that is the honest
   * answer rather than the failure which exposed it. Returns the exception to throw, so the {@code
   * throw} stays visible at the catch site.
   */
  private RuntimeException existingSagaOr(String id, RuntimeException failure) {
    Optional<SagaStateSnapshot> existing = Optional.empty();
    try {
      existing = getStateSnapshot(id);
    } catch (Exception lookupEx) {
      failure.addSuppressed(lookupEx);
    }
    return existing.isPresent()
        ? new SagaAlreadyExistsException(id, existing.get(), failure)
        : failure;
  }

  @Override
  public void registerDefinition(SagaDefinition definition) {
    String json = definitionSerializer.serialize(definition);
    String name = definition.getName();
    String version = definition.getVersion();

    runInTransaction(
        tx -> {
          Optional<Result> existing = tx.get(buildDefinitionGet(name, version));

          if (existing.isPresent()) {
            SagaDefinition existingDef =
                definitionSerializer.deserialize(existing.get().getText("definition_json"));
            if (definition.equals(existingDef)) {
              return Boolean.TRUE; // idempotent no-op
            }
            throw SagaDefinitionException.versionContentConflict(name, version);
          }

          tx.insert(buildDefinitionInsert(name, version, json, monotonicStamp(tx, name)));
          return Boolean.TRUE;
        },
        () -> {
          Optional<SagaDefinition> found = getDefinition(name, version);
          if (found.isEmpty()) {
            return Optional.empty();
          }
          // Verify the found definition matches what we tried to register.
          // A different definition means a concurrent registration won;
          // our insert did not commit. Return empty so the retry's primary
          // action detects the conflict and throws SagaDefinitionException.
          String foundJson = definitionSerializer.serialize(found.get());
          if (!json.equals(foundJson)) {
            return Optional.empty();
          }
          return Optional.of(Boolean.TRUE);
        },
        "register definition " + name + " " + version);
  }

  @Override
  public Optional<SagaDefinition> getDefinition(String sagaName, String definitionVersion) {
    return runInTransaction(
        tx -> {
          Optional<Result> result = tx.get(buildDefinitionGet(sagaName, definitionVersion));
          return result.map(r -> definitionSerializer.deserialize(r.getText("definition_json")));
        },
        null, // read-only — retry the whole transaction on UTSE
        "get definition " + sagaName + " " + definitionVersion);
  }

  /**
   * {@inheritDoc}
   *
   * <p>"Latest" is the version with the greatest {@code registered_at}; see {@link
   * #newestRegistration} for how a tie resolves and why that is accepted.
   */
  @Override
  public Optional<SagaDefinition> getDefinition(String sagaName) {
    return runInTransaction(
        tx ->
            newestRegistration(tx.scan(buildDefinitionScan(sagaName)))
                .map(r -> definitionSerializer.deserialize(r.getText("definition_json"))),
        null, // read-only
        "get latest definition " + sagaName);
  }

  /**
   * The row with the greatest {@code registered_at}, which is what "latest version" means for this
   * store. The one place that ranks definition rows, so the lookup and the stamp that feeds it
   * cannot disagree.
   *
   * <p>Equal stamps can only come from two replicas registering different versions of the same saga
   * concurrently: a single replica always stamps strictly after every row it can see ({@link
   * #monotonicStamp}). Two replicas tie when their clocks read the same millisecond, or when both
   * clocks are behind the newest row, since both then clamp to one millisecond past it. On a tie
   * the first row in clustering-key order wins, which is the lexicographically smallest version
   * string. That is accepted rather than broken by a secondary key: versions are free-form text, so
   * no order on them means anything, and the version that loses the tie stays registered and
   * startable by version.
   *
   * <p>A row with no stamp is corrupt, not old: this store has written {@code registered_at} on
   * every insert since the table existed. Such a row fails the read as a deserialization failure
   * rather than being ranked somewhere arbitrary, and it fails whether or not it is the only row.
   */
  private static Optional<Result> newestRegistration(List<Result> rows) {
    Result newest = null;
    Instant newestAt = null;
    for (Result row : rows) {
      Instant at = registeredAt(row);
      if (newestAt == null || at.isAfter(newestAt)) {
        newest = row;
        newestAt = at;
      }
    }
    return Optional.ofNullable(newest);
  }

  private static Instant registeredAt(Result row) {
    Instant registeredAt = row.getTimestampTZ("registered_at");
    if (registeredAt == null) {
      throw SagaPersistenceException.deserializationFailed(
          new IllegalStateException("saga_definitions row has no registered_at"));
    }
    return registeredAt;
  }

  // ---------------------------------------------------------------------------
  // Events
  // ---------------------------------------------------------------------------

  @Override
  public void recordStepEvent(String sagaId, int sequence, StepEvent event) {
    validatePayloadSize(event.getPayload());
    String appendId = appendIdSupplier.get();
    runInTransaction(
        tx -> {
          tx.insert(buildEventInsert(sagaId, sequence, event, appendId, nowSupplier.get(), null));
          return Boolean.TRUE;
        },
        () ->
            verifyOwnEventCommitted(sagaId, sequence, appendId)
                ? Optional.of(Boolean.TRUE)
                : Optional.empty(),
        "append event for saga " + sagaId,
        sagaId);
  }

  @Override
  public SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current,
      int sequence,
      StatusEvent event,
      String ownerId,
      @Nullable Instant stateUpdatedAt) {
    validatePayloadSize(event.getPayload());
    requireNotWaiting(current);
    String sagaId = current.getSagaId();
    SagaStatus newStatus = event.getTargetStatus();
    int bucket = schema.bucketOf(sagaId);
    int slice = SagaSchema.sliceOf(current.getStatus(), false);
    long sweepAt = current.getUpdatedAt().toEpochMilli();
    String appendId = appendIdSupplier.get();

    return runInTransaction(
        tx -> {
          // The saga's sweep row sits at the key its current state names. If it is not there,
          // another writer moved the saga on since the caller read it.
          if (tx.get(buildSweepGet(bucket, slice, sweepAt, sagaId)).isEmpty()) {
            throw new SagaConcurrentModificationException(sagaId);
          }
          Instant now = nowSupplier.get();
          // The event's audit timestamp is always the real time; updatedAt is the recovery-scan
          // key, so a caller passes EPOCH to hand the saga to the sweeper at once (null = now).
          Instant newUpdatedAt = stateUpdatedAt != null ? stateUpdatedAt : now;
          SagaStateSnapshot updated = current.withTransition(newStatus, newUpdatedAt);
          StateColumns next =
              new StateColumns(
                  SagaSchema.sliceOf(newStatus, false), newUpdatedAt.toEpochMilli(), updated);
          appendTransition(tx, sequence, event, appendId, now, bucket, slice, sweepAt, next);
          return updated;
        },
        verifyOwnAppendCommitted(sagaId, sequence, appendId),
        "record transition for saga " + sagaId,
        sagaId);
  }

  // ---------------------------------------------------------------------------
  // Async park / resume
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot park(
      SagaStateSnapshot current,
      int sequence,
      StepEvent pendingEvent,
      @Nullable Instant parkedDeadline) {
    requireNotWaiting(current);
    requireInKeyRange(parkedDeadline, "parkedDeadline");
    String sagaId = current.getSagaId();
    int bucket = schema.bucketOf(sagaId);
    int slice = SagaSchema.sliceOf(current.getStatus(), false);
    long sweepAt = current.getUpdatedAt().toEpochMilli();
    String appendId = appendIdSupplier.get();

    return runInTransaction(
        tx -> {
          if (tx.get(buildSweepGet(bucket, slice, sweepAt, sagaId)).isEmpty()) {
            throw new SagaConcurrentModificationException(sagaId);
          }
          Instant now = nowSupplier.get();
          SagaStateSnapshot updated = current.withTransition(SagaStatus.WAITING, now);
          // A bounded park sorts by its deadline, where the timeout sweep finds it; an unbounded
          // one sorts by the park time, where no timeout sweep reads it.
          StateColumns next =
              new StateColumns(
                  SagaSchema.sliceOf(SagaStatus.WAITING, parkedDeadline != null),
                  (parkedDeadline != null ? parkedDeadline : now).toEpochMilli(),
                  updated);
          appendTransition(tx, sequence, pendingEvent, appendId, now, bucket, slice, sweepAt, next);
          return updated;
        },
        verifyOwnAppendCommitted(sagaId, sequence, appendId),
        "park saga " + sagaId,
        sagaId);
  }

  @Override
  public SagaStateSnapshot resumeParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent completedEvent) {
    validatePayloadSize(completedEvent.getPayload());
    return transitionParkedStep(
        current, sequence, completedEvent, SagaStatus.RUNNING, "resume parked step");
  }

  @Override
  public SagaStateSnapshot failParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent failedEvent, SagaStatus targetStatus) {
    if (targetStatus != SagaStatus.COMPENSATING && targetStatus != SagaStatus.ESCALATED) {
      throw new IllegalArgumentException(
          "failParkedStep targetStatus must be COMPENSATING or ESCALATED, got " + targetStatus);
    }
    validatePayloadSize(failedEvent.getPayload());
    return transitionParkedStep(current, sequence, failedEvent, targetStatus, "fail parked step");
  }

  @Override
  public SagaStateSnapshot redriveParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent redriveEvent) {
    return transitionParkedStep(
        current, sequence, redriveEvent, SagaStatus.RUNNING, "redrive parked step");
  }

  /**
   * Shared body for the three transitions out of {@code WAITING}: {@code resumeParkedStep}
   * (callback), {@code failParkedStep} (give-up) and {@code redriveParkedStep} (re-drive). In one
   * transaction it appends {@code event}, transitions the saga to {@code targetStatus}, and moves
   * its sweep row out of the parked slice.
   *
   * <p>The parked saga's sweep key is not in the snapshot: a park with a timeout sorts by its
   * deadline. It is read from the event just before {@code sequence}, the step's {@code
   * STEP_PENDING}, because a parked saga's log grows only through status changes (see {@link
   * SagaStore}). That event must also be the park the snapshot describes, by its time; otherwise
   * the caller holds an older park, and the saga has moved on.
   *
   * <p>The sweep-row delete is the real mutual-exclusion point: two transitions deleting the same
   * row conflict at commit, so exactly one commits; a loser retries, finds the row gone, and throws
   * {@link SagaConcurrentModificationException}. Kept in one place so the three
   * race-safety-critical paths cannot silently diverge.
   */
  private SagaStateSnapshot transitionParkedStep(
      SagaStateSnapshot current,
      int sequence,
      StepEvent event,
      SagaStatus targetStatus,
      String op) {
    String sagaId = current.getSagaId();
    if (current.getStatus() != SagaStatus.WAITING) {
      throw new SagaConcurrentModificationException(sagaId);
    }
    int bucket = schema.bucketOf(sagaId);
    String appendId = appendIdSupplier.get();

    return runInTransaction(
        tx -> {
          ParkedState parked = readParkedState(tx, sagaId, sequence, current);
          if (tx.get(buildSweepGet(bucket, parked.slice(), parked.sweepAt(), sagaId)).isEmpty()) {
            throw new SagaConcurrentModificationException(sagaId);
          }
          Instant now = nowSupplier.get();
          SagaStateSnapshot updated = current.withTransition(targetStatus, now);
          StateColumns next =
              new StateColumns(
                  SagaSchema.sliceOf(targetStatus, false), now.toEpochMilli(), updated);
          appendTransition(
              tx, sequence, event, appendId, now, bucket, parked.slice(), parked.sweepAt(), next);
          return updated;
        },
        verifyOwnAppendCommitted(sagaId, sequence, appendId),
        op + " for saga " + sagaId,
        sagaId);
  }

  /**
   * The sweep key of the park that {@code current} describes, read from the event just before
   * {@code sequence}.
   *
   * @throws SagaConcurrentModificationException when the saga is no longer in that park
   * @throws SagaPersistenceException when the saga is still parked but the event before {@code
   *     sequence} carries no state, which breaks the rule that a parked saga's log grows only
   *     through status changes
   */
  private ParkedState readParkedState(
      DistributedTransaction tx, String sagaId, int sequence, SagaStateSnapshot current)
      throws Exception {
    Optional<Result> previous =
        sequence > 0 ? tx.get(buildEventGet(sagaId, sequence - 1)) : Optional.empty();
    if (previous.isEmpty()) {
      throw new SagaConcurrentModificationException(sagaId);
    }
    Result row = previous.get();
    if (row.isNull("state_slice")) {
      // Either the caller is behind (the saga moved on, and later events filled the log), or the
      // log of a still-parked saga grew by an event that changed nothing. The first is a lost
      // race; the second is damage, and reporting it as a race would stall the saga silently.
      Optional<SagaStateSnapshot> latest = readHead(tx, sagaId).map(Head::state);
      if (latest.isPresent() && latest.get().getStatus() == SagaStatus.WAITING) {
        logger.error(
            "Saga {} is parked, but event {} before the caller's sequence carries no state;"
                + " a parked saga's log must grow only through status changes",
            sagaId,
            sequence - 1);
        throw SagaPersistenceException.deserializationFailed(
            new IllegalStateException(
                "Parked saga " + sagaId + " has a stateless event at " + (sequence - 1)));
      }
      throw new SagaConcurrentModificationException(sagaId);
    }
    int slice = row.getInt("state_slice");
    Instant parkedAt = row.getTimestampTZ("created_at");
    if (SagaSchema.statusOfSlice(slice) != SagaStatus.WAITING
        || parkedAt == null
        || parkedAt.toEpochMilli() != current.getUpdatedAt().toEpochMilli()) {
      throw new SagaConcurrentModificationException(sagaId);
    }
    return new ParkedState(slice, row.getBigInt("sweep_at"));
  }

  /**
   * Post-{@code UnknownTransactionStatus} verification shared by the event-append and transition
   * paths: identifies the event at {@code sequence} by the caller's {@code appendId}, a UUID minted
   * once per logical append and persisted with the row, and distinguishes three outcomes.
   *
   * <ul>
   *   <li>Present with our {@code append_id}: our commit landed; returns {@code true}.
   *   <li>Absent: our commit did not land; returns {@code false} so the caller retries.
   *   <li>Present with another {@code append_id}: another writer won this sequence. A mismatch
   *       proves only that the row is not ours, not what type it is; the winner may be a same-type
   *       racer (two at-least-once callbacks both appending {@code STEP_COMPLETED}) or a cross-type
   *       one (a callback racing a deadline timeout's re-drive). Either way it is a proven
   *       collision, not an unresolved commit, so this throws {@link
   *       SagaConcurrentModificationException} at once instead of reporting "not committed";
   *       retrying would only reuse the same taken sequence.
   * </ul>
   *
   * <p>Comparing {@code append_id} rather than {@code event_type} is what makes this exact: every
   * logical append mints its own UUID, so a same-type racer (which type alone cannot distinguish
   * from us) is caught, and a cross-type racer differs as well. The reader that sees the winner's
   * {@code append_id} knows the row is not its own.
   */
  private boolean verifyOwnEventCommitted(String sagaId, int sequence, String appendId) {
    return runInTransaction(
        tx -> {
          Optional<Result> event = tx.get(buildEventGet(sagaId, sequence));
          if (event.isEmpty()) {
            return false;
          }
          if (appendId.equals(event.get().getText("append_id"))) {
            return true;
          }
          // Another writer's row sits at our sequence: a proven collision, not an unresolved
          // commit. Retrying would only reuse the same taken sequence, so surface the conflict now.
          throw new SagaConcurrentModificationException(sagaId);
        },
        null,
        "verify event " + sagaId + " seq " + sequence);
  }

  /**
   * Verifier for any write that appends one event and changes the saga's state: the transitions
   * (park, resume, timeout, {@code recordStatusEvent}) and {@code createSaga}'s seq-0 insert.
   * Treats the tx as committed when the event at {@code sequence} was written by us (see {@link
   * #verifyOwnEventCommitted}). On a match it returns the resulting state snapshot; when the event
   * is absent it returns empty so the caller retries; when another writer won the sequence, {@link
   * #verifyOwnEventCommitted} throws {@link SagaConcurrentModificationException} directly.
   *
   * <p>{@code createSaga} translates that conflict into {@link SagaAlreadyExistsException}, because
   * at sequence 0 a foreign {@code append_id} means the id is taken rather than contended.
   */
  private CommitVerifier<SagaStateSnapshot> verifyOwnAppendCommitted(
      String sagaId, int sequence, String appendId) {
    return () ->
        verifyOwnEventCommitted(sagaId, sequence, appendId)
            ? loadStateSnapshot(sagaId)
            : Optional.empty();
  }

  @Override
  public List<SagaEvent> getEvents(String sagaId) {
    return runInTransaction(
        tx -> {
          List<Result> results = tx.scan(buildEventScan(sagaId));
          return results.stream().map(this::toSagaEvent).toList();
        },
        null,
        "get events for saga " + sagaId);
  }

  @Override
  public Optional<SagaStateAndEvents> getStateWithEvents(String sagaId, int maxEvents) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive, got " + maxEvents);
    }
    return runInTransaction(
        tx -> {
          // One newest-first pass serves both halves: the events to return, and the saga's state,
          // which is the newest event that carries one. The pass keeps reading past the events it
          // returns only until it reaches that state.
          List<Result> newestFirst = new ArrayList<>();
          @Nullable SagaStateSnapshot state = null;
          try (TransactionCrudOperable.Scanner scanner =
              tx.getScanner(buildNewestFirstScan(sagaId))) {
            for (Optional<Result> next = scanner.one(); next.isPresent(); next = scanner.one()) {
              Result r = next.get();
              if (newestFirst.size() <= maxEvents) {
                newestFirst.add(r);
              }
              if (state == null && isStateRow(r)) {
                state = toStateSnapshot(sagaId, r);
              }
              if (state != null && newestFirst.size() > maxEvents) {
                break;
              }
            }
          }
          if (newestFirst.isEmpty()) {
            return Optional.<SagaStateAndEvents>empty();
          }
          if (state == null) {
            throw noStateRow(sagaId);
          }
          boolean truncated = newestFirst.size() > maxEvents;
          List<SagaEvent> retained =
              new ArrayList<>(
                  (truncated ? newestFirst.subList(0, maxEvents) : newestFirst)
                      .stream().map(this::toSagaEvent).toList());
          Collections.reverse(retained); // back to ascending (chronological) order
          return Optional.of(new SagaStateAndEvents(state, retained, truncated));
        },
        null, // read-only — retry the whole transaction on UTSE
        "get saga state with events " + sagaId);
  }

  @Override
  public int getEventCount(String sagaId) {
    return runInTransaction(
        tx -> {
          List<Result> results =
              tx.scan(Scan.newBuilder(buildEventScan(sagaId)).projections("saga_id").build());
          return results.size();
        },
        null,
        "get event count for saga " + sagaId);
  }

  @Override
  public Optional<NewestEvent> getNewestEvent(String sagaId) {
    return runInTransaction(
        tx ->
            readHead(tx, sagaId)
                .map(
                    head -> {
                      Instant createdAt = head.newestCreatedAt();
                      if (createdAt == null) {
                        // Unreachable through this store, which stamps created_at on every append.
                        // Reporting the epoch rather than throwing keeps a damaged row claimable: a
                        // saga that cannot report progress must not become permanently unclaimable.
                        logger.warn(
                            "Newest event of saga {} has no created_at; treating as no progress",
                            sagaId);
                        createdAt = Instant.EPOCH;
                      }
                      return new NewestEvent(
                          head.newestType(),
                          createdAt,
                          head.newestSequence(),
                          head.lastClaimedAt(),
                          head.state());
                    }),
        null, // read-only — retry the whole transaction on UTSE
        "get newest event for saga " + sagaId);
  }

  // ---------------------------------------------------------------------------
  // Queries
  // ---------------------------------------------------------------------------

  @Override
  public Optional<SagaStateSnapshot> getStateSnapshot(String sagaId) {
    return loadStateSnapshot(sagaId);
  }

  /**
   * Lists saga snapshots for {@link SagaStore#listStateSnapshots} from {@code saga_sweep},
   * paginating by <b>whole cohorts</b> of one sort timestamp ({@code sweep_at}): a page boundary
   * never splits the rows that share one. {@code sweep_at} is the saga's {@code updatedAt}, except
   * in the slice of parked steps with a timeout, which sorts by deadline; there the time filters
   * are applied to {@code updated_at} in memory, and a cohort is the rows sharing one deadline.
   *
   * <h4>Why cohort pagination, and not a keyset cursor on {@code saga_id}</h4>
   *
   * The natural way to page a bucket-partitioned scan is a keyset cursor over the clustering key
   * {@code (slice, sweep_at, saga_id)}. Two ScalarDB properties rule that out here:
   *
   * <ul>
   *   <li>A {@code Scan} can only range on the <b>last</b> clustering-key column it specifies, with
   *       the preceding columns pinned to equality. Resuming "strictly after {@code (sweep_at,
   *       saga_id)}" would range on two columns at once, which ScalarDB rejects.
   *   <li>More fundamentally, ScalarDB does <b>not normalize TEXT collation</b>: the ordering of
   *       {@code saga_id} (a TEXT clustering key) is whatever the underlying database does and is
   *       not guaranteed to match byte order or Java {@code String} ordering. Any scheme that uses
   *       {@code saga_id} as a tiebreaker — a sentinel upper bound, or an in-memory {@code >}
   *       filter — is only correct on backends whose TEXT collation happens to match our
   *       assumption, a silent no-drop hazard at exact-timestamp ties.
   * </ul>
   *
   * <p>So this design never compares {@code saga_id} at all. The cursor is {@code (bucket, slice,
   * sweep_at)} and resume is a pure {@code sweep_at > cursor} range. {@code sweep_at} is an
   * integer, not TEXT, so its ordering is collation-independent and consistent across every
   * backend. To keep the cursor sound without a {@code saga_id} tiebreaker, a page returns a cohort
   * <b>in full or not at all</b>: when a slice fills the page limit, {@link #scanSlice} streams on
   * just far enough to <b>complete</b> the cohort straddling the boundary, then stops at the next
   * cohort and sets the cursor to the completed cohort's timestamp.
   *
   * <h4>Trade-off: {@code pageSize} is a target, and the memory bound is {@code pageSize} plus
   * cohort size</h4>
   *
   * Because a page never splits a cohort, {@code pageSize} is a <b>target</b>, not a cap: a full
   * page runs <b>over</b> it to finish the cohort straddling the limit, and a single cohort larger
   * than {@code pageSize} is returned whole as an over-sized page (a mass event spread across
   * slices yields several such pages, not one). So the rows materialized for one page are bounded
   * by {@code pageSize} plus the <b>largest cohort</b> (rows sharing one {@code sweep_at} within
   * one {@code (bucket, slice)}): the page can already hold up to a full target of rows from
   * earlier slices when the cohort that overflows it is completed. The cohort term is the one
   * unbounded quantity in this path: recovery caps its analogous per-slice scan (see {@link
   * ScalarDbSagaStoreConfig#getRecoveryScanLimit()}), but this listing does not. A pathological
   * cohort — e.g. a mass transition stamping many sagas with the same millisecond, divided only
   * across {@code numBuckets} — therefore drives peak memory for the call. Operators should
   * provision heap and response limits for {@code pageSize} plus the largest expected cohort.
   * Listing is best-effort under concurrent mutation.
   *
   * <h4>Future option: bound memory by splitting cohorts on {@code saga_id}</h4>
   *
   * To cap memory at {@code pageSize} and make page sizes exact, pagination could page
   * <b>within</b> a cohort using {@code saga_id} — the trailing clustering-key column — as a keyset
   * tiebreaker. The objection above is about a <b>global</b> {@code (sweep_at, saga_id)} keyset
   * (two ranging columns; and sentinel or in-memory schemes that assume an order). An
   * <b>intra-cohort</b> keyset sidesteps both: with {@code sweep_at} pinned to equality a scan
   * ranges on {@code saga_id} alone, and resuming with the backend's own {@code saga_id > last}
   * predicate is collation-safe — it relies only on each backend being self-consistent, never on an
   * assumed Java or byte order. It is deferred because it reintroduces a {@code saga_id} comparison
   * and a two-phase resume scan (finish the cohort, then advance {@code sweep_at}) on mid-cohort
   * resumes. It can be added later <b>without breaking compatibility</b>: today's cursor is a valid
   * future cursor with no intra-cohort offset, so existing page tokens keep resuming correctly.
   *
   * <h4>Cost: sequential per-slice transactions, {@code O(numBuckets × slices)} per page</h4>
   *
   * Each {@code (bucket, slice)} is scanned in its own read-only transaction (see {@link
   * #scanSlice}), and the slices are swept sequentially until the page fills or every slice drains.
   * A page therefore costs up to {@code numBuckets × slices} round-trips <b>independent of how many
   * rows it returns</b> — a sparse or empty match walks every slice before returning (at defaults,
   * 16 buckets × 7 slices = 112; a status filter collapses the inner sweep to one slice, or two for
   * {@code WAITING}). This is acceptable for a low-frequency admin listing: the cost is latency,
   * not correctness, and every transaction is read-only. The escape hatch, if it ever matters, is a
   * concurrent per-bucket fan-out, deferred because the bucket-ordered cursor would then need a
   * cross-bucket merge and a different resume scheme.
   */
  @Override
  public SagaPage<SagaStateSnapshot> listStateSnapshots(SagaQuery query) {
    int numBuckets = schema.getNumBuckets();
    int pageSize = query.getPageSize();
    @Nullable Instant updatedAfter = requireInKeyRange(query.getUpdatedAfter(), "updatedAfter");
    @Nullable Instant updatedBefore = requireInKeyRange(query.getUpdatedBefore(), "updatedBefore");

    // Which slices to sweep, in a stable ascending order, and where a token resumes.
    SagaStatus statusFilter = query.getStatus();
    int[] slices = statusFilter != null ? slicesOf(statusFilter) : ALL_SLICES;
    // A token is bound to the filters that produced it; reusing it under different filters is
    // rejected rather than silently resuming against the wrong data.
    String filterKey = PageCursor.filterKey(query);
    String pageToken = query.getPageToken();
    @Nullable PageCursor cursor =
        pageToken == null ? null : PageCursor.decode(pageToken, numBuckets, slices, filterKey);

    List<SagaStateSnapshot> items = new ArrayList<>();
    int startBucket = cursor != null ? cursor.bucket() : 0;

    for (int bucket = startBucket; bucket < numBuckets; bucket++) {
      int startSliceIdx =
          (cursor != null && bucket == cursor.bucket()) ? indexOf(slices, cursor.slice()) : 0;
      for (int si = startSliceIdx; si < slices.length; si++) {
        int slice = slices[si];
        // Resume from the token only in the exact (bucket, slice) it points at.
        @Nullable Instant afterTs =
            (cursor != null && bucket == cursor.bucket() && slice == cursor.slice())
                ? cursor.sweepAt()
                : null;
        SliceResult result =
            scanSlice(bucket, slice, afterTs, updatedAfter, updatedBefore, pageSize - items.size());
        items.addAll(result.rows());
        if (result.resumeTs() != null) {
          // The page filled within this slice; the token resumes it after the last complete cohort.
          return new SagaPage<>(
              items, new PageCursor(bucket, slice, result.resumeTs()).encode(filterKey));
        }
        // Slice drained — move on to the next one.
      }
    }
    // Every slice was swept to the end.
    return new SagaPage<>(items, null);
  }

  /**
   * Scans one {@code (bucket, slice)} of {@code saga_sweep} in its own transaction (never one tx
   * across a page), returning whole {@code sweep_at} cohorts up to about {@code limit} rows.
   *
   * <p>Streams the slice in clustering order with a {@link TransactionCrudOperable.Scanner}: once
   * it holds at least {@code limit} rows it keeps pulling only far enough to finish the cohort in
   * progress, then stops at the first row of the next cohort (which belongs to the next page — the
   * timestamp-exclusive cursor re-reads it). This is a single pass — no dropped-then-re-read
   * trailing cohort, no separate full-cohort scan, and no {@code saga_id} comparison — so it also
   * completes an over-sized single-timestamp cohort for free. The scanner lives entirely within
   * this one (read-only) transaction and is closed before commit.
   *
   * <p>In every slice but the one of parked steps with a timeout, {@code sweep_at} is the saga's
   * {@code updatedAt}, so the time filters bound the scan itself. In that slice they cannot, and
   * are applied to each row's {@code updated_at} instead.
   *
   * @return the rows plus, when the slice was <b>not</b> drained, the last complete timestamp to
   *     resume after; {@link SliceResult#resumeTs()} is {@code null} when the slice is fully
   *     drained
   */
  private SliceResult scanSlice(
      int bucket,
      int slice,
      @Nullable Instant afterTs,
      @Nullable Instant updatedAfter,
      @Nullable Instant updatedBefore,
      int limit) {
    boolean filterInMemory = slice == SagaSchema.WAITING_WITH_TIMEOUT_SLICE;
    return runInTransaction(
        tx -> {
          Instant startTs;
          boolean startInclusive;
          if (afterTs != null) {
            startTs = afterTs; // resume strictly after the last fully-returned timestamp
            startInclusive = false;
          } else if (filterInMemory) {
            startTs = MIN_KEY_INSTANT;
            startInclusive = true;
          } else {
            startTs = updatedAfter != null ? updatedAfter : Instant.EPOCH;
            startInclusive = true;
          }
          // Open-ended upper bound: scan to the latest instant the key can hold, so every stored
          // row
          // is in range. That sentinel keeps the end key at the same width as the start key.
          Instant endTs = filterInMemory || updatedBefore == null ? MAX_KEY_INSTANT : updatedBefore;

          List<SagaStateSnapshot> rows = new ArrayList<>();
          @Nullable Instant lastTs = null; // sort timestamp of the last row accepted
          try (TransactionCrudOperable.Scanner scanner =
              tx.getScanner(buildSweepRangeScan(bucket, slice, startTs, startInclusive, endTs))) {
            for (Optional<Result> next = scanner.one(); next.isPresent(); next = scanner.one()) {
              Result r = next.get();
              Instant ts = Instant.ofEpochMilli(r.getBigInt("sweep_at"));
              if (rows.size() >= limit && !ts.equals(lastTs)) {
                break; // limit met and a new cohort begins — leave it for the next page
              }
              SagaStateSnapshot snapshot = toSweepSnapshot(r);
              if (filterInMemory && !withinWindow(snapshot, updatedAfter, updatedBefore)) {
                continue;
              }
              rows.add(snapshot);
              lastTs = ts;
            }
          }
          // Short of the limit ⇒ the slice drained with room to spare (no resume point). Otherwise
          // resume strictly after the last complete cohort's timestamp.
          @Nullable Instant resumeTs = rows.size() >= limit ? lastTs : null;
          return new SliceResult(rows, resumeTs);
        },
        null,
        "list saga snapshots");
  }

  private static boolean withinWindow(
      SagaStateSnapshot snapshot, @Nullable Instant updatedAfter, @Nullable Instant updatedBefore) {
    long updatedAt = snapshot.getUpdatedAt().toEpochMilli();
    return (updatedAfter == null || updatedAt >= updatedAfter.toEpochMilli())
        && (updatedBefore == null || updatedAt <= updatedBefore.toEpochMilli());
  }

  /** The slices that hold sagas in {@code status}: one per status, and two for {@code WAITING}. */
  private static int[] slicesOf(SagaStatus status) {
    return status == SagaStatus.WAITING
        ? new int[] {status.getStatusCode(), SagaSchema.WAITING_WITH_TIMEOUT_SLICE}
        : new int[] {status.getStatusCode()};
  }

  private static int indexOf(int[] codes, int code) {
    for (int i = 0; i < codes.length; i++) {
      if (codes[i] == code) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Rejects a time outside the range the {@code sweep_at} key can hold ({@link #MIN_KEY_INSTANT} to
   * {@link #MAX_KEY_INSTANT}), with a clear message, rather than letting it surface as a
   * lower-level exception when the key is built. Sub-millisecond precision needs no handling here:
   * {@code toEpochMilli} drops it, the same way the stored values were written.
   *
   * @return {@code bound} unchanged (including {@code null}, which means no bound)
   */
  private static @Nullable Instant requireInKeyRange(@Nullable Instant bound, String field) {
    if (bound != null && (bound.isBefore(MIN_KEY_INSTANT) || bound.isAfter(MAX_KEY_INSTANT))) {
      throw new SagaIllegalArgumentException(
          field + " must be in [" + MIN_KEY_INSTANT + ", " + MAX_KEY_INSTANT + "]: " + bound);
    }
    return bound;
  }

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  @Override
  public ScanCursor initialSweepCursor(String ownerId) {
    return new PermutedBucketCursor(SweepScatter.seed(ownerId), 0);
  }

  @Override
  public @Nullable ScanCursor advanceSweepCursor(ScanCursor cursor) {
    SweepStep step = nextSweepStep(cursor);
    return step == null ? null : step.nextCursor();
  }

  /**
   * Resolves a sweep cursor to the bucket it points at and the cursor for the following bucket, or
   * {@code null} when the sweep is exhausted. A {@link PermutedBucketCursor} walks the owner's
   * scattered permutation; a {@code null} or {@link BucketCursor} cursor walks buckets in ascending
   * order (backward-compatible direct store access).
   */
  private @Nullable SweepStep nextSweepStep(@Nullable ScanCursor cursor) {
    int numBuckets = schema.getNumBuckets();
    if (cursor instanceof PermutedBucketCursor(int seed, int position)) {
      if (position >= numBuckets) {
        return null;
      }
      int bucket = SweepScatter.permutation(seed, numBuckets)[position];
      @Nullable ScanCursor next =
          position + 1 < numBuckets ? new PermutedBucketCursor(seed, position + 1) : null;
      return new SweepStep(bucket, next);
    }
    int startBucket;
    if (cursor == null) {
      startBucket = 0;
    } else if (cursor instanceof BucketCursor(int nextBucket)) {
      startBucket = nextBucket;
    } else {
      // A foreign cursor type would otherwise silently restart an un-scattered ascending sweep —
      // the exact lockstep failure the scattered cursors exist to prevent. Fail fast instead.
      throw new IllegalArgumentException(
          "Unknown ScanCursor type "
              + cursor.getClass().getName()
              + "; cursors are only valid with the store that created them");
    }
    if (startBucket >= numBuckets) {
      return null;
    }
    @Nullable ScanCursor next =
        startBucket + 1 < numBuckets ? new BucketCursor(startBucket + 1) : null;
    return new SweepStep(startBucket, next);
  }

  @Override
  public int recoveryPageSize() {
    return config.getRecoveryScanLimit() * RECOVERABLE_SLICES.length;
  }

  @Override
  public Recoverables findRecoverable(Instant threshold, @Nullable ScanCursor cursor) {
    SweepStep step = nextSweepStep(cursor);
    if (step == null) {
      return new Recoverables(List.of(), null);
    }

    int bucket = step.bucket();
    // Cap each slice scan to avoid unbounded memory usage in large buckets.
    int scanLimit = config.getRecoveryScanLimit();
    long thresholdMillis = threshold.toEpochMilli();

    List<SliceScan> scans =
        runInTransaction(
            tx -> {
              List<SliceScan> slices = new ArrayList<>();
              for (int slice : RECOVERABLE_SLICES) {
                @Nullable SweepKey resume = recoveryResume.get(new BucketSlice(bucket, slice));
                if (resume != null && resume.sweepAt() > thresholdMillis) {
                  resume = null; // the clock went back past it; start over
                }
                Instant start =
                    resume == null ? Instant.EPOCH : Instant.ofEpochMilli(resume.sweepAt());
                List<SagaStateSnapshot> found = new ArrayList<>();
                // Rows sharing the resume timestamp are skipped by position, not compared by id:
                // the store orders saga ids by its own collation, which need not be Java's.
                int toSkip = resume == null ? 0 : resume.rowsReturnedAt();
                long lastSweepAt = Long.MIN_VALUE;
                int returnedAtLast = 0;
                try (TransactionCrudOperable.Scanner scanner =
                    tx.getScanner(buildSweepRangeScan(bucket, slice, start, true, threshold))) {
                  for (Optional<Result> next = scanner.one();
                      next.isPresent() && found.size() < scanLimit;
                      next = scanner.one()) {
                    Result r = next.get();
                    long sweepAt = r.getBigInt("sweep_at");
                    if (resume != null && sweepAt == resume.sweepAt() && toSkip > 0) {
                      toSkip--; // returned by an earlier pass
                      continue;
                    }
                    found.add(toSweepSnapshot(r));
                    returnedAtLast = sweepAt == lastSweepAt ? returnedAtLast + 1 : 1;
                    lastSweepAt = sweepAt;
                  }
                }
                // A full slice continues after its last row next pass; a short one starts over.
                @Nullable SweepKey resumeAfter = null;
                if (found.size() >= scanLimit) {
                  int returnedAt =
                      resume != null && resume.sweepAt() == lastSweepAt
                          ? resume.rowsReturnedAt() + returnedAtLast
                          : returnedAtLast;
                  resumeAfter = new SweepKey(lastSweepAt, returnedAt);
                }
                slices.add(new SliceScan(slice, found, resumeAfter));
              }
              return slices;
            },
            null,
            "find recoverable sagas");

    List<SagaStateSnapshot> result = new ArrayList<>();
    for (SliceScan scan : scans) {
      result.addAll(scan.sagas());
      BucketSlice at = new BucketSlice(bucket, scan.slice());
      @Nullable SweepKey resumeAfter = scan.resumeAfter();
      if (resumeAfter != null) {
        recoveryResume.put(at, resumeAfter);
      } else {
        recoveryResume.remove(at);
      }
    }
    return new Recoverables(result, step.nextCursor());
  }

  private record BucketSlice(int bucket, int slice) {}

  /**
   * Where a full recovery scan of one slice stopped: the {@code sweep_at} of its last row, and how
   * many rows at that {@code sweep_at} have been returned since the scan last started over.
   */
  private record SweepKey(long sweepAt, int rowsReturnedAt) {}

  private record SliceScan(
      int slice, List<SagaStateSnapshot> sagas, @Nullable SweepKey resumeAfter) {}

  /**
   * {@inheritDoc}
   *
   * <p>The claim is a compare-and-set on the event log: it inserts {@code SAGA_CLAIMED} at the
   * sequence after {@code probe}'s, so anything appended since the probe — another replica's claim,
   * or a step its live driver just finished — makes the insert collide. Every status change appends
   * an event, so the sequence covers those as well. The claim first reads the swept sweep row and
   * refuses if it is gone: a purge frees the sequence, so only the row tells a saga purged since
   * the probe, or recreated under its id, from the one that was swept. A change between the sweep
   * and the probe is caught before writing, by comparing the sweep's snapshot with the state the
   * probe read; otherwise the claim would record the sweep's stale state. A collision or a mismatch
   * is a lost race, never retried: it proves another writer touched the saga. Only a saga handed to
   * recovery at {@code EPOCH} also moves its sweep row, to the claim time, so the next sweep does
   * not offer it again; any other claim leaves the row where it is, and the claim event alone keeps
   * other replicas' probes away for a staleness window.
   */
  @Override
  public Optional<SagaStateSnapshot> claimForRecovery(
      SagaStateSnapshot saga, NewestEvent probe, String newOwnerId) {
    SagaStatus status = saga.getStatus();
    if (!status.isRecoverable()) {
      throw new IllegalArgumentException(
          "Only RUNNING or COMPENSATING sagas are claimed: " + status);
    }
    long sweepAt = saga.getUpdatedAt().toEpochMilli();
    // Compared in the sweep key's unit: the store keeps milliseconds.
    if (probe.state().getStatus() != status
        || probe.state().getUpdatedAt().toEpochMilli() != sweepAt) {
      return Optional.empty();
    }
    String sagaId = saga.getSagaId();
    int bucket = schema.bucketOf(sagaId);
    int slice = SagaSchema.sliceOf(status, false);
    boolean handedOff = saga.getUpdatedAt().equals(Instant.EPOCH);
    int sequence = probe.sequence() + 1;
    String appendId = appendIdSupplier.get();
    // Written by the transaction body and returned by the verifier, so an unknown commit that did
    // land reports the snapshot this claim produced.
    AtomicReference<SagaStateSnapshot> claimedRef = new AtomicReference<>(saga);

    try {
      SagaStateSnapshot claimed =
          runInTransaction(
              tx -> {
                Instant now = nowSupplier.get();
                SagaStateSnapshot result = handedOff ? saga.withTransition(status, now) : saga;
                StateColumns next =
                    new StateColumns(slice, result.getUpdatedAt().toEpochMilli(), result);
                StatusEvent event = StatusEvent.claimed(status, newOwnerId);
                // The sequence alone cannot fence a saga that was purged since the probe: the
                // purge frees the sequence, and a saga recreated under the same id may not have
                // reached it. The swept sweep row is gone in both cases, since its key carries the
                // time.
                if (tx.get(buildSweepGet(bucket, slice, sweepAt, sagaId)).isEmpty()) {
                  throw new SagaConcurrentModificationException(sagaId);
                }
                if (handedOff) {
                  appendTransition(
                      tx, sequence, event, appendId, now, bucket, slice, sweepAt, next);
                } else {
                  tx.insert(buildEventInsert(sagaId, sequence, event, appendId, now, next));
                }
                claimedRef.set(result);
                return result;
              },
              () ->
                  verifyOwnEventCommitted(sagaId, sequence, appendId)
                      ? Optional.of(Objects.requireNonNull(claimedRef.get()))
                      : Optional.empty(),
              "claim saga " + sagaId + " for recovery",
              false);
      return Optional.of(claimed);
    } catch (SagaConcurrentModificationException e) {
      return Optional.empty();
    } catch (SagaPersistenceException e) {
      if (e.getCause() instanceof CommitConflictException
          || e.getCause() instanceof CrudConflictException) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public void markForRecovery(String sagaId) {
    int bucket = schema.bucketOf(sagaId);
    try {
      runInTransaction(
          tx -> {
            Optional<Head> head = readHead(tx, sagaId);
            if (head.isEmpty() || !head.get().state().getStatus().isRecoverable()) {
              // Gone, parked, or finished: only a saga being driven needs handing over.
              return Boolean.TRUE;
            }
            SagaStateSnapshot current = head.get().state();
            int slice = SagaSchema.sliceOf(current.getStatus(), false);
            long sweepAt = current.getUpdatedAt().toEpochMilli();
            if (tx.get(buildSweepGet(bucket, slice, sweepAt, sagaId)).isEmpty()) {
              return Boolean.TRUE; // the log and the sweep table disagree; leave it to recovery
            }
            Instant now = nowSupplier.get();
            SagaStateSnapshot marked = current.withTransition(current.getStatus(), Instant.EPOCH);
            appendTransition(
                tx,
                head.get().newestSequence() + 1,
                StatusEvent.released(current.getStatus()),
                appendIdSupplier.get(),
                now,
                bucket,
                slice,
                sweepAt,
                new StateColumns(slice, 0L, marked));
            return Boolean.TRUE;
          },
          null, // best-effort — no verifier
          "mark for recovery " + sagaId);
    } catch (Exception e) {
      // Best effort — conflict with executing thread is expected and harmless
      logger.warn("markForRecovery failed for saga {} (best-effort)", sagaId, e);
    }
  }

  // ---------------------------------------------------------------------------
  // Parked-step timeout (async WAITING sagas)
  // ---------------------------------------------------------------------------

  @Override
  public OverdueParked findOverdueParkedSagas(Instant threshold, @Nullable ScanCursor cursor) {
    SweepStep step = nextSweepStep(cursor);
    if (step == null) {
      return new OverdueParked(List.of(), null);
    }

    int bucket = step.bucket();
    List<String> sagaIds =
        runInTransaction(
            tx -> {
              // Cap the scan to bound memory; anything beyond is picked up next recovery cycle.
              List<Result> rows =
                  tx.scan(
                      Scan.newBuilder(
                              buildSweepRangeScan(
                                  bucket,
                                  SagaSchema.WAITING_WITH_TIMEOUT_SLICE,
                                  MIN_KEY_INSTANT,
                                  true,
                                  threshold))
                          .limit(config.getRecoveryScanLimit())
                          .build());
              return rows.stream().map(r -> r.getText("saga_id")).toList();
            },
            null,
            "find overdue parked sagas");

    return new OverdueParked(sagaIds, step.nextCursor());
  }

  // ---------------------------------------------------------------------------
  // Data retention
  // ---------------------------------------------------------------------------

  @Override
  public List<SagaStateSnapshot> findByStatusOlderThan(
      SagaStatus status, Instant threshold, int maxResults, String ownerId, int rotation) {
    int numBuckets = schema.getNumBuckets();
    int[] order = SweepScatter.permutation(SweepScatter.seed(ownerId), numBuckets);
    List<SagaStateSnapshot> results = new ArrayList<>();
    int breakerThreshold = Math.min(MAX_CONSECUTIVE_FAILED_BUCKETS, numBuckets);
    int consecutiveFailures = 0;
    for (int i = 0; i < numBuckets && results.size() < maxResults; i++) {
      int bucket = order[Math.floorMod(i + rotation, numBuckets)];
      try {
        results.addAll(
            findByStatusInBucket(bucket, status, threshold, maxResults - results.size()));
        consecutiveFailures = 0;
      } catch (RuntimeException e) {
        // One bucket's scan failure (e.g., a row that cannot be deserialized) must not abandon
        // every bucket after it in the sweep order; skip it and let the next pass retry. But
        // consecutive failures mean the store itself is unavailable, not a poison bucket:
        // rethrow so the pass ends with one ERROR from the caller instead of sweeping the whole
        // ring of an outage at WARN level.
        if (++consecutiveFailures >= breakerThreshold) {
          throw e;
        }
        logger.warn("Scan of bucket {} for status {} failed; skipping it", bucket, status, e);
      }
    }
    return results;
  }

  /**
   * Finds sagas in a specific (bucket, status) slice with {@code updatedAt} older than the
   * threshold.
   */
  private List<SagaStateSnapshot> findByStatusInBucket(
      int bucket, SagaStatus status, Instant threshold, int maxResults) {
    return runInTransaction(
        tx -> {
          List<Result> rows =
              tx.scan(
                  Scan.newBuilder(
                          buildSweepRangeScan(
                              bucket,
                              SagaSchema.sliceOf(status, false),
                              Instant.EPOCH,
                              true,
                              threshold))
                      .limit(maxResults)
                      .build());
          return rows.stream().map(this::toSweepSnapshot).toList();
        },
        null,
        "find sagas by status");
  }

  @Override
  public boolean deleteSaga(String sagaId) {
    int bucket = schema.bucketOf(sagaId);
    // Written by every attempt of the transaction body and read by the commit verifier, so after
    // an unknown commit it can tell "our attempt deleted the saga" from "the saga was already gone
    // before this call ran" — post-state alone cannot distinguish the two.
    AtomicBoolean sawSaga = new AtomicBoolean();
    return runInTransaction(
        tx -> {
          // Only the keys and the state columns are read: payloads are unbounded by default.
          List<Result> events =
              tx.scan(
                  Scan.newBuilder(buildEventScan(sagaId))
                      .projections("saga_id", "sequence", "state_slice", "sweep_at")
                      .build());
          sawSaga.set(!events.isEmpty());
          if (events.isEmpty()) {
            return false;
          }
          // The state is the newest event that carries one; the scan is in ascending order.
          @Nullable Result stateRow = null;
          for (Result r : events) {
            if (isStateRow(r)) {
              stateRow = r;
            }
          }
          if (stateRow == null) {
            throw noStateRow(sagaId);
          }
          SagaStatus status = SagaSchema.statusOfSlice(stateRow.getInt("state_slice"));
          if (!status.isTerminal()) {
            throw new IllegalStateException("Cannot delete saga in non-terminal status: " + status);
          }
          int slice = stateRow.getInt("state_slice");
          long sweepAt = stateRow.getBigInt("sweep_at");
          if (tx.get(buildSweepGet(bucket, slice, sweepAt, sagaId)).isPresent()) {
            tx.delete(buildSweepDelete(bucket, slice, sweepAt, sagaId));
          }
          for (Result r : events) {
            tx.delete(buildEventDelete(sagaId, r.getInt("sequence")));
          }
          // True only when this call removed the saga; an already-purged saga is a no-op so
          // budgeting callers can tell real purges from races with other replicas.
          return true;
        },
        () -> {
          if (!sawSaga.get()) {
            // The attempt never saw the saga, so whatever the commit's fate, this call deleted
            // nothing: an already-purged saga must not be reported as a purge, or budgeting
            // callers would charge work that never happened.
            return Optional.of(Boolean.FALSE);
          }
          // Verify deletion after an unknown commit: the attempt saw the saga, so its absence now
          // means our delete (or an equivalent one) landed; report it as a real purge. If it is
          // still present the commit did not land, and the transaction is retried. A deletion
          // leaves no marker of who performed it, so a racer that also saw the saga but lost the
          // commit may reach here and report true as well, double charging one budget unit. That
          // takes both rarities in one call: a lost race whose commit outcome is also unknown. A
          // cleanly failed commit never gets here — it is retried, and the retry sees the saga
          // already gone and reports false.
          Optional<SagaStateSnapshot> state = loadStateSnapshot(sagaId);
          return state.isEmpty() ? Optional.of(Boolean.TRUE) : Optional.empty();
        },
        "delete saga " + sagaId);
  }

  // ---------------------------------------------------------------------------
  // Transaction execution helper
  // ---------------------------------------------------------------------------

  /** Action to run within a transaction. */
  @FunctionalInterface
  interface TransactionAction<T> {
    T run(DistributedTransaction tx) throws Exception;
  }

  /** Verifier to check whether a transaction was committed after an unknown status. */
  @FunctionalInterface
  interface CommitVerifier<T> {
    Optional<T> verify() throws Exception;
  }

  /**
   * Runs the given action in a ScalarDB transaction with retry logic.
   *
   * <p>Retries on {@link TransactionException} (including {@link CrudConflictException} and {@link
   * CommitConflictException}). On {@link UnknownTransactionStatusException}, uses the commit
   * verifier to determine whether the transaction was committed. If the verifier is {@code null},
   * the entire transaction is retried.
   *
   * @param action the transaction action to run
   * @param commitVerifier verifier to check commit status on UTSE, or {@code null} to retry the
   *     whole transaction
   * @param operationName description for error messages
   * @return the result of the action
   */
  <T> T runInTransaction(
      TransactionAction<T> action,
      @Nullable CommitVerifier<T> commitVerifier,
      String operationName) {
    return runInTransaction(action, commitVerifier, operationName, true, null);
  }

  /**
   * Runs an event-append transaction, adding append-family collision classification to the plain
   * retry loop: when retries are exhausted on a conflict, whether it surfaced during CRUD or at
   * commit, the verifier is asked once more, and a {@link SagaConcurrentModificationException} from
   * it is a proven collision for {@code sagaId} rather than the retryable {@link
   * SagaPersistenceException}. Only the append family — {@code recordStepEvent}, {@code
   * recordStatusEvent}, {@code park}, {@code transitionParkedStep} — uses this, since only there
   * does a taken sequence mean another writer rather than a store failure.
   *
   * <p>The classification is only as sound as the verifier passed in, which is what actually
   * performs the check: it must identify our own write by its {@code append_id} and throw {@link
   * SagaConcurrentModificationException} when a foreign one holds the sequence. Pass {@link
   * #verifyOwnAppendCommitted}, or a lambda over {@link #verifyOwnEventCommitted} as {@code
   * recordStepEvent} does. A verifier that cannot tell writers apart will either claim collisions
   * it has not proven or never report one. See {@link #verifyAfterExhaustedConflict}.
   */
  <T> T runInTransaction(
      TransactionAction<T> action,
      @Nullable CommitVerifier<T> commitVerifier,
      String operationName,
      String sagaId) {
    return runInTransaction(action, commitVerifier, operationName, true, sagaId);
  }

  /**
   * Runs a transaction with an option to skip retry on a conflict.
   *
   * @param action the transaction action to run
   * @param commitVerifier verifier to check commit status on UTSE, or {@code null} to retry the
   *     whole transaction
   * @param operationName description for error messages
   * @param retryOnConflict if {@code false}, neither {@link CommitConflictException} nor {@link
   *     CrudConflictException} is retried
   * @return the result of the action
   */
  <T> T runInTransaction(
      TransactionAction<T> action,
      @Nullable CommitVerifier<T> commitVerifier,
      String operationName,
      boolean retryOnConflict) {
    return runInTransaction(action, commitVerifier, operationName, retryOnConflict, null);
  }

  /**
   * Shared impl. A non-null {@code sagaId} opts in to append-family collision mapping: retries
   * exhausted on a conflict are settled by one verifier read, which can surface {@link
   * SagaConcurrentModificationException} for that saga instead of the retryable {@link
   * SagaPersistenceException}. Null, or no verifier to gather evidence with, keeps the generic
   * exhaustion behavior.
   */
  private <T> T runInTransaction(
      TransactionAction<T> action,
      @Nullable CommitVerifier<T> commitVerifier,
      String operationName,
      boolean retryOnConflict,
      @Nullable String sagaId) {
    int maxAttempts = config.getTransactionRetryCount();
    Exception lastException = null;

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        sleepForRetry(attempt - 1);
      }
      DistributedTransaction tx = null;
      try {
        tx = txManager.begin();
        T result = action.run(tx);
        tx.commit();
        return result;
      } catch (UnknownTransactionStatusException e) {
        logger.warn(
            "Unknown transaction status for {} (txId={})",
            operationName,
            e.getTransactionId().orElse("unknown"),
            e);
        if (commitVerifier == null) {
          // Read-only — just retry the whole transaction
          lastException = e;
          continue;
        }
        for (int v = 0; v < maxAttempts; v++) {
          try {
            Optional<T> verified = commitVerifier.verify();
            if (verified.isPresent()) {
              return verified.get();
            }
            break; // Verified not committed — retry the transaction
          } catch (Exception ve) {
            // A permanent (non-retryable) persistence failure — e.g. a deserialization or parse
            // error while verifying — fails identically on every attempt; propagate it as-is
            // instead of retrying and masking it as the retryable failure thrown below.
            if (ve instanceof SagaPersistenceException pe && !pe.isRetryable()) {
              throw pe;
            }
            // Business-logic or programming errors propagate immediately.
            // Only a retryable SagaPersistenceException (infrastructure failure from
            // inner transactions) and checked exceptions are retried.
            if (ve instanceof RuntimeException re && !(ve instanceof SagaPersistenceException)) {
              throw re;
            }
            e.addSuppressed(ve);
            if (v < maxAttempts - 1) {
              sleepForRetry(v);
              continue;
            }
            throw SagaPersistenceException.storeUnavailable(e);
          }
        }
        lastException = e;
      } catch (CommitConflictException | CrudConflictException e) {
        abortQuietly(tx);
        if (!retryOnConflict) {
          logger.debug(
              "Conflict for {} (txId={})", operationName, e.getTransactionId().orElse("unknown"));
          // #41 widened this catch from commit conflicts to CRUD conflicts too, hence the broader
          // log wording. The message it passed to the retired retryable(message, cause) factory is
          // now supplied by PERSISTENCE_STORE_UNAVAILABLE itself.
          throw SagaPersistenceException.storeUnavailable(e);
        }
        logger.debug(
            "Conflict for {} (txId={}), retrying",
            operationName,
            e.getTransactionId().orElse("unknown"));
        lastException = e;
      } catch (TransactionException e) {
        abortQuietly(tx);
        logger.warn(
            "Transaction failed for {} (txId={})",
            operationName,
            e.getTransactionId().orElse("unknown"),
            e);
        lastException = e;
      } catch (Exception e) {
        abortQuietly(tx);
        if (e instanceof RuntimeException re) {
          throw re;
        }
        throw SagaPersistenceException.storeUnavailable(e);
      }
    }
    logger.warn("All {} attempts exhausted for {}", maxAttempts, operationName, lastException);
    Exception cause = Objects.requireNonNull(lastException);
    if (sagaId != null
        && commitVerifier != null
        && (cause instanceof CommitConflictException || cause instanceof CrudConflictException)) {
      Optional<T> verified = verifyAfterExhaustedConflict(commitVerifier, cause, operationName);
      if (verified.isPresent()) {
        return verified.get();
      }
    }
    throw SagaPersistenceException.storeUnavailable(cause);
  }

  /**
   * Settles an append-family operation that exhausted its retries on a conflict, by reading back
   * the sequence instead of inferring an outcome from the exception type.
   *
   * <p>A commit conflict is a definite abort, so our write did not land. It does not follow that
   * another writer's did: two writers contending on the same rows can both conflict and both
   * exhaust with neither committing. Every attempt re-runs the action from scratch, and the
   * transition ops re-read the saga's state each time, so reaching here means we never once
   * observed another writer take it; {@code recordStepEvent} is a bare append that reads nothing
   * and so observes nothing at all. Either way this is the weakest evidence of a collision in this
   * class, not the strongest.
   *
   * <p>A CRUD conflict aborts the attempt just as a commit conflict does, and says just as little
   * about who won. The transition ops raise it when their pre-check read finds a record another
   * transaction has prepared but not resolved, and that writer may still abort and leave the
   * sequence free. Which layer reported the abort is not evidence either, so both are settled the
   * same way rather than one being trusted over the other.
   *
   * <p>The verifier settles it from what is actually persisted. It throws {@link
   * SagaConcurrentModificationException} when a foreign {@code append_id} holds our sequence, which
   * is a proven collision (409). It reports "not committed" when the sequence is still free, and
   * that stays the retryable exhaustion failure (503) so the caller retries rather than being told
   * a race it may have won was lost. It can also confirm our own {@code append_id}: an earlier
   * attempt whose status was unknown may since have been rolled forward, which is what makes the
   * later attempt conflict, and that operation succeeded.
   *
   * <p>A failure of the read itself proves nothing either way, so it is suppressed onto {@code
   * cause} rather than replacing it: the operation did fail with a retryable conflict, and
   * reporting that is more truthful than reporting how the evidence read broke.
   */
  private <T> Optional<T> verifyAfterExhaustedConflict(
      CommitVerifier<T> commitVerifier, Exception cause, String operationName) {
    try {
      return commitVerifier.verify();
    } catch (SagaConcurrentModificationException e) {
      throw e;
    } catch (Exception e) {
      logger.warn(
          "Collision check for {} failed after conflicts exhausted the retries", operationName, e);
      cause.addSuppressed(e);
      return Optional.empty();
    }
  }

  private void sleepForRetry(int retryIndex) {
    try {
      long delay = Math.min(100L * (1L << retryIndex), 5000L);
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // OPERATION_ABORTED, not storeUnavailable: the interrupt means this server is abandoning
      // the operation (typically shutdown), and reporting it as a store outage would emit false
      // store alarms on every deploy. Still retryable — the retry lands elsewhere.
      throw SagaPersistenceException.operationAborted(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  // -- saga_sweep builders --

  private Get buildSweepGet(int bucket, int slice, long sweepAt, String sagaId) {
    return Get.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.SWEEP_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(sweepClusteringKey(slice, sweepAt, sagaId))
        .build();
  }

  private Delete buildSweepDelete(int bucket, int slice, long sweepAt, String sagaId) {
    return Delete.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.SWEEP_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(sweepClusteringKey(slice, sweepAt, sagaId))
        .build();
  }

  private Insert buildSweepInsert(int bucket, int slice, long sweepAt, SagaStateSnapshot snapshot) {
    return Insert.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.SWEEP_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(sweepClusteringKey(slice, sweepAt, snapshot.getSagaId()))
        .textValue("saga_name", snapshot.getSagaName())
        .textValue("definition_version", snapshot.getDefinitionVersion())
        .timestampTZValue("created_at", snapshot.getCreatedAt())
        .timestampTZValue("updated_at", snapshot.getUpdatedAt())
        .build();
  }

  /**
   * Appends {@code event} carrying the saga's new state, and moves the saga's sweep row from its
   * old key to the one that state names. The caller has read the old row in the same transaction,
   * so the delete needs no read of its own and conflicts with any other writer moving the same row.
   */
  private void appendTransition(
      DistributedTransaction tx,
      int sequence,
      SagaEvent event,
      String appendId,
      Instant now,
      int bucket,
      int oldSlice,
      long oldSweepAt,
      StateColumns next)
      throws Exception {
    String sagaId = next.snapshot().getSagaId();
    tx.insert(buildEventInsert(sagaId, sequence, event, appendId, now, next));
    tx.delete(buildSweepDelete(bucket, oldSlice, oldSweepAt, sagaId));
    tx.insert(buildSweepInsert(bucket, next.slice(), next.sweepAt(), next.snapshot()));
  }

  /**
   * Builds a {@code sweep_at}-range scan over one {@code (bucket, slice)} of {@code saga_sweep},
   * from {@code [startTs .. endInclusive]} on the second clustering-key column ({@code slice}
   * fixed). ScalarDB only allows ranging on the last specified clustering key, which is why the
   * admin listing paginates by whole cohorts rather than a {@code saga_id} keyset (see {@link
   * #listStateSnapshots}).
   */
  private Scan buildSweepRangeScan(
      int bucket, int slice, Instant startTs, boolean startInclusive, Instant endInclusive) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.SWEEP_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .start(
            Key.newBuilder()
                .addInt("slice", slice)
                .addBigInt("sweep_at", startTs.toEpochMilli())
                .build(),
            startInclusive)
        .end(
            Key.newBuilder()
                .addInt("slice", slice)
                .addBigInt("sweep_at", endInclusive.toEpochMilli())
                .build(),
            true)
        .build();
  }

  private static Key sweepClusteringKey(int slice, long sweepAt, String sagaId) {
    return Key.newBuilder()
        .addInt("slice", slice)
        .addBigInt("sweep_at", sweepAt)
        .addText("saga_id", sagaId)
        .build();
  }

  /** A sweep row as a snapshot; see {@link #toStateSnapshot} for how {@code updatedAt} is read. */
  private SagaStateSnapshot toSweepSnapshot(Result r) {
    int slice = r.getInt("slice");
    Instant updatedAt =
        slice == SagaSchema.WAITING_WITH_TIMEOUT_SLICE
            ? r.getTimestampTZ("updated_at")
            : Instant.ofEpochMilli(r.getBigInt("sweep_at"));
    return new SagaStateSnapshot(
        r.getText("saga_id"),
        r.getText("saga_name"),
        SagaSchema.statusOfSlice(slice),
        r.getText("definition_version"),
        r.getTimestampTZ("created_at"),
        updatedAt);
  }

  // -- the saga's state, read from and written to the event log --

  /** The state columns a state-changing event carries; see {@link SagaSchema#sagaEventsTable()}. */
  private record StateColumns(int slice, long sweepAt, SagaStateSnapshot snapshot) {}

  /** The sweep key of a parked saga, as its {@code STEP_PENDING} event recorded it. */
  private record ParkedState(int slice, long sweepAt) {}

  /**
   * What a newest-first read of a saga's events finds: the newest event, the saga's state (the
   * newest event that carries one), and when the saga was last claimed if no status change has
   * happened since.
   */
  private record Head(
      int newestSequence,
      EventType newestType,
      @Nullable Instant newestCreatedAt,
      SagaStateSnapshot state,
      @Nullable Instant lastClaimedAt) {}

  private Scan buildNewestFirstScan(String sagaId) {
    return Scan.newBuilder(buildEventScan(sagaId)).ordering(Scan.Ordering.desc("sequence")).build();
  }

  /**
   * Reads a saga's events newest-first until it reaches the newest one that carries the saga's
   * state, and stops there. A plain step event carries none, so the read passes as many of them as
   * the saga has appended since its last status change. Consensus Commit fetches rows from storage
   * a page at a time ({@code scalar.db.scan_fetch_size}), so this is one round trip unless that
   * many step events exceed a page.
   *
   * @return the head, or empty when the saga has no events
   */
  private Optional<Head> readHead(DistributedTransaction tx, String sagaId) throws Exception {
    try (TransactionCrudOperable.Scanner scanner = tx.getScanner(buildNewestFirstScan(sagaId))) {
      @Nullable Result newest = null;
      for (Optional<Result> next = scanner.one(); next.isPresent(); next = scanner.one()) {
        Result r = next.get();
        if (newest == null) {
          newest = r;
        }
        if (isStateRow(r)) {
          EventType stateType = eventTypeOf(r);
          @Nullable Instant lastClaimedAt =
              stateType == EventType.SAGA_CLAIMED ? r.getTimestampTZ("created_at") : null;
          return Optional.of(
              new Head(
                  newest.getInt("sequence"),
                  eventTypeOf(newest),
                  newest.getTimestampTZ("created_at"),
                  toStateSnapshot(sagaId, r),
                  lastClaimedAt));
        }
      }
      if (newest == null) {
        return Optional.empty();
      }
      throw noStateRow(sagaId);
    }
  }

  private static boolean isStateRow(Result r) {
    return !r.isNull("state_slice");
  }

  /**
   * The saga's state as a state-carrying event recorded it. {@code updatedAt} is {@code sweep_at},
   * except in the slice of parked steps with a timeout, where {@code sweep_at} is the deadline and
   * {@code updatedAt} is the park time, the event's own {@code created_at}.
   */
  private static SagaStateSnapshot toStateSnapshot(String sagaId, Result r) {
    int slice = r.getInt("state_slice");
    Instant updatedAt =
        slice == SagaSchema.WAITING_WITH_TIMEOUT_SLICE
            ? r.getTimestampTZ("created_at")
            : Instant.ofEpochMilli(r.getBigInt("sweep_at"));
    return new SagaStateSnapshot(
        sagaId,
        r.getText("saga_name"),
        SagaSchema.statusOfSlice(slice),
        r.getText("definition_version"),
        r.getTimestampTZ("saga_created_at"),
        updatedAt);
  }

  /** A saga with events but none that carries its state: the store never writes one. */
  private static SagaPersistenceException noStateRow(String sagaId) {
    return SagaPersistenceException.deserializationFailed(
        new IllegalStateException("Saga " + sagaId + " has events but none carries its state"));
  }

  /**
   * Rejects a {@code WAITING} snapshot for a write that derives the saga's sweep key from the
   * snapshot: a parked saga's key may be its deadline, which the snapshot does not carry.
   */
  private static void requireNotWaiting(SagaStateSnapshot current) {
    if (current.getStatus() == SagaStatus.WAITING) {
      throw new IllegalArgumentException(
          "A WAITING saga leaves that state only by a resume, a give-up or a re-drive: "
              + current.getSagaId());
    }
  }

  // -- saga_definitions builders --

  /**
   * Returns a {@code registered_at} strictly after every version already registered under {@code
   * name}, so registration order is what the latest-version lookup reads.
   *
   * <p>The lookup picks the greatest {@code registered_at}, and the value comes from whichever
   * replica happens to serve the registration. Wall clocks across replicas disagree — by seconds,
   * routinely — so a replica running behind could register a NEWER version with an OLDER stamp and
   * lose the selection race to the version it replaces, leaving the version it was meant to replace
   * still serving.
   *
   * <p>The scan runs inside the caller's transaction, so two replicas registering different
   * versions of the same saga at once read overlapping rows and the transaction layer has what it
   * needs to make one of them lose. How far that goes is the store's to decide: whether a scan
   * carries phantom protection is a property of the transaction implementation, not of this method.
   * What this method guarantees on its own is the single-writer case — a clock behind the latest
   * row still stamps after it.
   */
  private Instant monotonicStamp(DistributedTransaction tx, String name) throws Exception {
    // Truncated to what the column stores. TIMESTAMPTZ does not keep sub-millisecond precision, so
    // comparing the raw clock against a value read back from the store compares two different
    // things: a clock reading a fraction of a millisecond past the latest row would look strictly
    // later, then persist as the same millisecond and tie it — leaving the ordering this method
    // exists to guarantee up to whichever row the latest-version scan happens to see first.
    Instant now = nowSupplier.get().truncatedTo(ChronoUnit.MILLIS);
    Instant latest =
        newestRegistration(tx.scan(buildDefinitionScan(name)))
            .map(ScalarDbSagaStore::registeredAt)
            .orElse(null);
    if (latest == null || now.isAfter(latest)) {
      return now;
    }
    // The smallest step the column can still tell apart.
    return latest.plusMillis(1);
  }

  private Insert buildDefinitionInsert(String name, String version, String json, Instant stamp) {
    return Insert.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.DEFINITIONS_TABLE)
        .partitionKey(Key.ofText("saga_name", name))
        .clusteringKey(Key.ofText("definition_version", version))
        .textValue("definition_json", json)
        .timestampTZValue("registered_at", stamp)
        .build();
  }

  private Scan buildDefinitionScan(String sagaName) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.DEFINITIONS_TABLE)
        .partitionKey(Key.ofText("saga_name", sagaName))
        .build();
  }

  private Get buildDefinitionGet(String sagaName, String definitionVersion) {
    return Get.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.DEFINITIONS_TABLE)
        .partitionKey(Key.ofText("saga_name", sagaName))
        .clusteringKey(Key.ofText("definition_version", definitionVersion))
        .build();
  }

  // -- saga_events builders --

  private Scan buildEventScan(String sagaId) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.EVENTS_TABLE)
        .partitionKey(Key.ofText("saga_id", sagaId))
        .build();
  }

  /**
   * Builds an event insert. A state-changing event also carries the saga's state after it ({@code
   * state}); a plain step event passes {@code null} and leaves those columns out of the write
   * entirely, so Cassandra writes no tombstones for them.
   */
  private Insert buildEventInsert(
      String sagaId,
      int sequence,
      SagaEvent event,
      String appendId,
      Instant now,
      @Nullable StateColumns state) {
    var builder =
        Insert.newBuilder()
            .namespace(SagaSchema.NAMESPACE)
            .table(SagaSchema.EVENTS_TABLE)
            .partitionKey(Key.ofText("saga_id", sagaId))
            .clusteringKey(Key.ofInt("sequence", sequence))
            .textValue("event_type", event.getEventType().name())
            .textValue("append_id", appendId)
            .textValue("payload", event.getPayload())
            .timestampTZValue("created_at", now);
    switch (event) {
      case StatusEvent se -> builder.intValue("step_index", -1);
      case StepEvent ste ->
          builder
              .intValue("step_index", ste.getStepIndex())
              .textValue("step_name", ste.getStepName());
    }
    if (state != null) {
      SagaStateSnapshot snapshot = state.snapshot();
      builder
          .intValue("state_slice", state.slice())
          .bigIntValue("sweep_at", state.sweepAt())
          .textValue("saga_name", snapshot.getSagaName())
          .textValue("definition_version", snapshot.getDefinitionVersion())
          .timestampTZValue("saga_created_at", snapshot.getCreatedAt());
    }
    return builder.build();
  }

  private Get buildEventGet(String sagaId, int sequence) {
    return Get.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.EVENTS_TABLE)
        .partitionKey(Key.ofText("saga_id", sagaId))
        .clusteringKey(Key.ofInt("sequence", sequence))
        .build();
  }

  private Delete buildEventDelete(String sagaId, int sequence) {
    return Delete.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.EVENTS_TABLE)
        .partitionKey(Key.ofText("saga_id", sagaId))
        .clusteringKey(Key.ofInt("sequence", sequence))
        .build();
  }

  private SagaEvent toSagaEvent(Result r) {
    int stepIndex = r.getInt("step_index");
    @Nullable String stepName = r.isNull("step_name") ? null : r.getText("step_name");
    @Nullable String payload = r.isNull("payload") ? null : r.getText("payload");
    Instant createdAt = r.getTimestampTZ("created_at");

    EventType eventType = eventTypeOf(r);

    if (stepIndex >= 0) {
      String name = Objects.requireNonNull(stepName, "stepName must not be null for step events");
      StepEvent event =
          switch (eventType) {
            case STEP_PENDING -> StepEvent.pending(stepIndex, name);
            case STEP_REISSUING -> StepEvent.reissuing(stepIndex, name);
            case STEP_COMPLETED -> StepEvent.completed(stepIndex, name, payload);
            case STEP_FAILED -> StepEvent.failed(stepIndex, name, payload);
            case STEP_COMPENSATED -> StepEvent.compensated(stepIndex, name);
            case STEP_COMPENSATION_FAILED -> StepEvent.compensationFailed(stepIndex, name, payload);
            default ->
                throw SagaPersistenceException.deserializationFailed(
                    new IllegalStateException("Unknown step event type: " + eventType));
          };
      return event.withTimestamp(createdAt);
    } else {
      StatusEvent event =
          switch (eventType) {
            case SAGA_STARTED -> StatusEvent.started(payload);
            case SAGA_COMPENSATING -> StatusEvent.compensating();
            case SAGA_COMPLETED -> StatusEvent.completed();
            case SAGA_COMPENSATED -> StatusEvent.compensated();
            case SAGA_ESCALATED -> StatusEvent.escalated(payload != null ? payload : "");
            case SAGA_FORCE_COMPLETED ->
                StatusEvent.reconstruct(eventType, SagaStatus.COMPLETED, payload);
            case SAGA_RECOVERING, SAGA_RESET ->
                StatusEvent.reconstruct(eventType, AdminAuditPayload.target(payload), payload);
            case SAGA_CLAIMED, SAGA_RELEASED -> {
              if (!isStateRow(r)) {
                throw SagaPersistenceException.deserializationFailed(
                    new IllegalStateException(eventType + " event carries no state"));
              }
              yield StatusEvent.reconstruct(
                  eventType, SagaSchema.statusOfSlice(r.getInt("state_slice")), payload);
            }
            default ->
                throw SagaPersistenceException.deserializationFailed(
                    new IllegalStateException("Unknown saga event type: " + eventType));
          };
      return event.withTimestamp(createdAt);
    }
  }

  /** The stored event type, or a deserialization failure for a type this version does not know. */
  private static EventType eventTypeOf(Result r) {
    String eventTypeStr = Objects.requireNonNull(r.getText("event_type"), "event_type");
    try {
      return EventType.valueOf(eventTypeStr);
    } catch (IllegalArgumentException e) {
      // During a rolling upgrade an older replica reading a type only a newer one writes lands
      // here; a raw IllegalArgumentException would escape the store's methods unwrapped.
      throw SagaPersistenceException.deserializationFailed(e);
    }
  }

  private Optional<SagaStateSnapshot> loadStateSnapshot(String sagaId) {
    return runInTransaction(
        tx -> readHead(tx, sagaId).map(Head::state), null, "load saga state " + sagaId);
  }

  private void validateSagaId(String sagaId) {
    if (!SAGA_ID_PATTERN.matcher(sagaId).matches()) {
      throw new SagaIllegalArgumentException(
          "Invalid saga ID format (must match [a-zA-Z0-9._-]{1,128})");
    }
  }

  private void validatePayloadSize(@Nullable String payload) {
    int limit = config.getMaxEventPayloadBytes();
    if (limit > 0 && payload != null) {
      int byteSize = payload.getBytes(StandardCharsets.UTF_8).length;
      if (byteSize > limit) {
        // SagaIllegalArgumentException, not the bare stdlib type: the wire mappers answer a bare
        // one with a 500 carrying no detail at all, and the limit is configurable, so the actual
        // size and bound are exactly what a remote caller cannot guess.
        throw new SagaIllegalArgumentException(
            "Event payload exceeds limit: " + byteSize + " bytes > " + limit);
      }
    }
  }

  private String toJson(Object obj) {
    try {
      return objectMapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw SagaPersistenceException.serializationFailed(e);
    }
  }

  private void abortQuietly(@Nullable DistributedTransaction tx) {
    if (tx != null) {
      try {
        tx.abort();
      } catch (AbortException e) {
        logger.debug("Failed to abort transaction", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Cursor implementation
  // ---------------------------------------------------------------------------

  /** Internal cursor tracking which bucket to scan next. */
  private record BucketCursor(int nextBucket) implements ScanCursor {}

  /**
   * Cursor for a scattered sweep: {@code position} indexes into the bucket permutation derived from
   * {@code seed} (see {@link SweepScatter}), so the cursor stays small while the full order is
   * recomputed cheaply per call.
   */
  private record PermutedBucketCursor(int seed, int position) implements ScanCursor {}

  /** One resolved sweep step: the bucket to scan and the cursor for the following bucket. */
  private record SweepStep(int bucket, @Nullable ScanCursor nextCursor) {}

  /**
   * Result of scanning one {@code (bucket, status)} slice: the rows to emit, and — when the slice
   * was not drained — the last <b>complete</b> {@code updated_at} to resume after. A {@code null}
   * {@link #resumeTs()} means the slice was fully drained within {@code [.., updatedBefore]}.
   */
  private record SliceResult(List<SagaStateSnapshot> rows, @Nullable Instant resumeTs) {}

  /**
   * The wire-portable position of an Admin listing: the {@code (bucket, slice, sweep_at)} of the
   * last fully-returned cohort. There is deliberately no {@code saga_id} — pagination never
   * compares that TEXT column, since ScalarDB does not normalize its collation (see {@link
   * #listStateSnapshots}).
   *
   * <p>The token also carries a {@link #filterKey(SagaQuery) filter key} — the normalized status
   * and time-window filters that produced it. On decode the key must match the current query's
   * filters, so a token minted under one filter set (say {@code status=RUNNING}) cannot silently
   * resume a different query (say unfiltered, or a wider time window): widening the filters would
   * otherwise skip earlier buckets and status slices, and a changed lower time bound would be
   * overridden by the cursor timestamp and return out-of-window rows.
   *
   * <p>Encoded as an opaque, versioned Base64URL token; decoding is fail-closed — any malformed,
   * out-of-range, unknown-version, or filter-mismatched token throws {@link
   * SagaIllegalArgumentException} (mapped to 400) rather than silently scanning the wrong data. The
   * token carries only a scan position and filter key within already-authorized data, so it is not
   * signed.
   */
  private record PageCursor(int bucket, int slice, Instant sweepAt) {

    private static final String DELIMITER = "|";

    /** Filter-key token for "no status filter" (list every status). */
    private static final String ANY_STATUS = "*";

    /** Filter-key token for an absent time bound (no lower or no upper bound). */
    private static final String NO_BOUND = "-";

    /**
     * Defensive upper bound on the encoded token length, checked before Base64 decoding so the core
     * library is self-defending even when called outside the daemon's request-size limits. A valid
     * token is well under this (~160 chars today, and still comfortably under even once a future
     * intra-cohort {@code saga_id} is added); this only rejects absurd input, it is not a tight
     * format check.
     */
    private static final int MAX_ENCODED_LENGTH = 512;

    /**
     * The normalized status and time-window filters, as a canonical {@code status|after|before}
     * string embedded in the token and re-checked on decode. Two queries share a key exactly when
     * they sweep the same slices over the same window, so a key mismatch means the token belongs to
     * a different query. The sub-parts use the same {@link #DELIMITER} as the outer payload, which
     * is safe: none of {@link SagaStatus} codes (integers) nor {@link Instant} strings (ISO-8601)
     * contain {@code "|"}.
     */
    static String filterKey(SagaQuery query) {
      SagaStatus statusFilter = query.getStatus();
      Instant updatedAfter = query.getUpdatedAfter();
      Instant updatedBefore = query.getUpdatedBefore();
      String status =
          statusFilter == null ? ANY_STATUS : Integer.toString(statusFilter.getStatusCode());
      String after = updatedAfter == null ? NO_BOUND : updatedAfter.toString();
      String before = updatedBefore == null ? NO_BOUND : updatedBefore.toString();
      return String.join(DELIMITER, status, after, before);
    }

    String encode(String filterKey) {
      String payload =
          String.join(
              DELIMITER,
              PAGE_TOKEN_VERSION,
              filterKey, // three sub-parts: status, after, before
              Integer.toString(bucket),
              Integer.toString(slice),
              sweepAt.toString());
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }

    static PageCursor decode(
        String token, int numBuckets, int[] allowedSlices, String expectedFilterKey) {
      // Reject an oversized token before allocating its decoded bytes (defense in depth; the daemon
      // also bounds request size).
      if (token.length() > MAX_ENCODED_LENGTH) {
        throw new SagaIllegalArgumentException("Page token too long");
      }
      String payload;
      try {
        payload = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
      } catch (IllegalArgumentException e) {
        throw new SagaIllegalArgumentException("Malformed page token", e);
      }
      // version | status | after | before | bucket | slice | sweepAt
      String[] parts = payload.split(Pattern.quote(DELIMITER), 7);
      if (parts.length != 7 || !PAGE_TOKEN_VERSION.equals(parts[0])) {
        throw new SagaIllegalArgumentException("Unrecognized page token");
      }
      String filterKey = String.join(DELIMITER, parts[1], parts[2], parts[3]);
      if (!expectedFilterKey.equals(filterKey)) {
        throw new SagaIllegalArgumentException("Page token does not match the query");
      }
      int bucket;
      int slice;
      Instant sweepAt;
      try {
        bucket = Integer.parseInt(parts[4]);
        slice = Integer.parseInt(parts[5]);
        sweepAt = Instant.parse(parts[6]);
      } catch (RuntimeException e) {
        throw new SagaIllegalArgumentException("Malformed page token", e);
      }
      if (bucket < 0 || bucket >= numBuckets) {
        throw new SagaIllegalArgumentException("Page token bucket out of range");
      }
      // Defense in depth: even with a matching filter key the token is unsigned, so guard the
      // resume math against a tampered slice outside the swept set (would index out of bounds).
      if (indexOf(allowedSlices, slice) < 0) {
        throw new SagaIllegalArgumentException("Page token does not match the query");
      }
      // Held to the same range as the query bounds, since it becomes the next scan's start key.
      requireInKeyRange(sweepAt, "Page token sweepAt");
      return new PageCursor(bucket, slice, sweepAt);
    }
  }
}
