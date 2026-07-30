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
import com.scalar.db.io.TimestampTZColumn;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScalarDB-backed implementation of {@link SagaStore}.
 *
 * <p>Uses two tables ({@code saga_events} and {@code saga_state}) for saga persistence and a third
 * ({@code saga_definitions}) for definition storage. All state-mutating operations use ScalarDB
 * transactions for atomicity.
 *
 * <p>All transaction operations are executed through {@link #runInTransaction}, which provides
 * unified retry logic for {@link TransactionException} (including {@link
 * UnknownTransactionStatusException}). A conflict is retried by default, whether it surfaces during
 * CRUD as {@link CrudConflictException} or at commit as {@link CommitConflictException}; retry can
 * be disabled per operation (e.g., {@code createSaga} treats a conflict as permanent).
 */
public class ScalarDbSagaStore implements SagaStore {

  private static final Logger logger = LoggerFactory.getLogger(ScalarDbSagaStore.class);

  private static final Pattern SAGA_ID_PATTERN = Pattern.compile("[a-zA-Z0-9._-]{1,128}");
  private static final int[] RECOVERABLE_STATUS_CODES =
      java.util.Arrays.stream(SagaStatus.values())
          .filter(SagaStatus::isRecoverable)
          .mapToInt(SagaStatus::getStatusCode)
          .toArray();

  /**
   * All status codes in ascending order — the stable sweep order for an all-status admin listing.
   */
  private static final int[] ALL_STATUS_CODES =
      java.util.Arrays.stream(SagaStatus.values())
          .mapToInt(SagaStatus::getStatusCode)
          .sorted()
          .toArray();

  /** Format version prefix for the opaque list page token. */
  private static final String PAGE_TOKEN_VERSION = "1";

  private final DistributedTransactionManager txManager;
  private final ObjectMapper objectMapper;
  private final SagaSchema schema;
  private final ScalarDbSagaStoreConfig config;
  private final SagaDefinitionSerializer definitionSerializer;
  private final Supplier<String> appendIdSupplier;

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
    this(txManager, objectMapper, schema, config, () -> UUID.randomUUID().toString());
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
    this.txManager = Objects.requireNonNull(txManager, "txManager must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.schema = Objects.requireNonNull(schema, "schema must not be null");
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.appendIdSupplier =
        Objects.requireNonNull(appendIdSupplier, "appendIdSupplier must not be null");
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
    // Minted for schema uniformity — every saga_events row has an append_id — even though
    // createSaga's verifier is state-row based, not append-id based.
    String appendId = appendIdSupplier.get();

    try {
      return runInTransaction(
          tx -> {
            Instant now = Instant.now();
            tx.insert(buildEventInsert(id, 0, startedEvent, appendId, now));
            SagaStateSnapshot snapshot =
                new SagaStateSnapshot(
                    id, sagaName, SagaStatus.RUNNING, ownerId, definitionVersion, now, now);
            tx.insert(buildStateInsert(bucket, snapshot));
            return snapshot;
          },
          () -> loadStateSnapshot(id),
          "create saga " + id,
          false);
    } catch (SagaPersistenceException e) {
      Optional<SagaStateSnapshot> existing = Optional.empty();
      try {
        existing = getStateSnapshot(id);
      } catch (Exception lookupEx) {
        e.addSuppressed(lookupEx);
      }
      if (existing.isPresent()) {
        throw new SagaAlreadyExistsException(id, existing.get(), e);
      }
      throw e;
    }
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

          tx.insert(buildDefinitionInsert(name, version, json));
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

  @Override
  public Optional<SagaDefinition> getDefinition(String sagaName) {
    return runInTransaction(
        tx -> {
          List<Result> results = tx.scan(buildDefinitionScan(sagaName));
          return results.stream()
              .max(Comparator.comparing(r -> r.getTimestampTZ("registered_at")))
              .map(r -> definitionSerializer.deserialize(r.getText("definition_json")));
        },
        null, // read-only
        "get latest definition " + sagaName);
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
          tx.insert(buildEventInsert(sagaId, sequence, event, appendId, Instant.now()));
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
    String sagaId = current.getSagaId();
    SagaStatus newStatus = event.getTargetStatus();
    int bucket = schema.bucketOf(sagaId);
    String appendId = appendIdSupplier.get();

    return runInTransaction(
        tx -> {
          Instant now = Instant.now();

          // Verify the row still exists at the snapshot's CK.
          // Because status and updated_at are part of the clustering key,
          // every mutation DELETEs the old CK and INSERTs at a new CK.
          // If the row exists here, no other replica has touched it.
          int oldStatus = current.getStatus().getStatusCode();
          Optional<Result> row =
              tx.get(buildStateGet(bucket, oldStatus, current.getUpdatedAt(), sagaId));
          if (row.isEmpty()) {
            throw new SagaConcurrentModificationException(sagaId);
          }

          tx.insert(buildEventInsert(sagaId, sequence, event, appendId, now));
          tx.delete(buildStateDelete(bucket, oldStatus, current.getUpdatedAt(), sagaId));
          // The event's audit timestamp is always the real time; the state row's updated_at is the
          // recovery-scan key, so a caller passes EPOCH to hand the saga to the sweeper immediately
          // (null = the transition time).
          Instant rowUpdatedAt = stateUpdatedAt != null ? stateUpdatedAt : now;
          SagaStateSnapshot updated = current.withTransition(newStatus, ownerId, rowUpdatedAt);
          tx.insert(buildStateInsert(bucket, updated));
          return updated;
        },
        verifyTransitionCommitted(sagaId, sequence, appendId),
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
    String sagaId = current.getSagaId();
    int bucket = schema.bucketOf(sagaId);
    String appendId = appendIdSupplier.get();

    return runInTransaction(
        tx -> {
          Instant now = Instant.now();

          // Optimistic check: the row must still be at the snapshot's (RUNNING) CK.
          int oldStatus = current.getStatus().getStatusCode();
          if (tx.get(buildStateGet(bucket, oldStatus, current.getUpdatedAt(), sagaId)).isEmpty()) {
            throw new SagaConcurrentModificationException(sagaId);
          }

          tx.insert(buildEventInsert(sagaId, sequence, pendingEvent, appendId, now));
          tx.delete(buildStateDelete(bucket, oldStatus, current.getUpdatedAt(), sagaId));
          SagaStateSnapshot updated = current.withTransition(SagaStatus.WAITING, now);
          tx.insert(buildStateInsert(bucket, updated));
          // A bounded park records its deadline for the recovery sweeper; an unbounded park
          // (null deadline) writes no row and is never timed out.
          if (parkedDeadline != null) {
            tx.insert(buildParkedInsert(bucket, parkedDeadline, sagaId));
          }
          return updated;
        },
        verifyTransitionCommitted(sagaId, sequence, appendId),
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
   * Shared body for the three claim-less transitions out of {@code WAITING} — {@code
   * resumeParkedStep} (callback), {@code failParkedStep} (give-up), and {@code redriveParkedStep}
   * (re-drive). In one transaction, after a fail-fast pre-check that the row is still at the
   * snapshot's ({@code WAITING}) CK, it appends {@code event}, transitions the saga to {@code
   * targetStatus}, and clears the parked-deadline row (found via the saga_id secondary index since
   * its {@code parked_deadline} CK is unknown here; an unbounded park left none).
   *
   * <p>The state-row delete is the real mutual-exclusion point: two transitions deleting the same
   * {@code WAITING} CK conflict at commit, so exactly one commits; a loser retries, re-reads an
   * empty CK in the pre-check, and throws {@link SagaConcurrentModificationException}. Kept in one
   * place so the three race-safety-critical paths cannot silently diverge.
   */
  private SagaStateSnapshot transitionParkedStep(
      SagaStateSnapshot current,
      int sequence,
      StepEvent event,
      SagaStatus targetStatus,
      String op) {
    String sagaId = current.getSagaId();
    int bucket = schema.bucketOf(sagaId);
    String appendId = appendIdSupplier.get();

    return runInTransaction(
        tx -> {
          Instant now = Instant.now();

          // Fail-fast pre-check on the WAITING CK; the state-row delete below is the real
          // exclusion.
          int oldStatus = current.getStatus().getStatusCode();
          if (tx.get(buildStateGet(bucket, oldStatus, current.getUpdatedAt(), sagaId)).isEmpty()) {
            throw new SagaConcurrentModificationException(sagaId);
          }

          tx.insert(buildEventInsert(sagaId, sequence, event, appendId, now));
          tx.delete(buildStateDelete(bucket, oldStatus, current.getUpdatedAt(), sagaId));
          SagaStateSnapshot updated = current.withTransition(targetStatus, now);
          tx.insert(buildStateInsert(bucket, updated));

          for (Result parked : tx.scan(buildParkedIndexScan(sagaId))) {
            tx.delete(buildParkedDelete(bucket, parked.getTimestampTZ("parked_deadline"), sagaId));
          }
          return updated;
        },
        verifyTransitionCommitted(sagaId, sequence, appendId),
        op + " for saga " + sagaId,
        sagaId);
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
   * Verifier for a state transition (park, resume, timeout, {@code recordStatusEvent}): treats the
   * tx as committed when the event at {@code sequence} was written by us (see {@link
   * #verifyOwnEventCommitted}). On a match it returns the resulting state snapshot; when the event
   * is absent it returns empty so the caller retries; when another writer won the sequence, {@link
   * #verifyOwnEventCommitted} throws {@link SagaConcurrentModificationException} directly.
   */
  private CommitVerifier<SagaStateSnapshot> verifyTransitionCommitted(
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
  public Optional<SagaStateAndEvents> getStateWithEvents(String sagaId) {
    return runInTransaction(
        tx -> {
          Optional<SagaStateSnapshot> snapshot =
              tx.scan(buildStateIndexScan(sagaId)).stream()
                  .findFirst()
                  .map(this::toSagaStateSnapshot);
          if (snapshot.isEmpty()) {
            return Optional.<SagaStateAndEvents>empty();
          }
          List<SagaEvent> events =
              tx.scan(buildEventScan(sagaId)).stream().map(this::toSagaEvent).toList();
          return Optional.of(new SagaStateAndEvents(snapshot.get(), events));
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

  // ---------------------------------------------------------------------------
  // Queries
  // ---------------------------------------------------------------------------

  @Override
  public Optional<SagaStateSnapshot> getStateSnapshot(String sagaId) {
    return loadStateSnapshot(sagaId);
  }

  /**
   * Lists {@code saga_state} snapshots for {@link SagaStore#listStateSnapshots}, paginating by
   * <b>whole {@code updated_at} cohorts</b> — a page boundary never splits the rows that share a
   * single timestamp.
   *
   * <h4>Why cohort pagination, and not a keyset cursor on {@code saga_id}</h4>
   *
   * The natural way to page a bucket-partitioned scan is a keyset cursor over the clustering key
   * {@code (status, updated_at, saga_id)}. Two ScalarDB properties rule that out here:
   *
   * <ul>
   *   <li>A {@code Scan} can only range on the <b>last</b> clustering-key column it specifies, with
   *       the preceding columns pinned to equality. Resuming "strictly after {@code (updated_at,
   *       saga_id)}" would range on two columns at once, which ScalarDB rejects.
   *   <li>More fundamentally, ScalarDB does <b>not normalize TEXT collation</b>: the ordering of
   *       {@code saga_id} (a TEXT clustering key) is whatever the underlying database does and is
   *       not guaranteed to match byte order or Java {@code String} ordering. Any scheme that uses
   *       {@code saga_id} as a tiebreaker — a sentinel upper bound, or an in-memory {@code >}
   *       filter — is only correct on backends whose TEXT collation happens to match our
   *       assumption, a silent no-drop hazard at exact-timestamp ties.
   * </ul>
   *
   * <p>So this design never compares {@code saga_id} at all. The cursor is {@code (bucket, status,
   * updated_at)} and resume is a pure {@code updated_at > cursor} range. {@code updated_at} is a
   * timestamp, not TEXT, so its ordering is collation-independent and consistent across every
   * backend. To keep the cursor sound without a {@code saga_id} tiebreaker, a page returns a
   * timestamp's cohort <b>in full or not at all</b>: when a slice fills the page limit, {@link
   * #scanSlice} streams on just far enough to <b>complete</b> the cohort straddling the boundary,
   * then stops at the next cohort and sets the cursor to the completed cohort's timestamp.
   *
   * <h4>Trade-off: {@code pageSize} is a target, and the memory bound is cohort size</h4>
   *
   * Because a page never splits a cohort, {@code pageSize} is a <b>target</b>, not a cap: a full
   * page runs <b>over</b> it to finish the cohort straddling the limit, and a single cohort larger
   * than {@code pageSize} is returned whole as one over-sized page. So the rows materialized for
   * one page are bounded by the <b>largest cohort</b> (rows sharing one {@code updated_at} within a
   * bucket), <b>not</b> by {@code pageSize}. That is the one unbounded quantity in this path:
   * recovery caps its analogous per-status scan (see {@link
   * ScalarDbSagaStoreConfig#getRecoveryScanLimit()}), but this listing does not. A pathological
   * cohort — e.g. a mass transition stamping many sagas with the same millisecond {@code
   * updated_at}, divided only across {@code numBuckets} — therefore drives peak memory for the
   * call. Operators should provision heap and response limits for the largest expected cohort, not
   * for {@code pageSize}. Listing is best-effort under concurrent mutation.
   *
   * <h4>Future option: bound memory by splitting cohorts on {@code saga_id}</h4>
   *
   * To cap memory at {@code pageSize} and make page sizes exact, pagination could page
   * <b>within</b> a cohort using {@code saga_id} — the trailing clustering-key column — as a keyset
   * tiebreaker. The objection above is about a <b>global</b> {@code (updated_at, saga_id)} keyset
   * (two ranging columns; and sentinel or in-memory schemes that assume an order). An
   * <b>intra-cohort</b> keyset sidesteps both: with {@code updated_at} pinned to equality a scan
   * ranges on {@code saga_id} alone, and resuming with the backend's own {@code saga_id > last}
   * predicate is collation-safe — it relies only on each backend being self-consistent, never on an
   * assumed Java or byte order. It is deferred because it reintroduces a {@code saga_id} comparison
   * and a two-phase resume scan (finish the cohort, then advance {@code updated_at}) on mid-cohort
   * resumes. It can be added later <b>without breaking compatibility</b>: today's cursor is a valid
   * future cursor with no intra-cohort offset, so existing page tokens keep resuming correctly.
   *
   * <h4>Cost: sequential per-slice transactions, {@code O(numBuckets × statuses)} per page</h4>
   *
   * Each {@code (bucket, status)} slice is scanned in its own read-only transaction (see {@link
   * #scanSlice}), and the slices are swept sequentially until the page fills or every slice drains.
   * A page therefore costs up to {@code numBuckets × statuses} round-trips <b>independent of how
   * many rows it returns</b> — a sparse or empty match walks every slice before returning (at
   * defaults, 16 buckets × 6 statuses = 96; a status filter collapses the inner sweep to one, so up
   * to {@code numBuckets}). This is acceptable for a low-frequency admin listing: the cost is
   * latency, not correctness, and every transaction is read-only. The escape hatch, if it ever
   * matters, is a concurrent per-bucket fan-out, deferred because the bucket-ordered cursor would
   * then need a cross-bucket merge and a different resume scheme.
   */
  @Override
  public SagaPage<SagaStateSnapshot> listStateSnapshots(SagaQuery query) {
    int numBuckets = schema.getNumBuckets();
    int pageSize = query.getPageSize();
    @Nullable Instant updatedAfter =
        requireInTimestampTzRange(query.getUpdatedAfter(), "updatedAfter");
    @Nullable Instant updatedBefore =
        requireInTimestampTzRange(query.getUpdatedBefore(), "updatedBefore");
    // Open-ended upper bound: scan to the max instant TIMESTAMPTZ can store. That sentinel keeps
    // the end key at the same clustering-key width as the start key.
    Instant endTs = updatedBefore != null ? updatedBefore : TimestampTZColumn.MAX_VALUE;

    // Which status slices to sweep, in a stable ascending order, and where a token resumes.
    int[] statusCodes =
        query.getStatus() != null
            ? new int[] {query.getStatus().getStatusCode()}
            : ALL_STATUS_CODES;
    // A token is bound to the filters that produced it; reusing it under different filters is
    // rejected rather than silently resuming against the wrong data.
    String filterKey = PageCursor.filterKey(query);
    @Nullable PageCursor cursor =
        query.getPageToken() == null
            ? null
            : PageCursor.decode(query.getPageToken(), numBuckets, statusCodes, filterKey);

    List<SagaStateSnapshot> items = new ArrayList<>();
    int startBucket = cursor != null ? cursor.bucket() : 0;

    for (int bucket = startBucket; bucket < numBuckets; bucket++) {
      int startStatusIdx =
          (cursor != null && bucket == cursor.bucket())
              ? indexOfStatus(statusCodes, cursor.statusCode())
              : 0;
      for (int si = startStatusIdx; si < statusCodes.length; si++) {
        int statusCode = statusCodes[si];
        // Resume from the token only in the exact (bucket, status) slice it points at.
        @Nullable Instant afterTs =
            (cursor != null && bucket == cursor.bucket() && statusCode == cursor.statusCode())
                ? cursor.updatedAt()
                : null;
        SliceResult result =
            scanSlice(bucket, statusCode, afterTs, updatedAfter, endTs, pageSize - items.size());
        items.addAll(result.rows());
        if (result.resumeTs() != null) {
          // The page filled within this slice; the token resumes it after the last complete cohort.
          return new SagaPage<>(
              items, new PageCursor(bucket, statusCode, result.resumeTs()).encode(filterKey));
        }
        // Slice drained — move on to the next one.
      }
    }
    // Every slice was swept to the end.
    return new SagaPage<>(items, null);
  }

  /**
   * Scans one {@code (bucket, status)} slice in its own transaction (never one tx across a page),
   * returning whole {@code updated_at} cohorts up to about {@code limit} rows.
   *
   * <p>Streams the slice in clustering order with a {@link TransactionCrudOperable.Scanner}: once
   * it holds at least {@code limit} rows it keeps pulling only far enough to finish the cohort in
   * progress, then stops at the first row of the next cohort (which belongs to the next page — the
   * timestamp-exclusive cursor re-reads it). This is a single pass — no dropped-then-re-read
   * trailing cohort, no separate full-cohort scan, and no {@code saga_id} comparison — so it also
   * completes an over-sized single-timestamp cohort for free. The scanner lives entirely within
   * this one (read-only) transaction and is closed before commit.
   *
   * @return the rows plus, when the slice was <b>not</b> drained, the last complete timestamp to
   *     resume after; {@link SliceResult#resumeTs()} is {@code null} when the slice is fully
   *     drained
   */
  private SliceResult scanSlice(
      int bucket,
      int statusCode,
      @Nullable Instant afterTs,
      @Nullable Instant updatedAfter,
      Instant endTs,
      int limit) {
    return runInTransaction(
        tx -> {
          Instant startTs;
          boolean startInclusive;
          if (afterTs != null) {
            startTs = afterTs; // resume strictly after the last fully-returned timestamp
            startInclusive = false;
          } else {
            startTs = updatedAfter != null ? updatedAfter : Instant.EPOCH;
            startInclusive = true;
          }

          List<SagaStateSnapshot> rows = new ArrayList<>();
          @Nullable Instant lastTs = null; // timestamp of the last row accepted
          try (TransactionCrudOperable.Scanner scanner =
              tx.getScanner(
                  buildStateRangeScan(bucket, statusCode, startTs, startInclusive, endTs))) {
            for (Optional<Result> next = scanner.one(); next.isPresent(); next = scanner.one()) {
              Result r = next.get();
              Instant ts = r.getTimestampTZ("updated_at");
              if (rows.size() >= limit && !ts.equals(lastTs)) {
                break; // limit met and a new cohort begins — leave it for the next page
              }
              rows.add(toSagaStateSnapshot(r));
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

  private static int indexOfStatus(int[] statusCodes, int statusCode) {
    for (int i = 0; i < statusCodes.length; i++) {
      if (statusCodes[i] == statusCode) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Rejects an {@code updated_at} bound outside the range this store's TIMESTAMPTZ column can hold,
   * with a clear message, rather than letting it surface as a lower-level exception when the scan
   * key is built. Sub-millisecond precision needs no handling here: the scan routes the bound
   * through {@link TimestampTZColumn#of}, which truncates it to match the millisecond-granular
   * stored values.
   *
   * @return {@code bound} unchanged (including {@code null}, which means no bound)
   */
  private static @Nullable Instant requireInTimestampTzRange(
      @Nullable Instant bound, String field) {
    if (bound != null
        && (bound.isBefore(TimestampTZColumn.MIN_VALUE)
            || bound.isAfter(TimestampTZColumn.MAX_VALUE))) {
      throw new SagaIllegalArgumentException(
          field
              + " must be in ["
              + TimestampTZColumn.MIN_VALUE
              + ", "
              + TimestampTZColumn.MAX_VALUE
              + "]: "
              + bound);
    }
    return bound;
  }

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  @Override
  public Recoverables findRecoverable(Instant threshold, @Nullable ScanCursor cursor) {
    int startBucket = 0;
    if (cursor instanceof BucketCursor(int nextBucket)) {
      startBucket = nextBucket;
    }

    if (startBucket >= schema.getNumBuckets()) {
      return new Recoverables(List.of(), null);
    }

    int bucket = startBucket;

    List<SagaStateSnapshot> result =
        runInTransaction(
            tx -> {
              List<SagaStateSnapshot> snapshots = new ArrayList<>();
              // Cap each status scan to avoid unbounded memory usage in large buckets.
              // Any sagas beyond the limit are picked up on the next recovery cycle.
              int scanLimit = config.getRecoveryScanLimit();
              for (int status : RECOVERABLE_STATUS_CODES) {
                List<Result> rows =
                    tx.scan(
                        Scan.newBuilder(
                                buildStateRangeScan(bucket, status, Instant.EPOCH, true, threshold))
                            .limit(scanLimit)
                            .build());
                for (Result r : rows) {
                  snapshots.add(toSagaStateSnapshot(r));
                }
              }
              return snapshots;
            },
            null,
            "find recoverable sagas");

    int nextBucket = startBucket + 1;
    @Nullable ScanCursor nextCursor =
        nextBucket < schema.getNumBuckets() ? new BucketCursor(nextBucket) : null;
    return new Recoverables(result, nextCursor);
  }

  @Override
  public Optional<SagaStateSnapshot> claimForRecovery(SagaStateSnapshot saga, String newOwnerId) {
    String sagaId = saga.getSagaId();
    int bucket = schema.bucketOf(sagaId);
    int status = saga.getStatus().getStatusCode();

    try {
      SagaStateSnapshot result =
          runInTransaction(
              tx -> {
                Instant now = Instant.now();

                // Verify the row still exists at the snapshot's CK.
                Optional<Result> current =
                    tx.get(buildStateGet(bucket, status, saga.getUpdatedAt(), sagaId));

                if (current.isEmpty()) {
                  throw new SagaConcurrentModificationException(sagaId);
                }

                tx.delete(buildStateDelete(bucket, status, saga.getUpdatedAt(), sagaId));
                SagaStateSnapshot claimed =
                    new SagaStateSnapshot(
                        sagaId,
                        saga.getSagaName(),
                        saga.getStatus(),
                        newOwnerId,
                        saga.getDefinitionVersion(),
                        saga.getCreatedAt(),
                        now);
                tx.insert(buildStateInsert(bucket, claimed));
                return claimed;
              },
              () -> {
                Optional<SagaStateSnapshot> state = loadStateSnapshot(sagaId);
                if (state.isPresent() && newOwnerId.equals(state.get().getOwnerId())) {
                  return state;
                }
                return Optional.empty();
              },
              "claim saga " + sagaId + " for recovery");
      return Optional.of(result);
    } catch (SagaConcurrentModificationException e) {
      return Optional.empty();
    }
  }

  @Override
  public void markForRecovery(String sagaId) {
    try {
      runInTransaction(
          tx -> {
            Optional<Result> result = tx.scan(buildStateIndexScan(sagaId)).stream().findFirst();

            if (result.isEmpty()) {
              return Boolean.TRUE; // no-op
            }

            Result r = result.get();
            int bucket = r.getInt("bucket");
            SagaStateSnapshot current = toSagaStateSnapshot(r);

            tx.delete(
                buildStateDelete(
                    bucket, current.getStatus().getStatusCode(), current.getUpdatedAt(), sagaId));
            SagaStateSnapshot marked =
                new SagaStateSnapshot(
                    sagaId,
                    current.getSagaName(),
                    current.getStatus(),
                    current.getOwnerId(),
                    current.getDefinitionVersion(),
                    current.getCreatedAt(),
                    Instant.EPOCH);
            tx.insert(buildStateInsert(bucket, marked));
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
    int startBucket = 0;
    if (cursor instanceof BucketCursor(int nextBucket)) {
      startBucket = nextBucket;
    }
    if (startBucket >= schema.getNumBuckets()) {
      return new OverdueParked(List.of(), null);
    }

    int bucket = startBucket;
    List<String> sagaIds =
        runInTransaction(
            tx -> {
              // Cap the scan to bound memory; anything beyond is picked up next recovery cycle.
              List<Result> rows =
                  tx.scan(
                      Scan.newBuilder(buildParkedRangeScan(bucket, threshold))
                          .limit(config.getRecoveryScanLimit())
                          .build());
              return rows.stream().map(r -> r.getText("saga_id")).toList();
            },
            null,
            "find overdue parked sagas");

    int nextBucket = startBucket + 1;
    @Nullable ScanCursor nextCursor =
        nextBucket < schema.getNumBuckets() ? new BucketCursor(nextBucket) : null;
    return new OverdueParked(sagaIds, nextCursor);
  }

  // ---------------------------------------------------------------------------
  // Data retention
  // ---------------------------------------------------------------------------

  @Override
  public List<SagaStateSnapshot> findByStatusOlderThan(
      SagaStatus status, Instant threshold, int maxResults) {
    List<SagaStateSnapshot> results = new ArrayList<>();
    for (int bucket = 0; bucket < schema.getNumBuckets() && results.size() < maxResults; bucket++) {
      results.addAll(findByStatusInBucket(bucket, status, threshold, maxResults - results.size()));
    }
    return results;
  }

  /**
   * Finds sagas in a specific (bucket, status) partition with {@code updated_at} older than the
   * threshold.
   */
  private List<SagaStateSnapshot> findByStatusInBucket(
      int bucket, SagaStatus status, Instant threshold, int maxResults) {
    return runInTransaction(
        tx -> {
          List<Result> rows =
              tx.scan(
                  Scan.newBuilder(
                          buildStateRangeScan(
                              bucket, status.getStatusCode(), Instant.EPOCH, true, threshold))
                      .limit(maxResults)
                      .build());
          return rows.stream().map(this::toSagaStateSnapshot).toList();
        },
        null,
        "find sagas by status");
  }

  @Override
  public void deleteSaga(String sagaId) {
    runInTransaction(
        tx -> {
          Optional<Result> stateResult = tx.scan(buildStateIndexScan(sagaId)).stream().findFirst();

          if (stateResult.isPresent()) {
            Result r = stateResult.get();
            SagaStatus status = SagaStatus.fromStatusCode(r.getInt("status"));
            if (!status.isTerminal()) {
              throw new IllegalStateException(
                  "Cannot delete saga in non-terminal status: " + status);
            }
            tx.delete(
                buildStateDelete(
                    r.getInt("bucket"),
                    r.getInt("status"),
                    r.getTimestampTZ("updated_at"),
                    sagaId));
          }

          List<Result> eventResults =
              tx.scan(
                  Scan.newBuilder(buildEventScan(sagaId))
                      .projections("saga_id", "sequence")
                      .build());

          for (Result r : eventResults) {
            tx.delete(buildEventDelete(sagaId, r.getInt("sequence")));
          }

          return Boolean.TRUE;
        },
        () -> {
          // Verify deletion: state row should be absent
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
   * #verifyTransitionCommitted}, or a lambda over {@link #verifyOwnEventCommitted} as {@code
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
   * transition ops re-read the state row each time, so reaching here means we never once observed
   * another writer take it; {@code recordStepEvent} is a bare append that reads nothing and so
   * observes nothing at all. Either way this is the weakest evidence of a collision in this class,
   * not the strongest.
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
      throw SagaPersistenceException.storeUnavailable(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  // -- saga_state builders --

  private Get buildStateGet(int bucket, int status, Instant updatedAt, String sagaId) {
    return Get.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(stateClusteringKey(status, updatedAt, sagaId))
        .build();
  }

  private Delete buildStateDelete(int bucket, int status, Instant updatedAt, String sagaId) {
    return Delete.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(stateClusteringKey(status, updatedAt, sagaId))
        .build();
  }

  private Insert buildStateInsert(int bucket, SagaStateSnapshot snapshot) {
    return Insert.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(
            stateClusteringKey(
                snapshot.getStatus().getStatusCode(),
                snapshot.getUpdatedAt(),
                snapshot.getSagaId()))
        .textValue("saga_name", snapshot.getSagaName())
        .textValue("owner_id", snapshot.getOwnerId())
        .textValue("definition_version", snapshot.getDefinitionVersion())
        .timestampTZValue("created_at", snapshot.getCreatedAt())
        .build();
  }

  /**
   * Looks up a single state row by {@code saga_id} via the secondary index.
   *
   * <p>Uses a {@code Scan} with {@code limit(1)} rather than a {@code Get} on the index
   * deliberately. ConsensusCommit's Get-with-index currently throws {@link
   * IllegalArgumentException} when two physical rows transiently share the same {@code saga_id}
   * during a status transition. Scan tolerates this and keeps the store always runnable. Once that
   * limitation is resolved, this can be reverted to a single {@code Get} on the index.
   *
   * <p>Callers can therefore treat the result as at most one visible row: {@code saga_id} is a
   * unique UUID, and a status transition (delete old row + insert new row) is committed atomically
   * by ConsensusCommit, so under snapshot isolation a reader sees either the pre-state or the
   * post-state — never both. {@code stream().findFirst()} on the result is thus always the current
   * state; there is no set of rows to order by {@code updated_at}.
   */
  private Scan buildStateIndexScan(String sagaId) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .indexKey(Key.ofText("saga_id", sagaId))
        .limit(1)
        .build();
  }

  /**
   * Builds an {@code updated_at}-range scan over one {@code (bucket, status)} slice of {@code
   * saga_state}, from {@code [startTs .. endInclusive]} on the second clustering-key column ({@code
   * status} fixed). ScalarDB only allows ranging on the last specified clustering key, which is why
   * the Admin listing paginates by whole {@code updated_at} cohorts rather than a {@code saga_id}
   * keyset (see {@link #listStateSnapshots}). Recovery and retention call this as {@code (EPOCH,
   * true, threshold)}, reproducing their original {@code [EPOCH, threshold]} scan exactly.
   */
  private Scan buildStateRangeScan(
      int bucket, int status, Instant startTs, boolean startInclusive, Instant endInclusive) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .start(
            Key.newBuilder().addInt("status", status).addTimestampTZ("updated_at", startTs).build(),
            startInclusive)
        .end(
            Key.newBuilder()
                .addInt("status", status)
                .addTimestampTZ("updated_at", endInclusive)
                .build(),
            true)
        .build();
  }

  /** Scans one {@code saga_parked} bucket for deadlines in {@code [EPOCH, threshold]}. */
  private Scan buildParkedRangeScan(int bucket, Instant threshold) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.PARKED_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .start(Key.newBuilder().addTimestampTZ("parked_deadline", Instant.EPOCH).build(), true)
        .end(Key.newBuilder().addTimestampTZ("parked_deadline", threshold).build(), true)
        .build();
  }

  private Insert buildParkedInsert(int bucket, Instant parkedDeadline, String sagaId) {
    return Insert.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.PARKED_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(parkedClusteringKey(parkedDeadline, sagaId))
        .build();
  }

  private Delete buildParkedDelete(int bucket, Instant parkedDeadline, String sagaId) {
    return Delete.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.PARKED_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .clusteringKey(parkedClusteringKey(parkedDeadline, sagaId))
        .build();
  }

  /** Looks up a single {@code saga_parked} row by {@code saga_id} via the secondary index. */
  private Scan buildParkedIndexScan(String sagaId) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.PARKED_TABLE)
        .indexKey(Key.ofText("saga_id", sagaId))
        .limit(1)
        .build();
  }

  private static Key parkedClusteringKey(Instant parkedDeadline, String sagaId) {
    return Key.newBuilder()
        .addTimestampTZ("parked_deadline", parkedDeadline)
        .addText("saga_id", sagaId)
        .build();
  }

  // -- saga_definitions builders --

  private Insert buildDefinitionInsert(String name, String version, String json) {
    return Insert.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.DEFINITIONS_TABLE)
        .partitionKey(Key.ofText("saga_name", name))
        .clusteringKey(Key.ofText("definition_version", version))
        .textValue("definition_json", json)
        .timestampTZValue("registered_at", Instant.now())
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

  private Insert buildEventInsert(
      String sagaId, int sequence, SagaEvent event, String appendId, Instant now) {
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
    String eventTypeStr = r.getText("event_type");
    int stepIndex = r.getInt("step_index");
    @Nullable String stepName = r.isNull("step_name") ? null : r.getText("step_name");
    @Nullable String payload = r.isNull("payload") ? null : r.getText("payload");
    Instant createdAt = r.getTimestampTZ("created_at");

    EventType eventType;
    try {
      eventType = EventType.valueOf(eventTypeStr);
    } catch (IllegalArgumentException e) {
      throw SagaPersistenceException.deserializationFailed(e);
    }

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
            default ->
                throw SagaPersistenceException.deserializationFailed(
                    new IllegalStateException("Unknown saga event type: " + eventType));
          };
      return event.withTimestamp(createdAt);
    }
  }

  private SagaStateSnapshot toSagaStateSnapshot(Result r) {
    return new SagaStateSnapshot(
        r.getText("saga_id"),
        r.getText("saga_name"),
        SagaStatus.fromStatusCode(r.getInt("status")),
        r.getText("owner_id"),
        r.getText("definition_version"),
        r.getTimestampTZ("created_at"),
        r.getTimestampTZ("updated_at"));
  }

  private Optional<SagaStateSnapshot> loadStateSnapshot(String sagaId) {
    return runInTransaction(
        tx ->
            tx.scan(buildStateIndexScan(sagaId)).stream()
                .findFirst()
                .map(this::toSagaStateSnapshot),
        null,
        "load saga state " + sagaId);
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
        throw new IllegalArgumentException(
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

  private static Key stateClusteringKey(int status, Instant updatedAt, String sagaId) {
    return Key.newBuilder()
        .addInt("status", status)
        .addTimestampTZ("updated_at", updatedAt)
        .addText("saga_id", sagaId)
        .build();
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
   * Result of scanning one {@code (bucket, status)} slice: the rows to emit, and — when the slice
   * was not drained — the last <b>complete</b> {@code updated_at} to resume after. A {@code null}
   * {@link #resumeTs()} means the slice was fully drained within {@code [.., updatedBefore]}.
   */
  private record SliceResult(List<SagaStateSnapshot> rows, @Nullable Instant resumeTs) {}

  /**
   * The wire-portable position of an Admin listing: the {@code (bucket, status, updated_at)} of the
   * last fully-returned timestamp cohort. There is deliberately no {@code saga_id} — pagination
   * never compares that TEXT column, since ScalarDB does not normalize its collation (see {@link
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
   * IllegalArgumentException} (mapped to 400) rather than silently scanning the wrong data. The
   * token carries only a scan position and filter key within already-authorized data, so it is not
   * signed.
   */
  private record PageCursor(int bucket, int statusCode, Instant updatedAt) {

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
      String status =
          query.getStatus() == null
              ? ANY_STATUS
              : Integer.toString(query.getStatus().getStatusCode());
      String after =
          query.getUpdatedAfter() == null ? NO_BOUND : query.getUpdatedAfter().toString();
      String before =
          query.getUpdatedBefore() == null ? NO_BOUND : query.getUpdatedBefore().toString();
      return String.join(DELIMITER, status, after, before);
    }

    String encode(String filterKey) {
      String payload =
          String.join(
              DELIMITER,
              PAGE_TOKEN_VERSION,
              filterKey, // three sub-parts: status, after, before
              Integer.toString(bucket),
              Integer.toString(statusCode),
              updatedAt.toString());
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }

    static PageCursor decode(
        String token, int numBuckets, int[] allowedStatusCodes, String expectedFilterKey) {
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
      // version | status | after | before | bucket | statusCode | updatedAt
      String[] parts = payload.split(Pattern.quote(DELIMITER), 7);
      if (parts.length != 7 || !PAGE_TOKEN_VERSION.equals(parts[0])) {
        throw new SagaIllegalArgumentException("Unrecognized page token");
      }
      String filterKey = String.join(DELIMITER, parts[1], parts[2], parts[3]);
      if (!expectedFilterKey.equals(filterKey)) {
        throw new SagaIllegalArgumentException("Page token does not match the query");
      }
      int bucket;
      int statusCode;
      Instant updatedAt;
      try {
        bucket = Integer.parseInt(parts[4]);
        statusCode = Integer.parseInt(parts[5]);
        updatedAt = Instant.parse(parts[6]);
      } catch (RuntimeException e) {
        throw new SagaIllegalArgumentException("Malformed page token", e);
      }
      if (bucket < 0 || bucket >= numBuckets) {
        throw new SagaIllegalArgumentException("Page token bucket out of range");
      }
      // Defense in depth: even with a matching filter key the token is unsigned, so guard the
      // resume math against a tampered statusCode outside the swept set (would index out of
      // bounds).
      if (indexOfStatus(allowedStatusCodes, statusCode) < 0) {
        throw new SagaIllegalArgumentException("Page token does not match the query");
      }
      return new PageCursor(bucket, statusCode, updatedAt);
    }
  }
}
