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
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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
 * unified retry logic for {@link TransactionException} (including {@link CrudConflictException} and
 * {@link UnknownTransactionStatusException}). {@link CommitConflictException} is retried by default
 * but can be disabled per operation (e.g., {@code createSaga} treats it as a permanent conflict).
 */
public class ScalarDbSagaStore implements SagaStore {

  private static final Logger logger = LoggerFactory.getLogger(ScalarDbSagaStore.class);

  private static final Pattern SAGA_ID_PATTERN = Pattern.compile("[a-zA-Z0-9._-]{1,128}");
  private static final int[] RECOVERABLE_STATUS_CODES =
      java.util.Arrays.stream(SagaStatus.values())
          .filter(SagaStatus::isRecoverable)
          .mapToInt(SagaStatus::getStatusCode)
          .toArray();

  private final DistributedTransactionManager txManager;
  private final ObjectMapper objectMapper;
  private final SagaSchema schema;
  private final ScalarDbSagaStoreConfig config;
  private final SagaDefinitionSerializer definitionSerializer;

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
    this.txManager = Objects.requireNonNull(txManager, "txManager must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.schema = Objects.requireNonNull(schema, "schema must not be null");
    this.config = Objects.requireNonNull(config, "config must not be null");
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
      throw new SagaPersistenceException("Failed to close transaction manager", e);
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

    try {
      return runInTransaction(
          tx -> {
            Instant now = Instant.now();
            tx.insert(buildEventInsert(id, 0, startedEvent, now));
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
            throw new SagaDefinitionException(
                "Definition '"
                    + name
                    + "' version '"
                    + version
                    + "' is already registered with different content. Bump the version instead.");
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
    runInTransaction(
        tx -> {
          tx.insert(buildEventInsert(sagaId, sequence, event, Instant.now()));
          return Boolean.TRUE;
        },
        () -> {
          // Verify the event was committed by re-reading it
          return runInTransaction(
              tx -> {
                Optional<Result> result = tx.get(buildEventGet(sagaId, sequence));
                return result.isPresent() ? Optional.of(Boolean.TRUE) : Optional.empty();
              },
              null,
              "verify event " + sagaId + " seq " + sequence);
        },
        "append event for saga " + sagaId);
  }

  @Override
  public SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current, int sequence, StatusEvent event) {
    validatePayloadSize(event.getPayload());
    String sagaId = current.getSagaId();
    SagaStatus newStatus = event.getTargetStatus();
    int bucket = schema.bucketOf(sagaId);

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

          tx.insert(buildEventInsert(sagaId, sequence, event, now));
          tx.delete(buildStateDelete(bucket, oldStatus, current.getUpdatedAt(), sagaId));
          SagaStateSnapshot updated = current.withTransition(newStatus, now);
          tx.insert(buildStateInsert(bucket, updated));
          return updated;
        },
        () ->
            runInTransaction(
                tx -> {
                  if (tx.get(buildEventGet(sagaId, sequence)).isPresent()) {
                    return loadStateSnapshot(sagaId);
                  }
                  return Optional.empty();
                },
                null,
                "verify transition " + sagaId + " seq " + sequence),
        "record transition for saga " + sagaId);
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

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  @Override
  public Recoverables findRecoverable(long recoveryTimeoutMillis, @Nullable ScanCursor cursor) {
    int startBucket = 0;
    if (cursor instanceof BucketCursor(int nextBucket)) {
      startBucket = nextBucket;
    }

    if (startBucket >= schema.getNumBuckets()) {
      return new Recoverables(List.of(), null);
    }

    Instant threshold = Instant.now().minusMillis(recoveryTimeoutMillis);
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
                        Scan.newBuilder(buildStateRangeScan(bucket, status, threshold))
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
                  Scan.newBuilder(buildStateRangeScan(bucket, status.getStatusCode(), threshold))
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
    return runInTransaction(action, commitVerifier, operationName, true);
  }

  /**
   * Runs a transaction with an option to skip retry on {@link CommitConflictException}.
   *
   * @param action the transaction action to run
   * @param commitVerifier verifier to check commit status on UTSE, or {@code null} to retry the
   *     whole transaction
   * @param operationName description for error messages
   * @param retryOnCommitConflict if {@code false}, {@link CommitConflictException} is not retried
   * @return the result of the action
   */
  <T> T runInTransaction(
      TransactionAction<T> action,
      @Nullable CommitVerifier<T> commitVerifier,
      String operationName,
      boolean retryOnCommitConflict) {
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
            // Business-logic or programming errors propagate immediately.
            // Only SagaPersistenceException (infrastructure failure from inner
            // transactions) and checked exceptions are retried.
            if (ve instanceof RuntimeException re && !(ve instanceof SagaPersistenceException)) {
              throw re;
            }
            e.addSuppressed(ve);
            if (v < maxAttempts - 1) {
              sleepForRetry(v);
              continue;
            }
            throw new SagaPersistenceException(
                "Failed to " + operationName + ": commit status unknown and verification failed",
                e);
          }
        }
        lastException = e;
      } catch (CommitConflictException e) {
        abortQuietly(tx);
        if (!retryOnCommitConflict) {
          logger.debug(
              "Commit conflict for {} (txId={})",
              operationName,
              e.getTransactionId().orElse("unknown"));
          throw new SagaPersistenceException("Failed to " + operationName, e);
        }
        logger.debug(
            "Commit conflict for {} (txId={}), retrying",
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
        throw new SagaPersistenceException("Failed to " + operationName, e);
      }
    }
    logger.warn("All {} attempts exhausted for {}", maxAttempts, operationName, lastException);
    throw new SagaPersistenceException(
        "Failed to " + operationName + " after " + maxAttempts + " attempts",
        Objects.requireNonNull(lastException));
  }

  private void sleepForRetry(int retryIndex) {
    try {
      long delay = Math.min(100L * (1L << retryIndex), 5000L);
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SagaPersistenceException("Interrupted during retry backoff", e);
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

  private Scan buildStateRangeScan(int bucket, int status, Instant threshold) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .start(
            Key.newBuilder()
                .addInt("status", status)
                .addTimestampTZ("updated_at", Instant.EPOCH)
                .build(),
            true)
        .end(
            Key.newBuilder()
                .addInt("status", status)
                .addTimestampTZ("updated_at", threshold)
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

  private Insert buildEventInsert(String sagaId, int sequence, SagaEvent event, Instant now) {
    var builder =
        Insert.newBuilder()
            .namespace(SagaSchema.NAMESPACE)
            .table(SagaSchema.EVENTS_TABLE)
            .partitionKey(Key.ofText("saga_id", sagaId))
            .clusteringKey(Key.ofInt("sequence", sequence))
            .textValue("event_type", event.getEventType().name())
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
      throw new SagaPersistenceException("Unknown event type: " + eventTypeStr, e);
    }

    if (stepIndex >= 0) {
      String name = Objects.requireNonNull(stepName, "stepName must not be null for step events");
      StepEvent event =
          switch (eventType) {
            case STEP_COMPLETED -> StepEvent.completed(stepIndex, name, payload);
            case STEP_FAILED -> StepEvent.failed(stepIndex, name, payload);
            case STEP_COMPENSATED -> StepEvent.compensated(stepIndex, name);
            case STEP_COMPENSATION_FAILED -> StepEvent.compensationFailed(stepIndex, name, payload);
            default ->
                throw new SagaPersistenceException(
                    "Unknown step event type: " + eventType,
                    new IllegalStateException(eventTypeStr));
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
            default ->
                throw new SagaPersistenceException(
                    "Unknown saga event type: " + eventType,
                    new IllegalStateException(eventTypeStr));
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
      throw new IllegalArgumentException(
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
      throw new SagaPersistenceException("Failed to serialize JSON", e);
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
}
