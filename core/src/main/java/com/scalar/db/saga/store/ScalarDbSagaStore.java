package com.scalar.db.saga.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
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
  private static final int[] RECOVERABLE_STATUS_CODES = {
    SagaStatus.RUNNING.getStatusCode(),
    SagaStatus.CONFIRMING.getStatusCode(),
    SagaStatus.COMPENSATING.getStatusCode()
  };

  private final DistributedTransactionManager txManager;
  private final ObjectMapper objectMapper;
  private final SagaSchema schema;
  private final ScalarDbSagaStoreConfig config;
  private final SagaDefinitionSerializer definitionSerializer;
  private final Cache<String, SagaStateSnapshot> cache;

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
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(config.getCacheMaxSize())
            .expireAfterWrite(config.getCacheExpireAfterWrite())
            .build();
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
    String payload = toJson(Map.of("sagaName", sagaName, "input", input));
    validatePayloadSize(payload);
    StatusEvent startedEvent = StatusEvent.started(payload);
    String id = sagaId; // effectively final for lambda
    int bucket = schema.bucketOf(id);

    try {
      SagaStateSnapshot result =
          runInTransaction(
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
      cache.put(id, result);
      return result;
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
            String existingJson = existing.get().getText("definition_json");
            if (json.equals(existingJson)) {
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
          return found.isPresent() ? Optional.of(Boolean.TRUE) : Optional.empty();
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

    try {
      SagaStateSnapshot result =
          runInTransaction(
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
              () -> {
                return runInTransaction(
                    tx -> {
                      if (tx.get(buildEventGet(sagaId, sequence)).isPresent()) {
                        return loadStateSnapshot(sagaId);
                      }
                      return Optional.empty();
                    },
                    null,
                    "verify transition " + sagaId + " seq " + sequence);
              },
              "record transition for saga " + sagaId);
      cache.put(sagaId, result);
      return result;
    } catch (SagaConcurrentModificationException | SagaPersistenceException e) {
      cache.invalidate(sagaId);
      throw e;
    }
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
    SagaStateSnapshot cached = cache.getIfPresent(sagaId);
    if (cached != null) {
      return Optional.of(cached);
    }
    return loadStateSnapshot(sagaId)
        .map(
            loaded -> {
              cache.put(sagaId, loaded);
              return loaded;
            });
  }

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  @Override
  public Recoverables findRecoverable(
      long recoveryTimeoutMillis, @Nullable RecoverablesCursor cursor) {
    int startBucket = 0;
    if (cursor instanceof BucketCursor bc) {
      startBucket = bc.nextBucket();
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
    @Nullable RecoverablesCursor nextCursor =
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
      cache.put(sagaId, result);
      return Optional.of(result);
    } catch (SagaConcurrentModificationException e) {
      cache.invalidate(sagaId);
      return Optional.empty();
    }
  }

  @Override
  public void markForRecovery(String sagaId) {
    try {
      runInTransaction(
          tx -> {
            Optional<Result> result = tx.get(buildStateIndexGet(sagaId));

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
      cache.invalidate(sagaId);
    } catch (Exception e) {
      // Best effort — conflict with executing thread is expected and harmless
      logger.debug("markForRecovery failed for saga {} (best-effort)", sagaId, e);
    }
  }

  // ---------------------------------------------------------------------------
  // Data retention
  // ---------------------------------------------------------------------------

  /**
   * Finds sagas in the given (bucket, status) partition with {@code updated_at} older than the
   * threshold. Used by {@code SagaRetentionManager} to find purgeable COMPLETED/COMPENSATED sagas.
   *
   * <p>Package-private because this overlaps with admin query methods (e.g., {@code
   * listStateSnapshots}) planned for Phase 5. Once those exist, the retention manager should switch
   * to the admin API and this method can be removed.
   */
  List<SagaStateSnapshot> findByStatusOlderThan(
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
          Optional<Result> stateResult = tx.get(buildStateIndexGet(sagaId));

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
    cache.invalidate(sagaId);
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

  private Get buildStateIndexGet(String sagaId) {
    return Get.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .indexKey(Key.ofText("saga_id", sagaId))
        .build();
  }

  private Scan buildStateRangeScan(int bucket, int status, Instant threshold) {
    return Scan.newBuilder()
        .namespace(SagaSchema.NAMESPACE)
        .table(SagaSchema.STATE_TABLE)
        .partitionKey(Key.ofInt("bucket", bucket))
        .start(Key.ofInt("status", status), true)
        .end(
            Key.newBuilder()
                .addInt("status", status)
                .addTimestampTZ("updated_at", threshold)
                .build(),
            true)
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
            case SAGA_CONFIRMING -> StatusEvent.confirming();
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
        tx -> tx.get(buildStateIndexGet(sagaId)).map(this::toSagaStateSnapshot),
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
  private record BucketCursor(int nextBucket) implements RecoverablesCursor {}
}
