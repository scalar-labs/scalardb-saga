package com.scalar.db.saga.store;

import com.scalar.db.api.Admin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.saga.api.SagaStatus;
import java.util.Map;

/**
 * Schema definitions for the saga persistence tables.
 *
 * <p>Defines three tables:
 *
 * <ul>
 *   <li>{@code saga_events}: the append-only event log, partitioned by saga ID. It is the only
 *       record of a saga: the newest event that carries a status is the saga's current state.
 *   <li>{@code saga_sweep}: one row per saga, bucket-partitioned and sorted for the background
 *       sweeps (recovery, parked-step timeouts, retention) and the admin listing.
 *   <li>{@code saga_definitions}: the saga definition registry.
 * </ul>
 *
 * <p>There is no secondary index: every read of one saga reads that saga's own event partition.
 */
public final class SagaSchema {

  /** The ScalarDB namespace holding every saga table. */
  public static final String NAMESPACE = "saga";

  /** The append-only event log, one partition per saga. */
  public static final String EVENTS_TABLE = "saga_events";

  /** The sweep table: one row per saga, bucket-partitioned and sorted for the sweeps. */
  public static final String SWEEP_TABLE = "saga_sweep";

  /** The saga definition registry, keyed by name and version. */
  public static final String DEFINITIONS_TABLE = "saga_definitions";

  /** Default number of buckets for partitioning {@code saga_sweep}. */
  public static final int DEFAULT_NUM_BUCKETS = 16;

  private final int numBuckets;

  /** Creates a schema with {@value #DEFAULT_NUM_BUCKETS} buckets. */
  public SagaSchema() {
    this(DEFAULT_NUM_BUCKETS);
  }

  /**
   * Creates a schema with the specified number of buckets.
   *
   * <p><b>Important:</b> {@code numBuckets} must remain constant once data has been written to the
   * {@code saga_sweep} table. Changing it maps existing sagas to different bucket partitions, so
   * every write that moves a saga's sweep row would look for it in the wrong partition.
   *
   * @param numBuckets the number of bucket partitions (must be &gt; 0)
   */
  public SagaSchema(int numBuckets) {
    if (numBuckets <= 0) {
      throw new IllegalArgumentException("numBuckets must be > 0, got " + numBuckets);
    }
    this.numBuckets = numBuckets;
  }

  /**
   * The number of bucket partitions of {@code saga_sweep}.
   *
   * @return the bucket count, always positive
   */
  public int getNumBuckets() {
    return numBuckets;
  }

  /**
   * Computes the bucket for a saga ID.
   *
   * @param sagaId the saga instance ID
   * @return a bucket index in {@code [0, numBuckets)}
   */
  public int bucketOf(String sagaId) {
    return (sagaId.hashCode() & 0x7FFF_FFFF) % numBuckets;
  }

  /**
   * Append-only event log, and the record of each saga's state.
   *
   * <p>Partition key: {@code saga_id}. Clustering key: {@code sequence} (ascending), dense per
   * saga, so two writers appending to one saga collide on the same key.
   *
   * <p>A row written by a store method that changes the saga's state (create, a status transition,
   * park, unpark, a recovery claim or a shutdown hand-off) also carries the state after it: {@code
   * state_slice}, {@code sweep_at}, and the snapshot fields {@code saga_name}, {@code
   * definition_version} and {@code saga_created_at}. The newest such row is the saga's current
   * state. A plain step event leaves these columns out of its write entirely, so it stays a single
   * insert that reads nothing, and Cassandra writes no tombstones for them.
   *
   * @return the metadata of {@code saga_events}
   */
  public static TableMetadata sagaEventsTable() {
    return TableMetadata.newBuilder()
        .addColumn("saga_id", DataType.TEXT) // PK
        .addColumn("sequence", DataType.INT) // CK: dense, increasing per saga
        .addColumn("event_type", DataType.TEXT) // SAGA_STARTED, STEP_COMPLETED, etc.
        .addColumn("append_id", DataType.TEXT) // per-append UUID; identifies the writer
        .addColumn("step_index", DataType.INT) // step index (-1 for saga-level events)
        .addColumn("step_name", DataType.TEXT) // step name (null for saga-level events)
        .addColumn("payload", DataType.TEXT) // JSON: step result, error, input, etc.
        .addColumn("created_at", DataType.TIMESTAMPTZ)
        .addColumn("state_slice", DataType.INT) // the saga's sweep slice after this event
        .addColumn("sweep_at", DataType.BIGINT) // its sweep position after this event, epoch ms
        .addColumn("saga_name", DataType.TEXT)
        .addColumn("definition_version", DataType.TEXT)
        .addColumn("saga_created_at", DataType.TIMESTAMPTZ)
        .addPartitionKey("saga_id")
        .addClusteringKey("sequence", Scan.Ordering.Order.ASC)
        .build();
  }

  /**
   * The sweep table: one row per saga, bucket-partitioned and sorted for the background sweeps and
   * the admin listing.
   *
   * <p>Partition key: {@code bucket}. Clustering key: {@code (slice, sweep_at, saga_id)}. A slice
   * is a {@link SagaStatus} code, or {@link #WAITING_WITH_TIMEOUT_SLICE} for a parked step that has
   * a timeout. {@code sweep_at} is the time the sweep orders by: the timeout deadline in that
   * slice, and the last status change everywhere else ({@code EPOCH} after a hand-off to recovery).
   * Each sweep reads only what is due with one range scan per bucket: stale {@code RUNNING} and
   * {@code COMPENSATING} rows, overdue deadlines, or terminal rows past retention. A wait without a
   * timeout sits in the {@code WAITING} slice by its last status change, so no timeout sweep reads
   * it and the admin listing's time filter stays exact there.
   *
   * <p>Because {@code slice} and {@code sweep_at} are part of the clustering key, moving a saga
   * deletes its row and inserts a new one. Every move is recorded in an event in the same
   * transaction (see {@link #sagaEventsTable()}), so a writer derives the row's key from the saga's
   * newest state without reading this table first.
   *
   * <p>{@code sweep_at} holds epoch milliseconds in a {@code BIGINT}, not a {@code TIMESTAMPTZ}:
   * Oracle rejects a time-zone timestamp in a primary key, and the integer sorts the same way on
   * every backend. {@code updated_at} is the saga's last status change, which equals {@code
   * sweep_at} outside the timeout slice.
   *
   * @return the metadata of {@code saga_sweep}
   */
  public static TableMetadata sagaSweepTable() {
    return TableMetadata.newBuilder()
        .addColumn("bucket", DataType.INT) // PK: hash(saga_id) % numBuckets
        .addColumn("slice", DataType.INT) // CK1: status code, or the timeout slice
        .addColumn("sweep_at", DataType.BIGINT) // CK2: sweep position, epoch millis
        .addColumn("saga_id", DataType.TEXT) // CK3: unique identifier
        .addColumn("saga_name", DataType.TEXT)
        .addColumn("definition_version", DataType.TEXT) // saga definition version at creation
        .addColumn("created_at", DataType.TIMESTAMPTZ)
        .addColumn("updated_at", DataType.TIMESTAMPTZ) // last status change
        .addPartitionKey("bucket")
        .addClusteringKey("slice", Scan.Ordering.Order.ASC)
        .addClusteringKey("sweep_at", Scan.Ordering.Order.ASC)
        .addClusteringKey("saga_id", Scan.Ordering.Order.ASC)
        .build();
  }

  /**
   * The sweep slice of a parked step that has a timeout. Persisted, so it freezes at the first
   * release like the {@link SagaStatus} codes it sits beside.
   */
  public static final int WAITING_WITH_TIMEOUT_SLICE = 6;

  /**
   * The sweep slice for a saga in {@code status}.
   *
   * @param status the saga's status
   * @param hasTimeout whether a {@code WAITING} saga's parked step has a timeout; ignored for every
   *     other status
   * @return the slice code
   */
  static int sliceOf(SagaStatus status, boolean hasTimeout) {
    return status == SagaStatus.WAITING && hasTimeout
        ? WAITING_WITH_TIMEOUT_SLICE
        : status.getStatusCode();
  }

  /**
   * The status a sweep slice holds.
   *
   * @param slice a slice code
   * @return the status
   * @throws IllegalArgumentException if {@code slice} is not a slice code
   */
  static SagaStatus statusOfSlice(int slice) {
    return slice == WAITING_WITH_TIMEOUT_SLICE
        ? SagaStatus.WAITING
        : SagaStatus.fromStatusCode(slice);
  }

  /**
   * Saga definition registry.
   *
   * <p>Partition key: {@code saga_name}. Clustering key: {@code definition_version}.
   *
   * @return the metadata of {@code saga_definitions}
   */
  public static TableMetadata sagaDefinitionsTable() {
    return TableMetadata.newBuilder()
        .addColumn("saga_name", DataType.TEXT) // PK
        .addColumn("definition_version", DataType.TEXT) // CK
        .addColumn("definition_json", DataType.TEXT) // full serialized SagaDefinition
        .addColumn("registered_at", DataType.TIMESTAMPTZ)
        .addPartitionKey("saga_name")
        .addClusteringKey("definition_version", Scan.Ordering.Order.ASC)
        .build();
  }

  /**
   * Creates all saga tables using the ScalarDB Admin API, passing {@code options} to the namespace
   * and every table. Idempotent — uses {@code ifNotExists}, so the options only shape what is
   * created now.
   *
   * @param admin the ScalarDB admin interface
   * @param options ScalarDB creation options, such as {@code replication-factor} for Cassandra
   * @throws ExecutionException if a table creation fails
   */
  public static void createAll(Admin admin, Map<String, String> options) throws ExecutionException {
    admin.createNamespace(NAMESPACE, true, options);
    admin.createTable(NAMESPACE, EVENTS_TABLE, sagaEventsTable(), true, options);
    admin.createTable(NAMESPACE, SWEEP_TABLE, sagaSweepTable(), true, options);
    admin.createTable(NAMESPACE, DEFINITIONS_TABLE, sagaDefinitionsTable(), true, options);
  }
}
