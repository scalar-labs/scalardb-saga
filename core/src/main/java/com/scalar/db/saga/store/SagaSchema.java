package com.scalar.db.saga.store;

import com.scalar.db.api.Admin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;

/**
 * Schema definitions for the saga persistence tables.
 *
 * <p>Defines three tables:
 *
 * <ul>
 *   <li>{@code saga_events} — append-only event log, partitioned by saga ID
 *   <li>{@code saga_state} — mutable status/recovery table, bucket-partitioned for parallel scans
 *   <li>{@code saga_definitions} — saga definition registry
 * </ul>
 */
public final class SagaSchema {

  public static final String NAMESPACE = "saga";
  public static final String EVENTS_TABLE = "saga_events";
  public static final String STATE_TABLE = "saga_state";
  public static final String DEFINITIONS_TABLE = "saga_definitions";

  /** Default number of buckets for partitioning {@code saga_state}. */
  public static final int DEFAULT_NUM_BUCKETS = 16;

  private final int numBuckets;

  public SagaSchema() {
    this(DEFAULT_NUM_BUCKETS);
  }

  /**
   * Creates a schema with the specified number of buckets.
   *
   * <p><b>Important:</b> {@code numBuckets} must remain constant once data has been written to the
   * {@code saga_state} table. Changing it causes existing sagas to map to different bucket
   * partitions, breaking lookups in {@code recordStatusEvent} and {@code claimForRecovery}.
   *
   * @param numBuckets the number of bucket partitions (must be &gt; 0)
   */
  public SagaSchema(int numBuckets) {
    if (numBuckets <= 0) {
      throw new IllegalArgumentException("numBuckets must be > 0, got " + numBuckets);
    }
    this.numBuckets = numBuckets;
  }

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
   * Append-only event log.
   *
   * <p>Partition key: {@code saga_id}. Clustering key: {@code sequence} (ascending).
   *
   * <p>Every state change is a single INSERT. No UPDATEs, no DELETEs. ScalarDB's clustering key
   * ensures efficient ordered scan by saga ID.
   */
  public static TableMetadata sagaEventsTable() {
    return TableMetadata.newBuilder()
        .addColumn("saga_id", DataType.TEXT) // PK
        .addColumn("sequence", DataType.INT) // CK: monotonically increasing per saga
        .addColumn("event_type", DataType.TEXT) // SAGA_STARTED, STEP_COMPLETED, etc.
        .addColumn("step_index", DataType.INT) // step index (-1 for saga-level events)
        .addColumn("step_name", DataType.TEXT) // step name (null for saga-level events)
        .addColumn("payload", DataType.TEXT) // JSON: step result, error, input, etc.
        .addColumn("created_at", DataType.TIMESTAMPTZ)
        .addPartitionKey("saga_id")
        .addClusteringKey("sequence", Scan.Ordering.Order.ASC)
        .build();
  }

  /**
   * Mutable status/recovery table, bucket-partitioned for parallel recovery scans.
   *
   * <p>Partition key: {@code bucket}. Clustering key: {@code (status, updated_at, saga_id)}.
   * Secondary index on {@code saga_id} for fast single-saga lookups.
   *
   * <p>One row per saga. Written on saga start and on each status transition. Because {@code
   * status} and {@code updated_at} are part of the clustering key (immutable in ScalarDB),
   * transitions require DELETE old row + INSERT new row in one transaction.
   *
   * <p>Bucket-based partitioning distributes recovery scans across database nodes — each bucket is
   * a separate partition, avoiding hot-partition problems that would occur if status alone were the
   * partition key. Clustering key design enables efficient recovery scans: scan each bucket with
   * {@code status=RUNNING} and {@code updated_at <= threshold}, reading only stale active sagas.
   */
  public static TableMetadata sagaStateTable() {
    return TableMetadata.newBuilder()
        .addColumn("bucket", DataType.INT) // PK: hash(saga_id) % numBuckets
        .addColumn("status", DataType.INT) // CK1: SagaStatus ordinal
        .addColumn("updated_at", DataType.TIMESTAMPTZ) // CK2: last state-change time
        .addColumn("saga_id", DataType.TEXT) // CK3: unique identifier
        .addColumn("saga_name", DataType.TEXT)
        .addColumn("owner_id", DataType.TEXT) // replica processing this saga (observability)
        .addColumn("definition_version", DataType.TEXT) // saga definition version at creation
        .addColumn("created_at", DataType.TIMESTAMPTZ)
        .addPartitionKey("bucket")
        .addClusteringKey("status", Scan.Ordering.Order.ASC)
        .addClusteringKey("updated_at", Scan.Ordering.Order.ASC)
        .addClusteringKey("saga_id", Scan.Ordering.Order.ASC)
        .addSecondaryIndex("saga_id")
        .build();
  }

  /**
   * Saga definition registry.
   *
   * <p>Partition key: {@code saga_name}. Clustering key: {@code definition_version}.
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
   * Creates all saga tables using the ScalarDB Admin API. Idempotent — uses {@code ifNotExists}.
   *
   * @param admin the ScalarDB admin interface
   * @throws ExecutionException if a table creation fails
   */
  public static void createAll(Admin admin) throws ExecutionException {
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(NAMESPACE, EVENTS_TABLE, sagaEventsTable(), true);
    admin.createTable(NAMESPACE, STATE_TABLE, sagaStateTable(), true);
    admin.createTable(NAMESPACE, DEFINITIONS_TABLE, sagaDefinitionsTable(), true);
  }
}
