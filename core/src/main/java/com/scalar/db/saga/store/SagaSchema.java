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
   */
  public static TableMetadata sagaEventsTable() {
    return TableMetadata.newBuilder()
        .addColumn("saga_id", DataType.TEXT)
        .addColumn("sequence", DataType.INT)
        .addColumn("event_type", DataType.TEXT)
        .addColumn("step_index", DataType.INT)
        .addColumn("step_name", DataType.TEXT)
        .addColumn("payload", DataType.TEXT)
        .addColumn("created_at", DataType.TIMESTAMPTZ)
        .addPartitionKey("saga_id")
        .addClusteringKey("sequence", Scan.Ordering.Order.ASC)
        .build();
  }

  /**
   * Mutable status/recovery table, bucket-partitioned for parallel recovery scans.
   *
   * <p>Partition key: {@code bucket}. Clustering key: {@code (status, updated_at, saga_id)}.
   * Secondary index on {@code saga_id} for fast lookups.
   */
  public static TableMetadata sagaStateTable() {
    return TableMetadata.newBuilder()
        .addColumn("bucket", DataType.INT)
        .addColumn("status", DataType.INT)
        .addColumn("updated_at", DataType.TIMESTAMPTZ)
        .addColumn("saga_id", DataType.TEXT)
        .addColumn("saga_name", DataType.TEXT)
        .addColumn("owner_id", DataType.TEXT)
        .addColumn("version", DataType.INT)
        .addColumn("definition_version", DataType.TEXT)
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
        .addColumn("saga_name", DataType.TEXT)
        .addColumn("definition_version", DataType.TEXT)
        .addColumn("definition_json", DataType.TEXT)
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
