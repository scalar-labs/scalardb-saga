package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.Admin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SagaSchemaTest {

  @Mock private Admin admin;

  // --- Constructor ---

  @Test
  void constructor_defaultBuckets_uses16() {
    // Act
    SagaSchema schema = new SagaSchema();

    // Assert
    assertThat(schema.getNumBuckets()).isEqualTo(16);
  }

  @Test
  void constructor_customBucketsGiven_usesSpecifiedValue() {
    // Act
    SagaSchema schema = new SagaSchema(32);

    // Assert
    assertThat(schema.getNumBuckets()).isEqualTo(32);
  }

  @Test
  void constructor_zeroBucketsGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaSchema(0)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeBucketsGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaSchema(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  // --- bucketOf ---

  @Test
  void bucketOf_sagaIdGiven_returnsNonNegativeWithinRange() {
    // Arrange
    SagaSchema schema = new SagaSchema(16);

    // Act
    int bucket = schema.bucketOf("test-saga-123");

    // Assert
    assertThat(bucket).isBetween(0, 15);
  }

  @Test
  void bucketOf_sameSagaId_returnsSameBucket() {
    // Arrange
    SagaSchema schema = new SagaSchema(16);

    // Act
    int bucket1 = schema.bucketOf("saga-abc");
    int bucket2 = schema.bucketOf("saga-abc");

    // Assert
    assertThat(bucket1).isEqualTo(bucket2);
  }

  @Test
  void bucketOf_singleBucket_alwaysReturnsZero() {
    // Arrange
    SagaSchema schema = new SagaSchema(1);

    // Act & Assert
    assertThat(schema.bucketOf("any-saga-id")).isEqualTo(0);
    assertThat(schema.bucketOf("another-saga")).isEqualTo(0);
  }

  // --- Table metadata ---

  @Test
  void sagaEventsTable_called_returnsMetadataWithExpectedColumns() {
    // Act
    TableMetadata metadata = SagaSchema.sagaEventsTable();

    // Assert
    assertThat(metadata.getColumnNames())
        .containsExactlyInAnyOrder(
            "saga_id",
            "sequence",
            "event_type",
            "step_index",
            "step_name",
            "payload",
            "created_at");
    assertThat(metadata.getPartitionKeyNames()).containsExactly("saga_id");
    assertThat(metadata.getClusteringKeyNames()).containsExactly("sequence");
  }

  @Test
  void sagaStateTable_called_returnsMetadataWithExpectedColumns() {
    // Act
    TableMetadata metadata = SagaSchema.sagaStateTable();

    // Assert
    assertThat(metadata.getColumnNames())
        .containsExactlyInAnyOrder(
            "bucket",
            "status",
            "updated_at",
            "saga_id",
            "saga_name",
            "owner_id",
            "version",
            "definition_version",
            "created_at");
    assertThat(metadata.getPartitionKeyNames()).containsExactly("bucket");
    assertThat(metadata.getClusteringKeyNames()).containsExactly("status", "updated_at", "saga_id");
    assertThat(metadata.getSecondaryIndexNames()).contains("saga_id");
  }

  @Test
  void sagaDefinitionsTable_called_returnsMetadataWithExpectedColumns() {
    // Act
    TableMetadata metadata = SagaSchema.sagaDefinitionsTable();

    // Assert
    assertThat(metadata.getColumnNames())
        .containsExactlyInAnyOrder(
            "saga_name", "definition_version", "definition_json", "registered_at");
    assertThat(metadata.getPartitionKeyNames()).containsExactly("saga_name");
    assertThat(metadata.getClusteringKeyNames()).containsExactly("definition_version");
  }

  // --- createAll ---

  @Test
  void createAll_called_createsNamespaceAndAllTables() throws ExecutionException {
    // Act
    SagaSchema.createAll(admin);

    // Assert
    verify(admin).createNamespace(SagaSchema.NAMESPACE, true);
    verify(admin)
        .createTable(
            eq(SagaSchema.NAMESPACE),
            eq(SagaSchema.EVENTS_TABLE),
            any(TableMetadata.class),
            eq(true));
    verify(admin)
        .createTable(
            eq(SagaSchema.NAMESPACE),
            eq(SagaSchema.STATE_TABLE),
            any(TableMetadata.class),
            eq(true));
    verify(admin)
        .createTable(
            eq(SagaSchema.NAMESPACE),
            eq(SagaSchema.DEFINITIONS_TABLE),
            any(TableMetadata.class),
            eq(true));
  }

  // --- Constants ---

  @Test
  void constants_checked_haveExpectedValues() {
    // Assert
    assertThat(SagaSchema.NAMESPACE).isEqualTo("saga");
    assertThat(SagaSchema.EVENTS_TABLE).isEqualTo("saga_events");
    assertThat(SagaSchema.STATE_TABLE).isEqualTo("saga_state");
    assertThat(SagaSchema.DEFINITIONS_TABLE).isEqualTo("saga_definitions");
    assertThat(SagaSchema.DEFAULT_NUM_BUCKETS).isEqualTo(16);
  }
}
