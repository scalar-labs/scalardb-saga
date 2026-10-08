package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ScalarDbSagaStoreFactoryTest {

  /** A 1 ms interval halves to zero, so every pause is a zero-length sleep. */
  private static final RetryPolicy NO_PAUSE =
      RetryPolicy.newBuilder().maxAttempts(3).initialIntervalMillis(1).maxIntervalMillis(1).build();

  private static final Map<String, String> NO_OPTIONS = Map.of();

  /**
   * Nothing else validates the {@code scalar.db.saga.store.} namespace, so a removed key that is
   * merely dropped from the parser reads as unset: the deployment starts and runs on a default the
   * operator believes they overrode. The message is asserted because an unconfigured {@code
   * TransactionFactory} throws the same exception type later in {@code create}, which would let
   * this pass for the wrong reason.
   */
  @Test
  void create_removedRecoveryScanLimitKeyGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.saga.store.recovery_scan_limit", "500");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.create(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("recovery_scan_limit")
        .hasMessageContaining("has been removed");
  }

  /** Renamed, not removed: the old spelling must fail rather than silently fall back to 16. */
  @Test
  void create_oldNumBucketsKeyGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.saga.store.num_buckets", "4");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.create(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scalar.db.saga.store.scalardb.num_buckets");
  }

  @Test
  void parseCreationOptions_prefixedKeysGiven_returnsNamesWithTrimmedValues() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.saga.store.scalardb.creation_options.no-scaling", " true ");
    props.setProperty("scalar.db.saga.store.scalardb.creation_options.ru", "1000");
    props.setProperty("scalar.db.saga.store.scalardb.num_buckets", "4");
    props.setProperty("scalar.db.storage", "dynamo");

    // Act
    Map<String, String> options = ScalarDbSagaStoreFactory.parseCreationOptions(props);

    // Assert
    assertThat(options).containsExactly(entry("no-scaling", "true"), entry("ru", "1000"));
  }

  @Test
  void parseCreationOptions_noPrefixedKeys_returnsEmptyMap() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "cassandra");

    // Act
    Map<String, String> options = ScalarDbSagaStoreFactory.parseCreationOptions(props);

    // Assert
    assertThat(options).isEmpty();
  }

  @Test
  void parseCreationOptions_keyWithoutOptionNameGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.saga.store.scalardb.creation_options.", "1");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.parseCreationOptions(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void createSchema_optionsGiven_passesThemToEveryCreationCall() throws Exception {
    // Arrange
    DistributedTransactionAdmin admin = mock(DistributedTransactionAdmin.class);
    Map<String, String> options = Map.of("replication-factor", "1");

    // Act
    ScalarDbSagaStoreFactory.createSchema(() -> admin, NO_PAUSE, options);

    // Assert
    verify(admin).createCoordinatorTables(true, options);
    verify(admin).createNamespace(SagaSchema.NAMESPACE, true, options);
    verify(admin, times(4))
        .createTable(
            eq(SagaSchema.NAMESPACE), anyString(), any(TableMetadata.class), eq(true), eq(options));
  }

  /**
   * The failure a replica sees when a sibling creates the coordinator namespace between this
   * replica's existence check and its own create. The rerun finds everything in place and returns.
   */
  @Test
  void createSchema_siblingReplicaWinsFirstAttempt_retriesAndReturns() throws Exception {
    // Arrange
    DistributedTransactionAdmin admin = mock(DistributedTransactionAdmin.class);
    doThrow(new IllegalArgumentException("DB-CORE-10050: The namespace already exists"))
        .doNothing()
        .when(admin)
        .createCoordinatorTables(true, NO_OPTIONS);

    // Act
    ScalarDbSagaStoreFactory.createSchema(() -> admin, NO_PAUSE, NO_OPTIONS);

    // Assert
    verify(admin, times(2)).createCoordinatorTables(true, NO_OPTIONS);
    verify(admin, times(2)).close();
  }

  @Test
  void createSchema_everyAttemptFails_throwsStoreUnavailableWithLastCause() throws Exception {
    // Arrange
    DistributedTransactionAdmin admin = mock(DistributedTransactionAdmin.class);
    IllegalArgumentException cause =
        new IllegalArgumentException("DB-CORE-10050: The namespace already exists");
    doThrow(cause).when(admin).createCoordinatorTables(true, NO_OPTIONS);

    // Act & Assert
    assertThatThrownBy(
            () -> ScalarDbSagaStoreFactory.createSchema(() -> admin, NO_PAUSE, NO_OPTIONS))
        .isInstanceOf(SagaPersistenceException.class)
        .hasCause(cause)
        .extracting(e -> ((SagaPersistenceException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);
    verify(admin, times(NO_PAUSE.getMaxAttempts())).createCoordinatorTables(true, NO_OPTIONS);
  }

  /** A shutdown during the pause must not be swallowed into another attempt. */
  @Test
  void createSchema_interruptedWhileWaitingToRetry_throwsOperationAbortedAndKeepsInterrupt()
      throws Exception {
    // Arrange
    DistributedTransactionAdmin admin = mock(DistributedTransactionAdmin.class);
    doThrow(new IllegalArgumentException("DB-CORE-10050: The namespace already exists"))
        .when(admin)
        .createCoordinatorTables(true, NO_OPTIONS);
    RetryPolicy longPause =
        RetryPolicy.newBuilder().initialIntervalMillis(60_000).maxIntervalMillis(60_000).build();
    Thread.currentThread().interrupt();

    try {
      // Act & Assert
      assertThatThrownBy(
              () -> ScalarDbSagaStoreFactory.createSchema(() -> admin, longPause, NO_OPTIONS))
          .isInstanceOf(SagaPersistenceException.class)
          .hasCauseInstanceOf(InterruptedException.class)
          .extracting(e -> ((SagaPersistenceException) e).getErrorCode())
          .isEqualTo(SagaErrorCode.OPERATION_ABORTED);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // Clear the flag so it does not leak into the next test on this thread.
      Thread.interrupted();
    }
    verify(admin, times(1)).createCoordinatorTables(true, NO_OPTIONS);
  }
}
