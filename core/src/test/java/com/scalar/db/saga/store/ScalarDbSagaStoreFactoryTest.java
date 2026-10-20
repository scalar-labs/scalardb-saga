package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
   * A removed key gets a message of its own, naming what replaced it, ahead of the generic
   * unknown-key check that would also reject it. The message is asserted because that check, and an
   * unconfigured {@code TransactionFactory} later in {@code create}, throw the same exception type,
   * which would let this pass for the wrong reason.
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

  @Test
  void create_jdbcTransactionManagerGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.transaction_manager", "jdbc");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.create(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validateTransactionManager_keyUnset_passes() {
    // Arrange — ScalarDB defaults to Consensus Commit
    Properties props = new Properties();

    // Act & Assert
    assertThatCode(() -> ScalarDbSagaStoreFactory.validateTransactionManager(props))
        .doesNotThrowAnyException();
  }

  @Test
  void validateTransactionManager_consensusCommitGiven_passes() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.transaction_manager", "consensus-commit");

    // Act & Assert
    assertThatCode(() -> ScalarDbSagaStoreFactory.validateTransactionManager(props))
        .doesNotThrowAnyException();
  }

  @Test
  void validateTransactionManager_consensusCommitInOtherCaseGiven_passes() {
    // Arrange — ScalarDB resolves the name case-insensitively
    Properties props = new Properties();
    props.setProperty("scalar.db.transaction_manager", "Consensus-Commit");

    // Act & Assert
    assertThatCode(() -> ScalarDbSagaStoreFactory.validateTransactionManager(props))
        .doesNotThrowAnyException();
  }

  @Test
  void validateTransactionManager_otherManagerGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.transaction_manager", "cluster");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.validateTransactionManager(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * A misspelled key must fail rather than leave its setting at the default. The message is
   * asserted for the reason given on the removed-key test above.
   */
  @Test
  void create_misspelledStoreKeyGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.saga.store.transaction_retry_cont", "5");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.create(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Unknown configuration key 'scalar.db.saga.store.transaction_retry_cont'");
  }

  @Test
  void rejectUnknownKeys_knownStoreAndOtherKeysGiven_passes() {
    // Arrange — every store key the factory reads, plus keys outside its namespace.
    Properties props = new Properties();
    props.setProperty("scalar.db.saga.store.max_event_payload_bytes", "1024");
    props.setProperty("scalar.db.saga.store.transaction_retry_count", "3");
    props.setProperty("scalar.db.saga.store.scalardb.num_buckets", "4");
    props.setProperty("scalar.db.saga.store.scalardb.creation_options.replication-factor", "3");
    props.setProperty("scalar.db.storage", "cassandra");
    props.setProperty("scalar.db.saga.server.http.port", "8080");

    // Act & Assert
    assertThatCode(() -> ScalarDbSagaStoreFactory.rejectUnknownKeys(props))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectUnknownKeys_keyBelowScalardbPrefixGiven_throwsIllegalArgumentException() {
    // Arrange — under the ScalarDB-specific prefix, but not a key the factory reads.
    Properties props = new Properties();
    props.setProperty("scalar.db.saga.store.scalardb.numbuckets", "4");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.rejectUnknownKeys(props))
        .isInstanceOf(IllegalArgumentException.class);
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
