package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ScalarDbSagaStoreFactoryTest {

  /** A 1 ms interval halves to zero, so every pause is a zero-length sleep. */
  private static final RetryPolicy NO_PAUSE =
      RetryPolicy.newBuilder().maxAttempts(3).initialIntervalMillis(1).maxIntervalMillis(1).build();

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

  /**
   * Checked before anything connects. The message is asserted because an unconfigured {@code
   * TransactionFactory} throws the same exception type later in {@code create}.
   */
  @Test
  void create_jdbcTransactionManagerGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.transaction_manager", "jdbc");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.create(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scalar.db.transaction_manager");
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
  void validateTransactionManager_otherManagerGiven_throwsIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.transaction_manager", "cluster");

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.validateTransactionManager(props))
        .isInstanceOf(IllegalArgumentException.class);
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
        .createCoordinatorTables(true);

    // Act
    ScalarDbSagaStoreFactory.createSchema(() -> admin, NO_PAUSE);

    // Assert
    verify(admin, times(2)).createCoordinatorTables(true);
    verify(admin, times(2)).close();
  }

  @Test
  void createSchema_everyAttemptFails_throwsStoreUnavailableWithLastCause() throws Exception {
    // Arrange
    DistributedTransactionAdmin admin = mock(DistributedTransactionAdmin.class);
    IllegalArgumentException cause =
        new IllegalArgumentException("DB-CORE-10050: The namespace already exists");
    doThrow(cause).when(admin).createCoordinatorTables(true);

    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreFactory.createSchema(() -> admin, NO_PAUSE))
        .isInstanceOf(SagaPersistenceException.class)
        .hasCause(cause)
        .extracting(e -> ((SagaPersistenceException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);
    verify(admin, times(NO_PAUSE.getMaxAttempts())).createCoordinatorTables(true);
  }

  /** A shutdown during the pause must not be swallowed into another attempt. */
  @Test
  void createSchema_interruptedWhileWaitingToRetry_throwsOperationAbortedAndKeepsInterrupt()
      throws Exception {
    // Arrange
    DistributedTransactionAdmin admin = mock(DistributedTransactionAdmin.class);
    doThrow(new IllegalArgumentException("DB-CORE-10050: The namespace already exists"))
        .when(admin)
        .createCoordinatorTables(true);
    RetryPolicy longPause =
        RetryPolicy.newBuilder().initialIntervalMillis(60_000).maxIntervalMillis(60_000).build();
    Thread.currentThread().interrupt();

    try {
      // Act & Assert
      assertThatThrownBy(() -> ScalarDbSagaStoreFactory.createSchema(() -> admin, longPause))
          .isInstanceOf(SagaPersistenceException.class)
          .hasCauseInstanceOf(InterruptedException.class)
          .extracting(e -> ((SagaPersistenceException) e).getErrorCode())
          .isEqualTo(SagaErrorCode.OPERATION_ABORTED);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // Clear the flag so it does not leak into the next test on this thread.
      Thread.interrupted();
    }
    verify(admin, times(1)).createCoordinatorTables(true);
  }
}
