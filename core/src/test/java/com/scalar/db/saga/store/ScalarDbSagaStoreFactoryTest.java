package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class ScalarDbSagaStoreFactoryTest {

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
}
