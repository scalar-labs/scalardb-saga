package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaStatusTest {

  @Test
  void fromStatusCode_validCodesGiven_returnsCorrectStatus() {
    // Act & Assert
    assertThat(SagaStatus.fromStatusCode(0)).isEqualTo(SagaStatus.RUNNING);
    assertThat(SagaStatus.fromStatusCode(1)).isEqualTo(SagaStatus.COMPLETED);
    assertThat(SagaStatus.fromStatusCode(2)).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(SagaStatus.fromStatusCode(3)).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(SagaStatus.fromStatusCode(4)).isEqualTo(SagaStatus.ESCALATED);
  }

  @Test
  void fromStatusCode_negativeCodeGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaStatus.fromStatusCode(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromStatusCode_unknownCodeGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaStatus.fromStatusCode(99))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getStatusCode_allStatuses_returnsExpectedCodes() {
    // Act & Assert
    assertThat(SagaStatus.RUNNING.getStatusCode()).isEqualTo(0);
    assertThat(SagaStatus.COMPLETED.getStatusCode()).isEqualTo(1);
    assertThat(SagaStatus.COMPENSATING.getStatusCode()).isEqualTo(2);
    assertThat(SagaStatus.COMPENSATED.getStatusCode()).isEqualTo(3);
    assertThat(SagaStatus.ESCALATED.getStatusCode()).isEqualTo(4);
  }

  @Test
  void isTerminal_terminalStatuses_returnsTrue() {
    // Act & Assert
    assertThat(SagaStatus.COMPLETED.isTerminal()).isTrue();
    assertThat(SagaStatus.COMPENSATED.isTerminal()).isTrue();
    assertThat(SagaStatus.ESCALATED.isTerminal()).isTrue();
  }

  @Test
  void isTerminal_nonTerminalStatuses_returnsFalse() {
    // Act & Assert
    assertThat(SagaStatus.RUNNING.isTerminal()).isFalse();
    assertThat(SagaStatus.COMPENSATING.isTerminal()).isFalse();
  }

  @Test
  void isPurgeable_purgeableStatuses_returnsTrue() {
    // Act & Assert
    assertThat(SagaStatus.COMPLETED.isPurgeable()).isTrue();
    assertThat(SagaStatus.COMPENSATED.isPurgeable()).isTrue();
  }

  @Test
  void isPurgeable_nonPurgeableStatuses_returnsFalse() {
    // Act & Assert
    assertThat(SagaStatus.RUNNING.isPurgeable()).isFalse();
    assertThat(SagaStatus.COMPENSATING.isPurgeable()).isFalse();
    assertThat(SagaStatus.ESCALATED.isPurgeable()).isFalse();
  }
}
