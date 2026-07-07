package com.scalar.db.saga.daemon.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SagaRoleTest {

  @Test
  void wireName_returnsSagaPrefixedName() {
    // Assert
    assertThat(SagaRole.READ.wireName()).isEqualTo("saga:read");
    assertThat(SagaRole.WRITE.wireName()).isEqualTo("saga:write");
    assertThat(SagaRole.ADMIN.wireName()).isEqualTo("saga:admin");
  }

  @Test
  void implies_sameRole_returnsTrue() {
    // Assert
    assertThat(SagaRole.READ.implies(SagaRole.READ)).isTrue();
    assertThat(SagaRole.WRITE.implies(SagaRole.WRITE)).isTrue();
    assertThat(SagaRole.ADMIN.implies(SagaRole.ADMIN)).isTrue();
  }

  @Test
  void implies_higherRoleGiven_returnsTrueForLowerRequirement() {
    // Assert — ADMIN > WRITE > READ
    assertThat(SagaRole.ADMIN.implies(SagaRole.WRITE)).isTrue();
    assertThat(SagaRole.ADMIN.implies(SagaRole.READ)).isTrue();
    assertThat(SagaRole.WRITE.implies(SagaRole.READ)).isTrue();
  }

  @Test
  void implies_lowerRoleGiven_returnsFalseForHigherRequirement() {
    // Assert
    assertThat(SagaRole.READ.implies(SagaRole.WRITE)).isFalse();
    assertThat(SagaRole.READ.implies(SagaRole.ADMIN)).isFalse();
    assertThat(SagaRole.WRITE.implies(SagaRole.ADMIN)).isFalse();
  }
}
