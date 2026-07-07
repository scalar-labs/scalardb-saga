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

  @Test
  void fromWireName_knownWireNameGiven_returnsRole() {
    // Assert
    assertThat(SagaRole.fromWireName("saga:read")).contains(SagaRole.READ);
    assertThat(SagaRole.fromWireName("saga:write")).contains(SagaRole.WRITE);
    assertThat(SagaRole.fromWireName("saga:admin")).contains(SagaRole.ADMIN);
  }

  @Test
  void fromWireName_unknownOrShortNameGiven_returnsEmpty() {
    // Assert — only the exact wire name matches (not the enum's own name)
    assertThat(SagaRole.fromWireName("read")).isEmpty();
    assertThat(SagaRole.fromWireName("SAGA:READ")).isEmpty();
    assertThat(SagaRole.fromWireName("saga:superuser")).isEmpty();
  }
}
