package com.scalar.db.saga.daemon.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SagaIdentityTest {

  @Test
  void of_principalAndRolesGiven_exposesThem() {
    // Act
    SagaIdentity identity = SagaIdentity.of("alice", EnumSet.of(SagaRole.READ, SagaRole.WRITE));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice");
    assertThat(identity.roles()).containsExactlyInAnyOrder(SagaRole.READ, SagaRole.WRITE);
  }

  @Test
  void of_emptyRolesGiven_hasNoRole() {
    // Act
    SagaIdentity identity = SagaIdentity.of("alice", Set.of());

    // Assert
    assertThat(identity.roles()).isEmpty();
    assertThat(identity.hasRole(SagaRole.READ)).isFalse();
  }

  @Test
  void of_blankPrincipalGiven_throwsException() {
    // Act / Assert
    assertThatThrownBy(() -> SagaIdentity.of("  ", Set.of(SagaRole.READ)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @SuppressWarnings("NullAway") // deliberately passing null to exercise the runtime guard
  void of_nullPrincipalGiven_throwsException() {
    // Act / Assert
    assertThatThrownBy(() -> SagaIdentity.of(null, Set.of(SagaRole.READ)))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void roles_returnedSetIsUnmodifiable() {
    // Arrange
    SagaIdentity identity = SagaIdentity.of("alice", Set.of(SagaRole.READ));

    // Act / Assert
    assertThatThrownBy(() -> identity.roles().add(SagaRole.ADMIN))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void of_callerMutatesSourceSet_identityUnaffected() {
    // Arrange
    Set<SagaRole> source = EnumSet.of(SagaRole.READ);
    SagaIdentity identity = SagaIdentity.of("alice", source);

    // Act — mutate the caller's set after construction
    source.add(SagaRole.ADMIN);

    // Assert — the identity kept a defensive copy
    assertThat(identity.roles()).containsExactly(SagaRole.READ);
  }

  @Test
  void hasRole_holdsHigherRole_satisfiesLowerRequirement() {
    // Arrange
    SagaIdentity admin = SagaIdentity.of("root", Set.of(SagaRole.ADMIN));

    // Assert — ADMIN implies WRITE and READ
    assertThat(admin.hasRole(SagaRole.READ)).isTrue();
    assertThat(admin.hasRole(SagaRole.WRITE)).isTrue();
    assertThat(admin.hasRole(SagaRole.ADMIN)).isTrue();
  }

  @Test
  void hasRole_holdsMixedLowerAndHigherRoles_authorizesViaHighest() {
    // Arrange — a set with a gap: READ and ADMIN present, WRITE absent
    SagaIdentity identity = SagaIdentity.of("alice", Set.of(SagaRole.READ, SagaRole.ADMIN));

    // Assert — ADMIN covers the missing WRITE (and READ); the lower READ never restricts
    assertThat(identity.hasRole(SagaRole.READ)).isTrue();
    assertThat(identity.hasRole(SagaRole.WRITE)).isTrue();
    assertThat(identity.hasRole(SagaRole.ADMIN)).isTrue();
  }

  @Test
  void hasRole_holdsOnlyLowerRole_deniesHigherRequirement() {
    // Arrange
    SagaIdentity reader = SagaIdentity.of("bob", Set.of(SagaRole.READ));

    // Assert
    assertThat(reader.hasRole(SagaRole.READ)).isTrue();
    assertThat(reader.hasRole(SagaRole.WRITE)).isFalse();
    assertThat(reader.hasRole(SagaRole.ADMIN)).isFalse();
  }

  @Test
  void equals_sameFields_isEqual() {
    // Arrange
    SagaIdentity a = SagaIdentity.of("alice", Set.of(SagaRole.READ, SagaRole.WRITE));
    SagaIdentity b = SagaIdentity.of("alice", Set.of(SagaRole.WRITE, SagaRole.READ));

    // Assert
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }

  @Test
  void equals_differentPrincipalOrRoles_isNotEqual() {
    // Arrange
    SagaIdentity base = SagaIdentity.of("alice", Set.of(SagaRole.READ));

    // Assert
    assertThat(base).isNotEqualTo(SagaIdentity.of("bob", Set.of(SagaRole.READ)));
    assertThat(base).isNotEqualTo(SagaIdentity.of("alice", Set.of(SagaRole.ADMIN)));
  }
}
