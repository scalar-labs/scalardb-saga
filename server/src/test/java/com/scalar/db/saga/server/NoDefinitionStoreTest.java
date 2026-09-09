package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.definition.SagaDefinition;
import org.junit.jupiter.api.Test;

/**
 * The store stand-in {@code --validate-config} uses. Only {@code register} has behavior worth
 * pinning: the two readers answer "nothing is registered" because offline there is no store, and a
 * test of that would only restate the field.
 */
class NoDefinitionStoreTest {

  @Test
  void register_anyDefinitionGiven_throws() {
    // Validation stops before a pass applies anything, so reaching this is a bug rather than a
    // no-op: it would mean writing to a store the caller said it did not have.
    // The definition is irrelevant: nothing about it can make this call legitimate.
    SagaDefinition definition = mock(SagaDefinition.class);

    assertThatThrownBy(() -> new NoDefinitionStore().register(definition))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void latest_anyNameGiven_reportsNothingRegistered() {
    // The honest offline answer, and what makes servingInsteadOf leave a rollback alone.
    assertThat(new NoDefinitionStore().latest("order-saga")).isNull();
    assertThat(new NoDefinitionStore().isRegistered("order-saga", "1")).isFalse();
  }
}
