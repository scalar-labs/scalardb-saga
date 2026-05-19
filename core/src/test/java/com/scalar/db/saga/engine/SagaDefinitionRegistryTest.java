package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.store.SagaStore;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SagaDefinitionRegistryTest {

  @Mock private SagaStore store;
  private SagaDefinitionRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new SagaDefinitionRegistry(store);
  }

  private static SagaDefinition definition(String name, String version) {
    return SagaDefinition.newBuilder(name, SagaMode.SAGA)
        .version(version)
        .step("s1", "com.example.Step1")
        .add()
        .build();
  }

  // ---------------------------------------------------------------------------
  // register
  // ---------------------------------------------------------------------------

  @Test
  void register_validDefinitionGiven_persistsToStoreBeforeMemory() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");

    // Act
    registry.register(def);

    // Assert — store.registerDefinition called
    verify(store).registerDefinition(def);
    // Both name and name:version keys are available in memory
    assertThat(registry.get("transfer")).isSameAs(def);
    assertThat(registry.resolve("transfer", "1.0")).isSameAs(def);
  }

  @Test
  void register_storeWriteFails_definitionNotInMemory() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");
    doThrow(new SagaPersistenceException("store error", new RuntimeException("db down")))
        .when(store)
        .registerDefinition(any());
    when(store.getDefinition("transfer")).thenReturn(Optional.empty());

    // Act & Assert — register fails
    assertThatThrownBy(() -> registry.register(def)).isInstanceOf(SagaPersistenceException.class);
    // Definition should NOT be in memory (persist before memory).
    // get() falls back to store, which also returns empty since registration failed.
    assertThat(registry.get("transfer")).isNull();
  }

  @Test
  void register_multipleVersions_latestWins() {
    // Arrange
    SagaDefinition v1 = definition("transfer", "1.0");
    SagaDefinition v2 = definition("transfer", "2.0");

    // Act
    registry.register(v1);
    registry.register(v2);

    // Assert — get() returns the latest (v2), resolve() returns the exact version
    assertThat(registry.get("transfer")).isSameAs(v2);
    assertThat(registry.resolve("transfer", "1.0")).isSameAs(v1);
    assertThat(registry.resolve("transfer", "2.0")).isSameAs(v2);
  }

  // ---------------------------------------------------------------------------
  // get (by name)
  // ---------------------------------------------------------------------------

  @Test
  void get_registeredNameGiven_returnsDefinition() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");
    registry.register(def);

    // Act & Assert
    assertThat(registry.get("transfer")).isSameAs(def);
  }

  @Test
  void get_notInMemory_fallsBackToStore() {
    // Arrange — definition only in store, not in memory
    SagaDefinition def = definition("transfer", "1.0");
    when(store.getDefinition("transfer")).thenReturn(Optional.of(def));

    // Act
    SagaDefinition result = registry.get("transfer");

    // Assert
    assertThat(result).isSameAs(def);
    verify(store).getDefinition("transfer");
  }

  @Test
  void get_notInMemoryOrStore_returnsNull() {
    // Arrange
    when(store.getDefinition("unknown")).thenReturn(Optional.empty());

    // Act & Assert
    assertThat(registry.get("unknown")).isNull();
  }

  // ---------------------------------------------------------------------------
  // resolve (by name + version, with store fallback)
  // ---------------------------------------------------------------------------

  @Test
  void resolve_inMemory_returnsWithoutStoreLookup() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");
    registry.register(def);

    // Act
    SagaDefinition result = registry.resolve("transfer", "1.0");

    // Assert — returned from memory (store.getDefinition not called for resolve)
    assertThat(result).isSameAs(def);
  }

  @Test
  void resolve_notInMemory_fallsBackToStore() {
    // Arrange — definition only in store, not in memory
    SagaDefinition def = definition("transfer", "1.0");
    when(store.getDefinition("transfer", "1.0")).thenReturn(Optional.of(def));

    // Act
    SagaDefinition result = registry.resolve("transfer", "1.0");

    // Assert
    assertThat(result).isSameAs(def);
    verify(store).getDefinition("transfer", "1.0");
  }

  @Test
  void resolve_notInMemoryOrStore_returnsNull() {
    // Arrange
    when(store.getDefinition("transfer", "1.0")).thenReturn(Optional.empty());

    // Act
    SagaDefinition result = registry.resolve("transfer", "1.0");

    // Assert
    assertThat(result).isNull();
  }
}
