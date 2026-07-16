package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.definition.SagaDefinition;
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
    return SagaDefinition.newBuilder(name)
        .saga()
        .version(version)
        .step("s1", "com.example.Step1")
        .add()
        .build();
  }

  // ---------------------------------------------------------------------------
  // register
  // ---------------------------------------------------------------------------

  @Test
  void register_validDefinitionGiven_persistsToStoreAndCachesVersionedKey() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");

    // Act
    registry.register(def);

    // Assert — store.registerDefinition called, versioned key cached in memory
    verify(store).registerDefinition(def);
    assertThat(registry.resolve("transfer", "1.0")).isSameAs(def);
  }

  @Test
  void register_storeWriteFails_definitionNotInMemory() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");
    doThrow(SagaPersistenceException.retryable("store error", new RuntimeException("db down")))
        .when(store)
        .registerDefinition(any());
    when(store.getDefinition("transfer", "1.0")).thenReturn(Optional.empty());

    // Act & Assert — register fails
    assertThatThrownBy(() -> registry.register(def)).isInstanceOf(SagaPersistenceException.class);
    // Definition should NOT be in memory (persist before memory).
    // resolve() falls back to store, which also returns empty since registration failed.
    assertThat(registry.resolve("transfer", "1.0")).isNull();
  }

  @Test
  void register_multipleVersions_resolvesEachCorrectly() {
    // Arrange
    SagaDefinition v1 = definition("transfer", "1.0");
    SagaDefinition v2 = definition("transfer", "2.0");

    // Act
    registry.register(v1);
    registry.register(v2);

    // Assert — resolve() returns the exact version
    assertThat(registry.resolve("transfer", "1.0")).isSameAs(v1);
    assertThat(registry.resolve("transfer", "2.0")).isSameAs(v2);
  }

  // ---------------------------------------------------------------------------
  // resolve (by name only — always queries store for latest)
  // ---------------------------------------------------------------------------

  @Test
  void resolve_nameOnlyGiven_definitionInStore_returnsDefinition() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");
    when(store.getDefinition("transfer")).thenReturn(Optional.of(def));

    // Act
    SagaDefinition result = registry.resolve("transfer");

    // Assert
    assertThat(result).isSameAs(def);
    verify(store).getDefinition("transfer");
  }

  @Test
  void resolve_nameOnlyGiven_definitionNotInStore_returnsNull() {
    // Arrange
    when(store.getDefinition("unknown")).thenReturn(Optional.empty());

    // Act & Assert
    assertThat(registry.resolve("unknown")).isNull();
  }

  @Test
  void resolve_nameOnlyGiven_definitionInStore_cachesVersionedKey() {
    // Arrange
    SagaDefinition def = definition("transfer", "1.0");
    when(store.getDefinition("transfer")).thenReturn(Optional.of(def));

    // Act — resolve(name) queries the store and caches the versioned key
    registry.resolve("transfer");

    // Assert — resolve(name, version) finds it in memory without hitting the versioned store lookup
    assertThat(registry.resolve("transfer", "1.0")).isSameAs(def);
    verify(store, never()).getDefinition("transfer", "1.0");
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
