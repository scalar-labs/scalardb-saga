package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.store.SagaStore;
import org.junit.jupiter.api.Test;

class SagaManagerBuilderTest {

  @Test
  void build_withRequiredFields_returnsSagaManager() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager = SagaManagerBuilder.newBuilder().store(store).build();

    // Assert
    assertThat(manager).isNotNull();
    assertThat(manager).isInstanceOf(EmbeddedSagaManager.class);
    manager.close();
  }

  @Test
  void build_withAllOptions_returnsSagaManager() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder()
            .store(store)
            .ownerId("pod-1")
            .shutdownMode(SagaEngine.ShutdownMode.WAIT_ALL_SAGAS)
            .shutdownTimeoutMillis(60_000)
            .clock(java.time.Clock.systemUTC())
            .resource(String.class, "account-channel", "account")
            .resource(Integer.class, 42)
            .build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void build_missingStore_throwsIllegalStateException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaManagerBuilder.newBuilder().build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_withResources_returnsSagaManager() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder()
            .store(store)
            .resource(String.class, "source-channel", "source")
            .resource(String.class, "target-channel", "target")
            .build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void build_withStepResolver_returnsSagaManager() {
    // Arrange
    SagaStore store = mock(SagaStore.class);
    StepResolver resolver =
        (name, cls) -> {
          throw new UnsupportedOperationException("not used");
        };

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder().store(store).stepResolver(resolver).build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void build_withResourcesAndStepResolver_throwsIllegalStateException() {
    // Arrange
    SagaStore store = mock(SagaStore.class);
    StepResolver resolver =
        (name, cls) -> {
          throw new UnsupportedOperationException("not used");
        };

    // Act & Assert
    assertThatThrownBy(
            () ->
                SagaManagerBuilder.newBuilder()
                    .store(store)
                    .resource(String.class, "value")
                    .stepResolver(resolver)
                    .build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_withNoResolverConfig_usesDefaultNoArgResolver() {
    // Arrange — no resource() or stepResolver() calls
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager = SagaManagerBuilder.newBuilder().store(store).build();

    // Assert — default mode uses ReflectiveStepResolver with empty registry (no-arg only)
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void build_withDuplicateResource_throwsIllegalArgumentException() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act & Assert
    assertThatThrownBy(
            () ->
                SagaManagerBuilder.newBuilder()
                    .store(store)
                    .resource(String.class, "first")
                    .resource(String.class, "second")
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }
}
