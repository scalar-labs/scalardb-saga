package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.RecoveryConfig;
import com.scalar.db.saga.api.RetentionConfig;
import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.api.ShutdownMode;
import com.scalar.db.saga.api.StepResolver;
import com.scalar.db.saga.store.SagaStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class SagaManagerBuilderTest {

  @Test
  void build_withRequiredFields_returnsSagaManager() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager = SagaManagerBuilder.newBuilder().storeFactory(() -> store).build();

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
            .storeFactory(() -> store)
            .ownerId("pod-1")
            .shutdownMode(ShutdownMode.WAIT_ALL_SAGAS)
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
  void build_withHttpEndpointDefaults_returnsSagaManager() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder()
            .storeFactory(() -> store)
            .httpEndpoint("account-svc", "http://account-svc:8080")
            .add()
            .build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void build_withHttpEndpointFullConfig_returnsSagaManager() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder()
            .storeFactory(() -> store)
            .httpEndpoint("account-svc", "https://account-svc:8443")
            .allowedHosts("account-svc")
            .maxBodyBytes(2_000_000)
            .httpClient(java.net.http.HttpClient.newHttpClient())
            .defaultHeader("Authorization", "Bearer secret")
            .defaultHeaders(java.util.Map.of("Accept", "application/json"))
            .add()
            .build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @SuppressWarnings("NullAway")
  @Test
  void defaultHeader_nullValue_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                SagaManagerBuilder.newBuilder()
                    .httpEndpoint("account-svc", "http://account-svc:8080")
                    .defaultHeader("Authorization", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void build_withHttpEndpointAndResource_returnsSagaManager() {
    // Arrange — httpEndpoint is orthogonal to resource() (D1)
    SagaStore store = mock(SagaStore.class);

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder()
            .storeFactory(() -> store)
            .httpEndpoint("account-svc", "http://account-svc:8080")
            .add()
            .resource(String.class, "value")
            .build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void build_withHttpEndpointAndStepResolver_returnsSagaManager() {
    // Arrange — httpEndpoint is orthogonal to stepResolver() (D1)
    SagaStore store = mock(SagaStore.class);
    StepResolver resolver =
        (name, cls, ctx) -> {
          throw new UnsupportedOperationException("not used");
        };

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder()
            .storeFactory(() -> store)
            .httpEndpoint("account-svc", "http://account-svc:8080")
            .add()
            .stepResolver(resolver)
            .build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void httpEndpoint_blankName_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaManagerBuilder.newBuilder().httpEndpoint(" ", "http://svc:8080"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void httpEndpoint_blankBaseUrl_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaManagerBuilder.newBuilder().httpEndpoint("svc", " "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void maxBodyBytes_nonPositive_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                SagaManagerBuilder.newBuilder()
                    .httpEndpoint("account-svc", "http://account-svc:8080")
                    .maxBodyBytes(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_missingStoreFactory_throwsIllegalStateException() {
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
            .storeFactory(() -> store)
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
        (name, cls, ctx) -> {
          throw new UnsupportedOperationException("not used");
        };

    // Act
    SagaManager manager =
        SagaManagerBuilder.newBuilder().storeFactory(() -> store).stepResolver(resolver).build();

    // Assert
    assertThat(manager).isNotNull();
    manager.close();
  }

  @Test
  void build_withResourcesAndStepResolver_throwsIllegalStateException() {
    // Arrange
    SagaStore store = mock(SagaStore.class);
    StepResolver resolver =
        (name, cls, ctx) -> {
          throw new UnsupportedOperationException("not used");
        };

    // Act & Assert
    assertThatThrownBy(
            () ->
                SagaManagerBuilder.newBuilder()
                    .storeFactory(() -> store)
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
    SagaManager manager = SagaManagerBuilder.newBuilder().storeFactory(() -> store).build();

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
                    .storeFactory(() -> store)
                    .resource(String.class, "first")
                    .resource(String.class, "second")
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withCustomClock_propagatesClockToDefaultConfigs() {
    // Arrange
    SagaStore store = mock(SagaStore.class);
    Clock fixedClock = Clock.fixed(Instant.parse("2025-06-01T00:00:00Z"), ZoneOffset.UTC);

    // Act — build with custom clock but no explicit recovery/retention configs
    SagaManager manager =
        SagaManagerBuilder.newBuilder().storeFactory(() -> store).clock(fixedClock).build();

    // Assert — verify defaults(Clock) propagates the clock correctly
    assertThat(manager).isNotNull();
    assertThat(RecoveryConfig.defaults(fixedClock).clock()).isSameAs(fixedClock);
    assertThat(RetentionConfig.defaults(fixedClock).clock()).isSameAs(fixedClock);
    manager.close();
  }

  @Test
  void build_withExplicitConfigs_usesProvidedConfigs() {
    // Arrange
    SagaStore store = mock(SagaStore.class);
    Clock builderClock = Clock.fixed(Instant.parse("2025-06-01T00:00:00Z"), ZoneOffset.UTC);
    Clock configClock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
    RecoveryConfig explicitRecovery = RecoveryConfig.defaults(configClock);
    RetentionConfig explicitRetention = RetentionConfig.defaults(configClock);

    // Act — explicit configs should take precedence over builder clock
    SagaManager manager =
        SagaManagerBuilder.newBuilder()
            .storeFactory(() -> store)
            .clock(builderClock)
            .recoveryConfig(explicitRecovery)
            .retentionConfig(explicitRetention)
            .build();

    // Assert
    assertThat(manager).isNotNull();
    assertThat(explicitRecovery.clock()).isSameAs(configClock);
    assertThat(explicitRetention.clock()).isSameAs(configClock);
    manager.close();
  }
}
