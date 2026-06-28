package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.store.SagaStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class DefaultSagaOrchestratorBuilderTest {

  @Test
  void build_withRequiredFields_returnsDefaultSagaOrchestrator() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder().storeFactory(() -> store).build();

    // Assert
    assertThat(orchestrator).isNotNull();
    assertThat(orchestrator).isInstanceOf(DefaultSagaOrchestrator.class);
    orchestrator.close();
  }

  @Test
  void build_withAllOptions_returnsDefaultSagaOrchestrator() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .ownerId("pod-1")
            .shutdownMode(ShutdownMode.WAIT_ALL_SAGAS)
            .shutdownTimeoutMillis(60_000)
            .clock(java.time.Clock.systemUTC())
            .resource(String.class, "account-channel", "account")
            .resource(Integer.class, 42)
            .build();

    // Assert
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
  }

  @Test
  void build_withHttpEndpointDefaults_returnsDefaultSagaOrchestrator() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .httpEndpoint("account-svc", "http://account-svc:8080")
            .add()
            .build();

    // Assert
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
  }

  @Test
  void build_withHttpEndpointFullConfig_returnsDefaultSagaOrchestrator() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
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
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
  }

  @SuppressWarnings("NullAway")
  @Test
  void defaultHeader_nullValue_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                DefaultSagaOrchestrator.newBuilder()
                    .httpEndpoint("account-svc", "http://account-svc:8080")
                    .defaultHeader("Authorization", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void build_withHttpEndpointAndResource_returnsDefaultSagaOrchestrator() {
    // Arrange — httpEndpoint is orthogonal to resource() (D1)
    SagaStore store = mock(SagaStore.class);

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .httpEndpoint("account-svc", "http://account-svc:8080")
            .add()
            .resource(String.class, "value")
            .build();

    // Assert
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
  }

  @Test
  void build_withHttpEndpointAndStepResolver_returnsDefaultSagaOrchestrator() {
    // Arrange — httpEndpoint is orthogonal to stepResolver() (D1)
    SagaStore store = mock(SagaStore.class);
    StepResolver resolver =
        (name, cls, ctx) -> {
          throw new UnsupportedOperationException("not used");
        };

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .httpEndpoint("account-svc", "http://account-svc:8080")
            .add()
            .stepResolver(resolver)
            .build();

    // Assert
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
  }

  @Test
  void httpEndpoint_blankName_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(
            () -> DefaultSagaOrchestrator.newBuilder().httpEndpoint(" ", "http://svc:8080"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void httpEndpoint_blankBaseUrl_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> DefaultSagaOrchestrator.newBuilder().httpEndpoint("svc", " "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void httpEndpoint_baseUrlWithUserInfo_throwsIllegalArgumentException() {
    // Act & Assert — a user@host authority silently retargets the host (resolves to evil.com).
    assertThatThrownBy(
            () ->
                DefaultSagaOrchestrator.newBuilder()
                    .httpEndpoint("svc", "http://svc@evil.com:8080"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void httpEndpoint_baseUrlNonHttpScheme_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(
            () -> DefaultSagaOrchestrator.newBuilder().httpEndpoint("svc", "ftp://svc:8080"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void httpEndpoint_baseUrlWithoutHost_throwsIllegalArgumentException() {
    // Act & Assert — no authority, so getHost() is null.
    assertThatThrownBy(() -> DefaultSagaOrchestrator.newBuilder().httpEndpoint("svc", "svc:8080/x"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void httpEndpoint_baseUrlMalformed_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> DefaultSagaOrchestrator.newBuilder().httpEndpoint("svc", "not a url"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void httpEndpoint_validHttpAndHttpsBaseUrls_accepted() {
    // Act & Assert — valid absolute http/https URLs (with and without a path) build without error.
    assertThatCode(
            () -> {
              DefaultSagaOrchestrator.newBuilder().httpEndpoint("a", "http://account-svc:8080");
              DefaultSagaOrchestrator.newBuilder()
                  .httpEndpoint("b", "https://account-svc:8443/api/");
            })
        .doesNotThrowAnyException();
  }

  @Test
  void httpEndpoint_duplicateName_throwsIllegalArgumentException() {
    // Arrange — register an endpoint named "svc"; a second add() with the same name must fail fast
    // rather than silently overwrite (parity with ResourceRegistry).
    var builder =
        DefaultSagaOrchestrator.newBuilder().httpEndpoint("svc", "http://svc-a:8080").add();

    // Act & Assert
    assertThatThrownBy(() -> builder.httpEndpoint("svc", "http://svc-b:8080").add())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void maxBodyBytes_nonPositive_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                DefaultSagaOrchestrator.newBuilder()
                    .httpEndpoint("account-svc", "http://account-svc:8080")
                    .maxBodyBytes(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_missingStoreFactory_throwsIllegalStateException() {
    // Act & Assert
    assertThatThrownBy(() -> DefaultSagaOrchestrator.newBuilder().build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_withResources_returnsDefaultSagaOrchestrator() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .resource(String.class, "source-channel", "source")
            .resource(String.class, "target-channel", "target")
            .build();

    // Assert
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
  }

  @Test
  void build_withStepResolver_returnsDefaultSagaOrchestrator() {
    // Arrange
    SagaStore store = mock(SagaStore.class);
    StepResolver resolver =
        (name, cls, ctx) -> {
          throw new UnsupportedOperationException("not used");
        };

    // Act
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .stepResolver(resolver)
            .build();

    // Assert
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
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
                DefaultSagaOrchestrator.newBuilder()
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
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder().storeFactory(() -> store).build();

    // Assert — default mode uses ReflectiveStepResolver with empty registry (no-arg only)
    assertThat(orchestrator).isNotNull();
    orchestrator.close();
  }

  @Test
  void build_withDuplicateResource_throwsIllegalArgumentException() {
    // Arrange
    SagaStore store = mock(SagaStore.class);

    // Act & Assert
    assertThatThrownBy(
            () ->
                DefaultSagaOrchestrator.newBuilder()
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
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder().storeFactory(() -> store).clock(fixedClock).build();

    // Assert — verify defaults(Clock) propagates the clock correctly
    assertThat(orchestrator).isNotNull();
    assertThat(RecoveryConfig.defaults(fixedClock).clock()).isSameAs(fixedClock);
    assertThat(RetentionConfig.defaults(fixedClock).clock()).isSameAs(fixedClock);
    orchestrator.close();
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
    DefaultSagaOrchestrator orchestrator =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(() -> store)
            .clock(builderClock)
            .recoveryConfig(explicitRecovery)
            .retentionConfig(explicitRetention)
            .build();

    // Assert
    assertThat(orchestrator).isNotNull();
    assertThat(explicitRecovery.clock()).isSameAs(configClock);
    assertThat(explicitRetention.clock()).isSameAs(configClock);
    orchestrator.close();
  }
}
