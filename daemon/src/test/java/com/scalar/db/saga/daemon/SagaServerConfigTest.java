package com.scalar.db.saga.daemon;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.scalar.db.saga.engine.RecoveryConfig;
import com.scalar.db.saga.engine.RetentionConfig;
import com.scalar.db.saga.engine.ShutdownMode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SagaServerConfigTest {

  @Test
  void load_noPayloadLimit_appliesDaemonDefault() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.properties().getProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY))
        .isEqualTo(Integer.toString(SagaServerConfig.DEFAULT_MAX_EVENT_PAYLOAD_BYTES));
  }

  @Test
  void load_explicitPayloadLimit_isPreserved() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "42");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.properties().getProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY))
        .isEqualTo("42");
  }

  @Test
  void load_blankPayloadLimit_appliesDaemonDefault() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "   ");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.properties().getProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY))
        .isEqualTo(Integer.toString(SagaServerConfig.DEFAULT_MAX_EVENT_PAYLOAD_BYTES));
  }

  @Test
  void rawProperties_returnUnresolvedInput_withoutStoreDefaults() {
    // Arrange
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    // Assert — properties() applies the daemon store default; rawProperties() is the untouched,
    // pre-resolution input (the boundary the API-key provider checks references against).
    assertThat(config.properties().getProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY))
        .isNotNull();
    assertThat(
            config.rawProperties().getProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY))
        .isNull();
  }

  @Test
  void threadPool_unset_appliesDefaults() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.httpMaxThreads()).isEqualTo(SagaServerConfig.DEFAULT_MAX_THREADS);
    assertThat(config.httpMinThreads()).isEqualTo(SagaServerConfig.DEFAULT_MIN_THREADS);
  }

  @Test
  void insecureModeEnabled_unset_defaultsFalse() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.insecureModeEnabled())
        .isEqualTo(SagaServerConfig.DEFAULT_INSECURE_MODE_ENABLED);
  }

  @Test
  void insecureModeEnabled_setTrue_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.INSECURE_MODE_ENABLED_KEY, "true");

    assertThat(SagaServerConfig.load(props).insecureModeEnabled()).isTrue();
  }

  @Test
  void threadPool_configuredValues_areParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_THREADS_KEY, "50");
    props.setProperty(SagaServerConfig.HTTP_MIN_THREADS_KEY, "4");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.httpMaxThreads()).isEqualTo(50);
    assertThat(config.httpMinThreads()).isEqualTo(4);
  }

  @Test
  void threadPool_minExceedsMax_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_THREADS_KEY, "4");
    props.setProperty(SagaServerConfig.HTTP_MIN_THREADS_KEY, "8");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void threadPool_zeroMaxThreads_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_THREADS_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void maxQueuedRequests_unset_defaultsToMultipleOfMaxThreads() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.httpMaxQueuedRequests())
        .isEqualTo(
            SagaServerConfig.DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD
                * SagaServerConfig.DEFAULT_MAX_THREADS);
  }

  @Test
  void maxQueuedRequests_unset_scalesWithConfiguredMaxThreads() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_THREADS_KEY, "50");

    // The default derives from maxThreads, so the shed point stays proportional to the pool.
    assertThat(SagaServerConfig.load(props).httpMaxQueuedRequests())
        .isEqualTo(SagaServerConfig.DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD * 50);
  }

  @Test
  void maxQueuedRequests_configured_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_QUEUED_REQUESTS_KEY, "500");

    assertThat(SagaServerConfig.load(props).httpMaxQueuedRequests()).isEqualTo(500);
  }

  @Test
  void maxQueuedRequests_zero_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_QUEUED_REQUESTS_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void defaultSagaTimeoutMillis_unset_defaultsToDisabled() {
    assertThat(SagaServerConfig.load(new Properties()).defaultSagaTimeoutMillis()).isZero();
  }

  @Test
  void defaultSagaTimeoutMillis_configured_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.DEFAULT_SAGA_TIMEOUT_MILLIS_KEY, "30000");

    assertThat(SagaServerConfig.load(props).defaultSagaTimeoutMillis()).isEqualTo(30_000L);
  }

  @Test
  void defaultSagaTimeoutMillis_negative_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.DEFAULT_SAGA_TIMEOUT_MILLIS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void maxStartRequestsPerMinute_unset_defaultsToDisabled() {
    assertThat(SagaServerConfig.load(new Properties()).maxStartRequestsPerMinute()).isZero();
  }

  @Test
  void maxStartRequestsPerMinute_configured_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY, "100");

    assertThat(SagaServerConfig.load(props).maxStartRequestsPerMinute()).isEqualTo(100);
  }

  @Test
  void securityProvider_unset_defaultsToNoop() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.securityProvider()).isEqualTo(SagaServerConfig.DEFAULT_SECURITY_PROVIDER);
  }

  @Test
  void securityProvider_setWithMixedCaseAndPadding_isNormalizedToLowerCaseTrimmed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "  JWT  ");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.securityProvider()).isEqualTo("jwt");
  }

  @Test
  void securityProvider_blank_defaultsToNoop() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "   ");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.securityProvider()).isEqualTo(SagaServerConfig.DEFAULT_SECURITY_PROVIDER);
  }

  @Test
  void grpcMaxInboundMessageBytes_returnsConfiguredPayloadLimit() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "12345");

    assertThat(SagaServerConfig.load(props).grpcMaxInboundMessageBytes()).isEqualTo(12345);
  }

  @Test
  void grpcMaxInboundMessageBytes_zeroPayloadLimit_mapsToIntegerMax() {
    // The store reads 0 as "no limit"; gRPC's maxInboundMessageSize(0) would reject every non-empty
    // message, so 0 must map to the effective maximum.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "0");

    assertThat(SagaServerConfig.load(props).grpcMaxInboundMessageBytes())
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void load_negativePayloadLimit_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_nonNumericPayloadLimit_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "not-a-number");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_unsetPort_usesDefault() {
    assertThat(SagaServerConfig.load(new Properties()).httpPort())
        .isEqualTo(SagaServerConfig.DEFAULT_HTTP_PORT);
  }

  @Test
  void load_unsetHost_usesDefault() {
    assertThat(SagaServerConfig.load(new Properties()).host())
        .isEqualTo(SagaServerConfig.DEFAULT_HOST);
  }

  @Test
  void load_hostGiven_isUsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");

    assertThat(SagaServerConfig.load(props).host()).isEqualTo("127.0.0.1");
  }

  @Test
  void load_unsetSyncTimeout_disabledByDefault() {
    assertThat(SagaServerConfig.load(new Properties()).syncTimeoutMillis())
        .isEqualTo(SagaServerConfig.DEFAULT_SYNC_TIMEOUT_MILLIS);
  }

  @Test
  void load_syncTimeoutGiven_parsesValue() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SYNC_TIMEOUT_MILLIS_KEY, "5000");

    assertThat(SagaServerConfig.load(props).syncTimeoutMillis()).isEqualTo(5000L);
  }

  @Test
  void load_negativeSyncTimeout_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SYNC_TIMEOUT_MILLIS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_nonNumericSyncTimeout_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SYNC_TIMEOUT_MILLIS_KEY, "soon");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_unsetCallbackMaxAge_disabledByDefault() {
    assertThat(SagaServerConfig.load(new Properties()).callbackMaxAgeSeconds())
        .isEqualTo(SagaServerConfig.DEFAULT_CALLBACK_MAX_AGE_SECONDS);
  }

  @Test
  void load_callbackMaxAgeGiven_parsesValue() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_MAX_AGE_SECONDS_KEY, "3600");

    assertThat(SagaServerConfig.load(props).callbackMaxAgeSeconds()).isEqualTo(3600L);
  }

  @Test
  void load_negativeCallbackMaxAge_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_MAX_AGE_SECONDS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_unsetSyncMaxWait_defaultsToCeiling() {
    assertThat(SagaServerConfig.load(new Properties()).syncMaxWaitMillis())
        .isEqualTo(SagaServerConfig.DEFAULT_SYNC_MAX_WAIT_MILLIS);
  }

  @Test
  void load_syncMaxWaitGiven_parsesValue() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, "30000");

    assertThat(SagaServerConfig.load(props).syncMaxWaitMillis()).isEqualTo(30000L);
  }

  @Test
  void load_zeroSyncMaxWait_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_negativeSyncMaxWait_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_nonNumericSyncMaxWait_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, "soon");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_noServiceKeys_returnsEmptyServices() {
    assertThat(SagaServerConfig.load(new Properties()).services()).isEmpty();
  }

  @Test
  void load_singleServiceBaseUrlGiven_parsesService() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.services())
        .containsExactly(
            entry(
                "account",
                new SagaServerConfig.ServiceConfig(
                    "http://account-svc:8080", List.of(), 0L, Map.of())));
  }

  @Test
  void load_multipleServiceBaseUrlsGiven_parsesAll() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(serviceKey("ledger", ".base_url"), "http://ledger-svc:9000");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.services()).containsOnlyKeys("account", "ledger");
    assertThat(requireNonNull(config.services().get("ledger")).baseUrl())
        .isEqualTo("http://ledger-svc:9000");
  }

  @Test
  void load_blankServiceBaseUrlGiven_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "   ");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_withFullServicePolicy_parsesEveryAttribute() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(serviceKey("account", ".allowed_hosts"), "account-svc, account-svc.internal");
    props.setProperty(serviceKey("account", ".max_body_bytes"), "2000000");
    props.setProperty(serviceKey("account", ".header.Authorization"), "Bearer token");
    props.setProperty(serviceKey("account", ".header.X-Tenant"), "acme");

    SagaServerConfig.ServiceConfig service =
        requireNonNull(SagaServerConfig.load(props).services().get("account"));

    assertThat(service.baseUrl()).isEqualTo("http://account-svc:8080");
    assertThat(service.allowedHosts()).containsExactly("account-svc", "account-svc.internal");
    assertThat(service.maxBodyBytes()).isEqualTo(2_000_000L);
    assertThat(service.headers())
        .containsOnly(entry("Authorization", "Bearer token"), entry("X-Tenant", "acme"));
  }

  @Test
  void load_serviceHeaderSecretReference_isResolved(@TempDir Path dir) throws IOException {
    // The header value is how a daemon authenticates to a downstream service, so it must accept a
    // secret reference rather than force the credential inline in the properties file.
    Path secret = dir.resolve("downstream.token");
    Files.writeString(secret, "Bearer resolved-token", StandardCharsets.UTF_8);
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(
        serviceKey("account", ".header.Authorization"), "${file:UTF-8:" + secret + "}");

    SagaServerConfig.ServiceConfig service =
        requireNonNull(SagaServerConfig.load(props).services().get("account"));

    assertThat(service.headers()).containsExactly(entry("Authorization", "Bearer resolved-token"));
  }

  @Test
  void load_serviceWithoutBaseUrl_throwsIllegalArgumentException() {
    // A policy without a base URL configures a service no declarative step can call.
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".max_body_bytes"), "1000");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_unknownServiceAttribute_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(serviceKey("account", ".timeout_millis"), "1000");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceNameContainingDot_throwsIllegalArgumentException() {
    // The name is split at the first '.', so a dotted name would silently become a different
    // service with an unknown attribute; it is rejected rather than half-parsed.
    Properties props = new Properties();
    props.setProperty(serviceKey("account.v2", ".base_url"), "http://account-svc:8080");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceKeyWithoutAttribute_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SERVICE_KEY_PREFIX + "account", "http://account-svc:8080");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceHeaderWithoutName_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(serviceKey("account", ".header."), "value");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceHeaderNamedLikeCorrelationHeader_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    // The engine stamps this on every outbound request, so a configured value never reaches the
    // participant. Reject it instead of accepting a key that silently does nothing.
    props.setProperty(serviceKey("account", ".header.X-Saga-Id"), "spoofed");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceHeaderNamedLikeCorrelationHeaderInAnotherCase_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    // Header names are case-insensitive, so a lower-cased spelling must be rejected too — it would
    // collide with the engine's header just the same.
    props.setProperty(serviceKey("account", ".header.x-saga-step"), "spoofed");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceHeaderNamedLikeCallbackUrlHeader_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    // Injected per call for async steps; a configured value would leak to non-async steps as a
    // callback URL the engine never issued.
    props.setProperty(serviceKey("account", ".header.X-Saga-Callback-Url"), "http://evil/cb");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceHeadersDifferingOnlyInCase_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    // Header names are case-insensitive, so these two collapse to one header downstream and which
    // value survives is not deterministic across restarts. Reject the pair instead.
    props.setProperty(serviceKey("account", ".header.Authorization"), "Bearer aaa");
    props.setProperty(serviceKey("account", ".header.authorization"), "Bearer bbb");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_sameHeaderNameOnDifferentServices_isAccepted() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(serviceKey("ledger", ".base_url"), "http://ledger-svc:8080");
    // The duplicate check is per service: two services may each carry their own Authorization.
    props.setProperty(serviceKey("account", ".header.Authorization"), "Bearer account");
    props.setProperty(serviceKey("ledger", ".header.authorization"), "Bearer ledger");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(requireNonNull(config.services().get("account")).headers())
        .containsExactly(entry("Authorization", "Bearer account"));
    assertThat(requireNonNull(config.services().get("ledger")).headers())
        .containsExactly(entry("authorization", "Bearer ledger"));
  }

  @Test
  void load_serviceAllowedHostsWithEmptyElement_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(serviceKey("account", ".allowed_hosts"), "account-svc,,other");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_serviceMaxBodyBytesZero_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(serviceKey("account", ".base_url"), "http://account-svc:8080");
    props.setProperty(serviceKey("account", ".max_body_bytes"), "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static String serviceKey(String name, String attribute) {
    return SagaServerConfig.SERVICE_KEY_PREFIX + name + attribute;
  }

  @Test
  void ownerId_unset_defaultsToRandomUuidPerLoad() {
    // Two instances must not share an owner id: the recovery claim is what stops both from driving
    // the same saga, so the unset default has to be unique per process rather than a fixed string.
    String first = SagaServerConfig.load(new Properties()).ownerId();
    String second = SagaServerConfig.load(new Properties()).ownerId();

    assertThat(first).isNotBlank().isNotEqualTo(second);
  }

  @Test
  void ownerId_configured_isUsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.OWNER_ID_KEY, "  saga-daemon-0  ");

    assertThat(SagaServerConfig.load(props).ownerId()).isEqualTo("saga-daemon-0");
  }

  @Test
  void recoveryConfig_unset_matchesEngineDefaults() {
    RecoveryConfig config = SagaServerConfig.load(new Properties()).recoveryConfig();
    RecoveryConfig defaults = RecoveryConfig.defaults();

    assertThat(config.recoveryTimeoutMillis()).isEqualTo(defaults.recoveryTimeoutMillis());
    assertThat(config.recoveryIntervalSeconds()).isEqualTo(defaults.recoveryIntervalSeconds());
    assertThat(config.compensationGracePeriod()).isEqualTo(defaults.compensationGracePeriod());
    assertThat(config.batchSize()).isEqualTo(defaults.batchSize());
    assertThat(config.maxConcurrentRecoveries()).isEqualTo(defaults.maxConcurrentRecoveries());
  }

  @Test
  void recoveryConfig_withAllOptions_setsAllFields() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_TIMEOUT_MILLIS_KEY, "90000");
    props.setProperty(SagaServerConfig.RECOVERY_INTERVAL_SECONDS_KEY, "15");
    props.setProperty(SagaServerConfig.RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY, "1800");
    props.setProperty(SagaServerConfig.RECOVERY_BATCH_SIZE_KEY, "2000");
    props.setProperty(SagaServerConfig.RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY, "25");

    RecoveryConfig config = SagaServerConfig.load(props).recoveryConfig();

    assertThat(config.recoveryTimeoutMillis()).isEqualTo(90_000L);
    assertThat(config.recoveryIntervalSeconds()).isEqualTo(15L);
    assertThat(config.compensationGracePeriod()).isEqualTo(Duration.ofMinutes(30));
    assertThat(config.batchSize()).isEqualTo(2000);
    assertThat(config.maxConcurrentRecoveries()).isEqualTo(25);
  }

  @Test
  void recoveryConfig_zeroInterval_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_INTERVAL_SECONDS_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void recoveryConfig_negativeTimeout_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_TIMEOUT_MILLIS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void recoveryConfig_nonNumericBatchSize_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_BATCH_SIZE_KEY, "many");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void recoveryConfig_batchSizeAboveIntRange_throwsIllegalArgumentException() {
    // Parsed as a long and range-checked: a bare (int) cast would wrap this to a small or negative
    // batch size instead of rejecting it.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_BATCH_SIZE_KEY, "4294967296");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void retentionConfig_unset_matchesEngineDefaults() {
    RetentionConfig config = SagaServerConfig.load(new Properties()).retentionConfig();
    RetentionConfig defaults = RetentionConfig.defaults();

    assertThat(config.retentionPeriod()).isEqualTo(defaults.retentionPeriod());
    assertThat(config.cleanupIntervalSeconds()).isEqualTo(defaults.cleanupIntervalSeconds());
    assertThat(config.batchSize()).isEqualTo(defaults.batchSize());
    assertThat(config.maxConcurrentPurges()).isEqualTo(defaults.maxConcurrentPurges());
  }

  @Test
  void retentionConfig_withAllOptions_setsAllFields() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RETENTION_PERIOD_SECONDS_KEY, "86400");
    props.setProperty(SagaServerConfig.RETENTION_CLEANUP_INTERVAL_SECONDS_KEY, "120");
    props.setProperty(SagaServerConfig.RETENTION_BATCH_SIZE_KEY, "500");
    props.setProperty(SagaServerConfig.RETENTION_MAX_CONCURRENT_PURGES_KEY, "4");

    RetentionConfig config = SagaServerConfig.load(props).retentionConfig();

    assertThat(config.retentionPeriod()).isEqualTo(Duration.ofDays(1));
    assertThat(config.cleanupIntervalSeconds()).isEqualTo(120L);
    assertThat(config.batchSize()).isEqualTo(500);
    assertThat(config.maxConcurrentPurges()).isEqualTo(4);
  }

  @Test
  void retentionConfig_zeroPeriod_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RETENTION_PERIOD_SECONDS_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void retentionConfig_zeroMaxConcurrentPurges_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RETENTION_MAX_CONCURRENT_PURGES_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shutdown_unset_matchesEngineDefaults() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.shutdownMode()).isEqualTo(SagaServerConfig.DEFAULT_SHUTDOWN_MODE);
    assertThat(config.shutdownTimeoutMillis())
        .isEqualTo(SagaServerConfig.DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
  }

  @Test
  void shutdown_configured_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SHUTDOWN_MODE_KEY, "  wait_all_sagas  ");
    props.setProperty(SagaServerConfig.SHUTDOWN_TIMEOUT_MILLIS_KEY, "120000");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.shutdownMode()).isEqualTo(ShutdownMode.WAIT_ALL_SAGAS);
    assertThat(config.shutdownTimeoutMillis()).isEqualTo(120_000L);
  }

  @Test
  void shutdownMode_unknownName_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SHUTDOWN_MODE_KEY, "immediate");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shutdownTimeoutMillis_zero_isAccepted() {
    Properties props = new Properties();
    // 0 is a drain of nothing, not a disabled setting: the engine accepts it and cancels in-flight
    // work at once, so the daemon must be able to express it too.
    props.setProperty(SagaServerConfig.SHUTDOWN_TIMEOUT_MILLIS_KEY, "0");

    assertThat(SagaServerConfig.load(props).shutdownTimeoutMillis()).isZero();
  }

  @Test
  void shutdownTimeoutMillis_negative_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SHUTDOWN_TIMEOUT_MILLIS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void grpcMaxInboundMetadataBytes_unset_usesDefault() {
    assertThat(SagaServerConfig.load(new Properties()).grpcMaxInboundMetadataBytes())
        .isEqualTo(SagaServerConfig.DEFAULT_GRPC_MAX_INBOUND_METADATA_BYTES);
  }

  @Test
  void grpcMaxInboundMetadataBytes_configured_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.GRPC_MAX_INBOUND_METADATA_BYTES_KEY, "32768");

    assertThat(SagaServerConfig.load(props).grpcMaxInboundMetadataBytes()).isEqualTo(32_768);
  }

  @Test
  void grpcMaxInboundMetadataBytes_zero_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.GRPC_MAX_INBOUND_METADATA_BYTES_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_bothTransportsOnSamePort_throwsIllegalArgumentException() {
    // Each transport binds its own listener, so a shared fixed port fails at bind time with a
    // "port in use" that names neither key.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "8080");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "8080");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_bothTransportsOnEphemeralPort_isAccepted() {
    // 0 means "bind any free port", so both transports asking for it cannot collide.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.httpPort()).isZero();
    assertThat(config.grpcPort()).isZero();
  }

  @Test
  void load_samePortWithOneTransportDisabled_isAccepted() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "50051");

    assertThat(SagaServerConfig.load(props).httpPort()).isEqualTo(50_051);
  }

  @Test
  void load_callbackBaseUrlWithoutSecret_throwsIllegalArgumentException() {
    // Half-configured async completion: a URL is handed out that no callback can authenticate
    // against, which would surface only when the first async saga runs.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_callbackSecretWithoutBaseUrl_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_bothCallbackKeys_arePresent() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080");
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.callbackBaseUrl()).contains("http://daemon:8080");
    assertThat(config.callbackSecret()).contains("s3cr3t-key");
  }

  @Test
  void load_unknownServerKey_throwsIllegalArgumentException() {
    // A misspelled key is otherwise indistinguishable from an unset one: the daemon would serve
    // traffic under the default while the operator believes the setting took effect.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SERVER_PREFIX + "max_treads", "50");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_unknownKeyOutsideServerNamespace_isForwarded() {
    // Only the daemon's own namespace is checked; ScalarDB and store keys pass through untouched.
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty("scalar.db.saga.store.num_buckets", "4");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.properties().getProperty("scalar.db.storage")).isEqualTo("jdbc");
    assertThat(config.properties().getProperty("scalar.db.saga.store.num_buckets")).isEqualTo("4");
  }

  @Test
  void load_securityProviderNamespaceKey_isDelegatedNotRejected() {
    // The jwt and apikey namespaces are validated by the provider configs that parse them, so the
    // unknown-key check must not reject a key it does not itself know.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_JWT_PREFIX + "issuer", "https://issuer.example");
    props.setProperty(SagaServerConfig.SECURITY_APIKEY_PREFIX + "key.svc.roles", "saga:read");

    assertThat(SagaServerConfig.load(props).securityProvider())
        .isEqualTo(SagaServerConfig.DEFAULT_SECURITY_PROVIDER);
  }

  @Test
  void load_unknownKeyInDefaults_throwsIllegalArgumentException() {
    // The check runs over the flattened table, so a key inherited from a defaults chain is caught
    // like any other.
    Properties defaults = new Properties();
    defaults.setProperty(SagaServerConfig.SERVER_PREFIX + "grpc.prot", "50051");
    Properties props = new Properties(defaults);

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void maxThreads_aboveIntRange_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_THREADS_KEY, "2147483648");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void maxQueuedRequests_unsetWithHugeMaxThreads_clampsToIntRange() {
    // The default is a multiple of maxThreads, so it has to be computed wide and clamped rather
    // than overflowing into a negative queue cap.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MAX_THREADS_KEY, Integer.toString(Integer.MAX_VALUE));

    assertThat(SagaServerConfig.load(props).httpMaxQueuedRequests()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void load_unsetTransportToggles_bothEnabledByDefault() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.httpEnabled()).isTrue();
    assertThat(config.grpcEnabled()).isTrue();
  }

  @Test
  void load_httpDisabled_leavesGrpcEnabled() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_ENABLED_KEY, "false");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.httpEnabled()).isFalse();
    assertThat(config.grpcEnabled()).isTrue();
  }

  @Test
  void load_grpcDisabled_leavesHttpEnabled() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.grpcEnabled()).isFalse();
    assertThat(config.httpEnabled()).isTrue();
  }

  @Test
  void load_transportToggleCaseInsensitive_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "FALSE");

    assertThat(SagaServerConfig.load(props).grpcEnabled()).isFalse();
  }

  @Test
  void load_bothTransportsDisabled_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_nonBooleanTransportToggle_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_ENABLED_KEY, "yes");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_sagaNamespaceSecretReference_isResolved(@TempDir Path dir) throws IOException {
    // A scalar.db.saga.* value may carry a secret reference; here host is resolved from a file.
    Path secret = dir.resolve("host");
    Files.writeString(secret, "10.1.2.3", StandardCharsets.UTF_8);
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "${file:UTF-8:" + secret + "}");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.host()).isEqualTo("10.1.2.3");
  }

  @Test
  void load_scalarDbNamespaceSecretReference_isLeftForScalarDb() {
    // A scalar.db.* store key is NOT resolved by the daemon (ScalarDB resolves it). If the daemon
    // wrongly resolved it, the missing-file reference would throw; instead it is preserved
    // verbatim.
    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", "${file:UTF-8:/does/not/exist}");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.properties().getProperty("scalar.db.contact_points"))
        .isEqualTo("${file:UTF-8:/does/not/exist}");
  }

  @Test
  void load_unsetCallbackSecret_isEmpty() {
    assertThat(SagaServerConfig.load(new Properties()).callbackSecret()).isEmpty();
  }

  @Test
  void load_blankCallbackSecret_isEmpty() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "   ");

    assertThat(SagaServerConfig.load(props).callbackSecret()).isEmpty();
  }

  @Test
  void load_callbackSecretGiven_isPresent() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080");
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");

    assertThat(SagaServerConfig.load(props).callbackSecret()).contains("s3cr3t-key");
  }

  @Test
  void load_unsetCallbackBaseUrl_isEmpty() {
    assertThat(SagaServerConfig.load(new Properties()).callbackBaseUrl()).isEmpty();
  }

  @Test
  void load_callbackBaseUrlGiven_isPresent() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080");

    assertThat(SagaServerConfig.load(props).callbackBaseUrl()).contains("http://daemon:8080");
  }

  @Test
  void load_callbackBaseUrlTrailingSlash_isStripped() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080/");

    assertThat(SagaServerConfig.load(props).callbackBaseUrl()).contains("http://daemon:8080");
  }

  @Test
  void load_callbackBaseUrlSlashesOnly_isEmpty() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "/");

    // Stripping the trailing slash leaves nothing meaningful, so it is treated as unset rather than
    // producing an empty base URL that would yield relative callback URLs.
    assertThat(SagaServerConfig.load(props).callbackBaseUrl()).isEmpty();
  }

  @Test
  void load_callbackSecretFileReference_isResolved(@TempDir Path dir) throws IOException {
    Path secretFile = dir.resolve("callback.secret");
    Files.writeString(secretFile, "resolved-secret-value", StandardCharsets.UTF_8);
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080");
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "${file:UTF-8:" + secretFile + "}");

    assertThat(SagaServerConfig.load(props).callbackSecret()).contains("resolved-secret-value");
  }

  @Test
  void load_nonStringPropertyEntry_isPreserved() {
    // A programmatically populated Properties may hold non-string entries (Properties extends
    // Hashtable). Since the same object is forwarded to ScalarDB as the store config,
    // resolveSecrets
    // must copy such entries through rather than silently drop them (stringPropertyNames() omits
    // them).
    Object nonStringValue = 12345;
    Properties props = new Properties();
    props.put("scalar.db.some.numeric_setting", nonStringValue);

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.properties().get("scalar.db.some.numeric_setting")).isEqualTo(nonStringValue);
  }

  @Test
  void load_nonSagaPropertyInDefaults_isPreserved() {
    // A caller may pass Properties backed by defaults (new Properties(defaults)).
    // stringPropertyNames() lists defaulted keys, but putAll/copyOf would not copy them —
    // resolveSecrets must still carry non-saga store keys through, or ScalarDB loses its connection
    // settings at startup.
    Properties defaults = new Properties();
    defaults.setProperty("scalar.db.contact_points", "cassandra-host");
    Properties props = new Properties(defaults);

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.properties().getProperty("scalar.db.contact_points"))
        .isEqualTo("cassandra-host");
  }

  @Test
  void load_sagaSecretReferenceInDefaults_isResolved(@TempDir Path dir) throws IOException {
    // A scalar.db.saga.* secret reference inherited from the defaults chain must still be resolved,
    // not just carried through verbatim.
    Path secret = dir.resolve("host");
    Files.writeString(secret, "10.9.8.7", StandardCharsets.UTF_8);
    Properties defaults = new Properties();
    defaults.setProperty(SagaServerConfig.HOST_KEY, "${file:UTF-8:" + secret + "}");
    Properties props = new Properties(defaults);

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.host()).isEqualTo("10.9.8.7");
  }

  @Test
  void rawProperties_sagaKeyInDefaults_isPreserved() {
    // rawProperties() is the pre-resolution copy a provider consults to tell a ${...} secret
    // reference from an inline value. It is copied from the original input (before resolveSecrets),
    // so copyOf must flatten the defaults chain too — otherwise a key set only in defaults is
    // dropped from the raw copy while resolveSecrets keeps it in properties(), and a provider that
    // discovers the key via properties() then reads null from rawProperties() (e.g. the API-key
    // provider falsely rejecting a validly-referenced secret).
    Properties defaults = new Properties();
    defaults.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "${env:UNSET_NO_SUCH_VAR}");
    Properties props = new Properties(defaults);

    SagaServerConfig config = SagaServerConfig.load(props);

    // The raw copy keeps the inherited key verbatim (unresolved), matching the resolved-side
    // flatten.
    assertThat(config.rawProperties().getProperty(SagaServerConfig.SECURITY_PROVIDER_KEY))
        .isEqualTo("${env:UNSET_NO_SUCH_VAR}");
  }
}
