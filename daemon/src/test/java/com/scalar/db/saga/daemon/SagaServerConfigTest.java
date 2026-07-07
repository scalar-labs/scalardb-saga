package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
  void grpcMaxInboundMessageBytes_negativePayloadLimit_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "-1");
    SagaServerConfig config = SagaServerConfig.load(props);

    assertThatThrownBy(config::grpcMaxInboundMessageBytes)
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void grpcMaxInboundMessageBytes_nonNumericPayloadLimit_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "not-a-number");
    SagaServerConfig config = SagaServerConfig.load(props);

    assertThatThrownBy(config::grpcMaxInboundMessageBytes)
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_unsetPort_usesDefault() {
    assertThat(SagaServerConfig.load(new Properties()).port())
        .isEqualTo(SagaServerConfig.DEFAULT_PORT);
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
  void load_noServiceKeys_returnsEmptyServiceBaseUrls() {
    assertThat(SagaServerConfig.load(new Properties()).serviceBaseUrls()).isEmpty();
  }

  @Test
  void load_singleServiceBaseUrlGiven_parsesService() {
    Properties props = new Properties();
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + "account" + SagaServerConfig.SERVICE_BASE_URL_SUFFIX,
        "http://account-svc:8080");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.serviceBaseUrls())
        .containsExactly(entry("account", "http://account-svc:8080"));
  }

  @Test
  void load_multipleServiceBaseUrlsGiven_parsesAll() {
    Properties props = new Properties();
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + "account" + SagaServerConfig.SERVICE_BASE_URL_SUFFIX,
        "http://account-svc:8080");
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + "ledger" + SagaServerConfig.SERVICE_BASE_URL_SUFFIX,
        "http://ledger-svc:9000");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.serviceBaseUrls())
        .containsOnly(
            entry("account", "http://account-svc:8080"), entry("ledger", "http://ledger-svc:9000"));
  }

  @Test
  void load_blankServiceBaseUrlGiven_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + "account" + SagaServerConfig.SERVICE_BASE_URL_SUFFIX,
        "   ");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
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
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080");

    assertThat(SagaServerConfig.load(props).callbackBaseUrl()).contains("http://daemon:8080");
  }

  @Test
  void load_callbackBaseUrlTrailingSlash_isStripped() {
    Properties props = new Properties();
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
}
