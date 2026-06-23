package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.util.Properties;
import org.junit.jupiter.api.Test;

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
  void load_unsetPort_usesDefault() {
    assertThat(SagaServerConfig.load(new Properties()).port())
        .isEqualTo(SagaServerConfig.DEFAULT_PORT);
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
}
