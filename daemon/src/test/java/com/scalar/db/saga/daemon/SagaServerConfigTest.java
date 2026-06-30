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
}
