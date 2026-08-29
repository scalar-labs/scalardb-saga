package com.scalar.db.saga.server;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.engine.RecoveryConfig;
import com.scalar.db.saga.engine.RetentionConfig;
import com.scalar.db.saga.engine.ShutdownMode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  /**
   * The keys whose default leaves a protection off, so the general blank-is-unset rule would turn a
   * templated value that resolved empty into a silently disabled control. Omitting the key remains
   * the way to accept the default; only the empty spelling is refused.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY,
        SagaServerConfig.CALLBACK_MAX_AGE_SECONDS_KEY,
        SagaServerConfig.TLS_ENABLED_KEY
      })
  void load_blankProtectionDisablingKey_throwsIllegalArgumentException(String key) {
    Properties props = new Properties();
    props.setProperty(key, "   ");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key);
  }

  @Test
  void load_protectionDisablingKeysAbsent_stillDefaultToDisabled() {
    // The other half of the rule above: rejecting blank must not turn these into required keys.
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.maxStartRequestsPerMinute())
        .isEqualTo(SagaServerConfig.DEFAULT_MAX_START_REQUESTS_PER_MINUTE);
    assertThat(config.callbackMaxAgeSeconds())
        .isEqualTo(SagaServerConfig.DEFAULT_CALLBACK_MAX_AGE_SECONDS);
    assertThat(config.tlsEnabled()).isEqualTo(SagaServerConfig.DEFAULT_TLS_ENABLED);
  }

  @Test
  void load_unsetTls_disabledWithNoPaths() {
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    assertThat(config.tlsEnabled()).isFalse();
    assertThat(config.tlsCertChainPath()).isEmpty();
    assertThat(config.tlsPrivateKeyPath()).isEmpty();
  }

  @Test
  void load_tlsEnabledWithBothPaths_parsesTrimmedPaths() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, "  /etc/tls/tls.crt  ");
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, "/etc/tls/tls.key");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.tlsEnabled()).isTrue();
    assertThat(config.tlsCertChainPath()).contains(Path.of("/etc/tls/tls.crt"));
    assertThat(config.tlsPrivateKeyPath()).contains(Path.of("/etc/tls/tls.key"));
  }

  @Test
  void load_tlsEnabledWithoutPrivateKeyPath_throwsNamingMissingKey() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, "/etc/tls/tls.crt");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_ENABLED_KEY)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
  }

  @Test
  void load_tlsEnabledWithoutCertChainPath_throwsNamingMissingKey() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, "/etc/tls/tls.key");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_ENABLED_KEY)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY);
  }

  @Test
  void load_tlsEnabledWithoutAnyPath_throwsNamingBothKeys() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
  }

  @Test
  void load_tlsEnabledWithBlankPrivateKeyPath_throwsAsMissingPair() {
    // Blank-is-unset composes with the pairing rule: a blank path reports as the missing half of
    // the pair, exactly as an absent key does.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, "/etc/tls/tls.crt");
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, "   ");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
  }

  @Test
  void load_tlsPathsWithExplicitFalse_ignoredAndDisabled() {
    // The deliberate toggle-off move: material stays mounted and configured, the explicit false
    // switches it off, and the getters hide the ignored paths so nothing downstream can serve
    // them by accident.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, "/etc/tls/tls.crt");
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, "/etc/tls/tls.key");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.tlsEnabled()).isFalse();
    assertThat(config.tlsCertChainPath()).isEmpty();
    assertThat(config.tlsPrivateKeyPath()).isEmpty();
  }

  @Test
  void load_tlsCertChainPathWithoutEnabledKey_throwsForgottenSwitch() {
    // Material without the switch is the forgot-the-switch hole: the operator mounted certificates
    // expecting TLS, and the server would silently serve plaintext.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, "/etc/tls/tls.crt");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining(SagaServerConfig.TLS_ENABLED_KEY);
  }

  @Test
  void load_tlsPrivateKeyPathAloneWithoutEnabledKey_throwsForgottenSwitch() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, "/etc/tls/tls.key");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining(SagaServerConfig.TLS_ENABLED_KEY);
  }

  @Test
  void load_blankTlsPathWithoutEnabledKey_isNoOp() {
    // The other doctrine-composition cell: blank is unset, so a blank path with no tls.enabled is
    // not the forgotten switch — it is a template variable that resolved empty, and the server
    // starts on the plaintext default.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, "");

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.tlsEnabled()).isFalse();
  }

  @Test
  void load_tlsPathWithNulByte_throwsWithoutEchoingValue() {
    // InvalidPathException's own message embeds the raw input, which for a mis-pasted secret
    // reference would be the secret's plaintext; the parser must throw its own redacted message.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, "s3cr3t\0plaintext");
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, "/etc/tls/tls.key");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageNotContaining("s3cr3t")
        .hasNoCause();
  }

  @Test
  void load_secretFileReferenceOnNumericKey_throwsWithoutEchoingSecret(@TempDir Path dir)
      throws IOException {
    // The shipped template puts secret references and numeric keys a few lines apart, so a
    // reference pasted onto the wrong key must fail without writing the resolved plaintext to
    // the log; pod logs are readable far more widely than the secret itself. The message
    // assertions are the behavior under test here: the key locates the bad line, the value
    // stays out, and so does the NumberFormatException cause, whose own message embeds it.
    Path secret = dir.resolve("api.token");
    Files.writeString(secret, "s3cr3t-plaintext", StandardCharsets.UTF_8);
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "${file:UTF-8:" + secret + "}");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.HTTP_PORT_KEY)
        .hasMessageNotContaining("s3cr3t-plaintext")
        .hasNoCause();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        SagaServerConfig.HTTP_PORT_KEY,
        SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY,
        SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY,
        SagaServerConfig.SHUTDOWN_MODE_KEY,
        SagaServerConfig.INSECURE_MODE_ENABLED_KEY
      })
  void load_unparseableValue_throwsNamingKeyWithoutEchoingValue(String key) {
    // One key per parser family (port, bounded long, payload bytes, enum, boolean): every parse
    // error names the key and never echoes the value, which may be a resolved secret.
    Properties props = new Properties();
    props.setProperty(key, "swordfish-like-a-secret");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key)
        .hasMessageNotContaining("swordfish")
        .hasNoCause();
  }

  @Test
  void load_outOfRangeNumericPort_throwsWithoutEchoingValue() {
    // A purely numeric secret parses successfully, so the semantic branches must redact too: the
    // range check used to print the parsed number, which is the resolved value canonicalized.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "48291736");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.HTTP_PORT_KEY)
        .hasMessageContaining("between 0 and 65535")
        .hasMessageNotContaining("48291736");
  }

  @Test
  void load_negativeNumericBoundedValue_throwsWithoutEchoingValue() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY, "-7231946");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY)
        .hasMessageContaining("must be >=")
        .hasMessageNotContaining("7231946");
  }

  @Test
  void load_intOverflowNumericValue_throwsWithoutEchoingValue() {
    // Between int and long range: parses as a long, then fails the int narrowing check.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY, "99999999999");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY)
        .hasMessageContaining("must be <=")
        .hasMessageNotContaining("99999999999");
  }

  @Test
  void load_negativeNumericPayloadBytes_throwsWithoutEchoingValue() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "-424242");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY)
        .hasMessageContaining("must not be negative")
        .hasMessageNotContaining("424242");
  }

  @Test
  void load_definitionsPathGiven_isParsedTrimmed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, "  /etc/saga/definitions  ");

    assertThat(SagaServerConfig.load(props).definitionsPath())
        .contains(Path.of("/etc/saga/definitions"));
  }

  @Test
  void load_pathValueWithNulCharacter_throwsWithoutEchoingValue() {
    // A NUL character makes Path.of throw InvalidPathException, whose message embeds the input —
    // which is a resolved value — so the parse must remap it to the usual key-plus-redaction shape.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, "s3cr3t\0plaintext");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.DEFINITIONS_PATH_KEY)
        .hasMessageNotContaining("s3cr3t")
        .hasNoCause();
  }

  @Test
  void load_collidingPorts_throwsNamingKeysWithoutEchoingValue() {
    // The collision message needs no number: echoing it would confirm that a numeric secret on
    // one port key equals the other key's port.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "18080");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "18080");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.HTTP_PORT_KEY)
        .hasMessageContaining(SagaServerConfig.GRPC_PORT_KEY)
        .hasMessageNotContaining("18080");
  }

  @Test
  void load_minThreadsAboveMaxThreads_throwsNamingKeysWithoutEchoingValue() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_MIN_THREADS_KEY, "9999999");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.HTTP_MIN_THREADS_KEY)
        .hasMessageContaining(SagaServerConfig.HTTP_MAX_THREADS_KEY)
        .hasMessageNotContaining("9999999");
  }

  @Test
  void load_blankMaxBodyBytes_isTreatedAsUnset(@TempDir Path dir) throws IOException {
    // Deliberately on the other side of the line from the two keys above: unset leaves the engine's
    // own cap in place, so a blank value still bounds the body rather than removing a protection.
    Files.writeString(
        dir.resolve("account.properties"), "base_url=http://account-svc:8080\nmax_body_bytes=\n");
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, dir.toString());

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(requireNonNull(config.services().get("account")).maxBodyBytes()).isZero();
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
  void load_unsetGrpcPort_usesDefault() {
    assertThat(SagaServerConfig.load(new Properties()).grpcPort())
        .isEqualTo(SagaServerConfig.DEFAULT_GRPC_PORT);
  }

  @Test
  void defaultPorts_areTheDocumentedValues() {
    // Pinned as literals, not against the constants the parser reads, so that changing a default
    // has to be a deliberate edit here as well. These numbers are published in the image's EXPOSE,
    // the compose and Kubernetes examples, and the shipped server.properties; a silent change would
    // leave those wrong.
    assertThat(SagaServerConfig.DEFAULT_HTTP_PORT).isEqualTo(12_080);
    assertThat(SagaServerConfig.DEFAULT_GRPC_PORT).isEqualTo(12_051);
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
  void load_noServicesPath_returnsEmptyServices() {
    assertThat(SagaServerConfig.load(new Properties()).services()).isEmpty();
  }

  @Test
  void load_servicesPathGiven_loadsServicesFromDirectory(@TempDir Path dir) throws IOException {
    Files.writeString(dir.resolve("account.properties"), "base_url=http://account-svc:8080\n");
    Files.writeString(dir.resolve("ledger.properties"), "base_url=http://ledger-svc:9000\n");
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, dir.toString());

    SagaServerConfig config = SagaServerConfig.load(props);

    assertThat(config.services()).containsOnlyKeys("account", "ledger");
    assertThat(requireNonNull(config.services().get("account")).baseUrl())
        .isEqualTo("http://account-svc:8080");
  }

  @Test
  void load_oldServiceKeyGiven_throwsWithMigrationHint() {
    // The pre-services_path format is the one unknown-key family with a known history, so the
    // rejection names the migration instead of reading as a typo.
    Properties props = new Properties();
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + "account.base_url", "http://account-svc:8080");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("services_path");
  }

  @Test
  void load_reloadKeysUnset_defaultsApply() {
    ReloadConfig reload = SagaServerConfig.load(new Properties()).reloadConfig();

    assertThat(reload.servicesPath()).isNull();
    assertThat(reload.intervalSeconds())
        .isEqualTo(SagaServerConfig.DEFAULT_RELOAD_INTERVAL_SECONDS);
    assertThat(reload.secretsRoot().toString()).isEqualTo(SagaServerConfig.DEFAULT_SECRETS_ROOT);
    assertThat(reload.allowedHostsCeiling()).isEmpty();
  }

  @Test
  void load_reloadKeysGiven_parsesEach(@TempDir Path dir, @TempDir Path secretsRoot) {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.RELOAD_INTERVAL_SECONDS_KEY, "0");
    props.setProperty(SagaServerConfig.SECRETS_ROOT_KEY, secretsRoot.toString());
    props.setProperty(SagaServerConfig.EGRESS_ALLOWED_HOSTS_CEILING_KEY, "a-svc, b-svc");

    ReloadConfig reload = SagaServerConfig.load(props).reloadConfig();

    assertThat(reload.servicesPath()).isEqualTo(dir);
    assertThat(reload.intervalSeconds()).isZero();
    assertThat(reload.secretsRoot()).isEqualTo(secretsRoot);
    assertThat(reload.allowedHostsCeiling()).containsExactly("a-svc", "b-svc");
  }

  @Test
  void load_negativeReloadInterval_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RELOAD_INTERVAL_SECONDS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_ceilingGiven_serviceOutsideItIsRejected(@TempDir Path dir) throws IOException {
    // The end-to-end wiring of the ceiling: the per-case matrix lives in ServiceFileParserTest.
    Files.writeString(
        dir.resolve("account.properties"),
        "base_url=http://account-svc:8080\nallowed_hosts=other-svc\n");
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.EGRESS_ALLOWED_HOSTS_CEILING_KEY, "account-svc");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ceiling");
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
  void ownerId_controlCharactersGiven_throwsWithoutEchoingTheValue() {
    // The owner id is echoed in log lines; a CRLF in it would forge log entries, so it is
    // rejected — and the error must not echo the value it rejects.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.OWNER_ID_KEY, "pod-7\nFORGED LOG LINE");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.OWNER_ID_KEY)
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("FORGED"));
  }

  @Test
  void ownerId_overlongValueGiven_throws() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.OWNER_ID_KEY, "x".repeat(129));

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.OWNER_ID_KEY);
  }

  @Test
  void recoveryConfig_unset_matchesEngineDefaults() {
    RecoveryConfig config = SagaServerConfig.load(new Properties()).recoveryConfig();
    RecoveryConfig defaults = RecoveryConfig.defaults();

    assertThat(config.stalenessThresholdMillis()).isEqualTo(defaults.stalenessThresholdMillis());
    assertThat(config.intervalSeconds()).isEqualTo(defaults.intervalSeconds());
    assertThat(config.compensationGracePeriod()).isEqualTo(defaults.compensationGracePeriod());
    assertThat(config.maxRecoveriesPerPass()).isEqualTo(defaults.maxRecoveriesPerPass());
    assertThat(config.maxConcurrentRecoveries()).isEqualTo(defaults.maxConcurrentRecoveries());
  }

  @Test
  void recoveryConfig_withAllOptions_setsAllFields() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY, "90000");
    props.setProperty(SagaServerConfig.RECOVERY_INTERVAL_SECONDS_KEY, "15");
    props.setProperty(SagaServerConfig.RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY, "1800");
    props.setProperty(SagaServerConfig.RECOVERY_MAX_RECOVERIES_PER_PASS_KEY, "2000");
    props.setProperty(SagaServerConfig.RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY, "25");

    RecoveryConfig config = SagaServerConfig.load(props).recoveryConfig();

    assertThat(config.stalenessThresholdMillis()).isEqualTo(90_000L);
    assertThat(config.intervalSeconds()).isEqualTo(15L);
    assertThat(config.compensationGracePeriod()).isEqualTo(Duration.ofMinutes(30));
    assertThat(config.maxRecoveriesPerPass()).isEqualTo(2000);
    assertThat(config.maxConcurrentRecoveries()).isEqualTo(25);
  }

  @Test
  void recoveryConfig_negativeTimeout_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY, "-1");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void recoveryConfig_nonNumericMaxRecoveriesPerPass_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_MAX_RECOVERIES_PER_PASS_KEY, "many");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void recoveryConfig_maxRecoveriesPerPassAboveIntRange_throwsIllegalArgumentException() {
    // Parsed as a long and range-checked: a bare (int) cast would wrap this to a small or negative
    // batch size instead of rejecting it.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_MAX_RECOVERIES_PER_PASS_KEY, "4294967296");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void retentionConfig_unset_matchesEngineDefaults() {
    RetentionConfig config = SagaServerConfig.load(new Properties()).retentionConfig();
    RetentionConfig defaults = RetentionConfig.defaults();

    assertThat(config.retentionPeriod()).isEqualTo(defaults.retentionPeriod());
    assertThat(config.intervalSeconds()).isEqualTo(defaults.intervalSeconds());
    assertThat(config.maxPurgesPerPass()).isEqualTo(defaults.maxPurgesPerPass());
    assertThat(config.maxConcurrentPurges()).isEqualTo(defaults.maxConcurrentPurges());
  }

  @Test
  void retentionConfig_withAllOptions_setsAllFields() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RETENTION_PERIOD_SECONDS_KEY, "86400");
    props.setProperty(SagaServerConfig.RETENTION_INTERVAL_SECONDS_KEY, "120");
    props.setProperty(SagaServerConfig.RETENTION_MAX_PURGES_PER_PASS_KEY, "500");
    props.setProperty(SagaServerConfig.RETENTION_MAX_CONCURRENT_PURGES_KEY, "4");

    RetentionConfig config = SagaServerConfig.load(props).retentionConfig();

    assertThat(config.retentionPeriod()).isEqualTo(Duration.ofDays(1));
    assertThat(config.intervalSeconds()).isEqualTo(120L);
    assertThat(config.maxPurgesPerPass()).isEqualTo(500);
    assertThat(config.maxConcurrentPurges()).isEqualTo(4);
  }

  /**
   * Pins every recovery and retention bound against the engine's own validation, which rejects
   * anything below 1 on all nine. Both directions of drift are silent: a looser daemon bound lets a
   * value through that the engine then rejects, and a stricter one refuses a value embedded mode
   * accepts, with an ordinary-looking IllegalArgumentException either way. That is not hypothetical
   * — shutdown.timeout_millis shipped requiring 1 while the engine accepted 0, and only a review
   * caught it. A hand audit does not survive the next refactor; this does.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        SagaServerConfig.RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY,
        SagaServerConfig.RECOVERY_INTERVAL_SECONDS_KEY,
        SagaServerConfig.RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY,
        SagaServerConfig.RECOVERY_MAX_RECOVERIES_PER_PASS_KEY,
        SagaServerConfig.RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY,
        SagaServerConfig.RETENTION_PERIOD_SECONDS_KEY,
        SagaServerConfig.RETENTION_INTERVAL_SECONDS_KEY,
        SagaServerConfig.RETENTION_MAX_PURGES_PER_PASS_KEY,
        SagaServerConfig.RETENTION_MAX_CONCURRENT_PURGES_KEY
      })
  void load_zeroRecoveryOrRetentionBound_throwsIllegalArgumentException(String key) {
    Properties props = new Properties();
    props.setProperty(key, "0");

    // Asserting the message is what makes this test pin the daemon's bound rather than the
    // engine's.
    // Two validation paths throw IllegalArgumentException for this input — the bound here and the
    // config record's own constructor — so the type alone cannot tell them apart, and a daemon
    // bound
    // that drifted below the engine's would still throw from the record and keep an isInstanceOf
    // assertion green. Only the daemon names the property key; the record names its field.
    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key);
  }

  @Test
  void load_everyRecoveryAndRetentionBoundAtOne_isAccepted() {
    // The other half of the pin above: rejecting 0 alone would still allow a bound to drift to 2
    // and
    // refuse a value the engine takes. 1 is the smallest the engine accepts on all nine, so setting
    // them together proves no daemon bound sits above it.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY, "1");
    props.setProperty(SagaServerConfig.RECOVERY_INTERVAL_SECONDS_KEY, "1");
    props.setProperty(SagaServerConfig.RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY, "1");
    props.setProperty(SagaServerConfig.RECOVERY_MAX_RECOVERIES_PER_PASS_KEY, "1");
    props.setProperty(SagaServerConfig.RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY, "1");
    props.setProperty(SagaServerConfig.RETENTION_PERIOD_SECONDS_KEY, "1");
    props.setProperty(SagaServerConfig.RETENTION_INTERVAL_SECONDS_KEY, "1");
    props.setProperty(SagaServerConfig.RETENTION_MAX_PURGES_PER_PASS_KEY, "1");
    props.setProperty(SagaServerConfig.RETENTION_MAX_CONCURRENT_PURGES_KEY, "1");

    SagaServerConfig config = SagaServerConfig.load(props);

    RecoveryConfig recovery = config.recoveryConfig();
    assertThat(recovery.stalenessThresholdMillis()).isEqualTo(1L);
    assertThat(recovery.intervalSeconds()).isEqualTo(1L);
    assertThat(recovery.compensationGracePeriod()).isEqualTo(Duration.ofSeconds(1));
    assertThat(recovery.maxRecoveriesPerPass()).isEqualTo(1);
    assertThat(recovery.maxConcurrentRecoveries()).isEqualTo(1);
    RetentionConfig retention = config.retentionConfig();
    assertThat(retention.retentionPeriod()).isEqualTo(Duration.ofSeconds(1));
    assertThat(retention.intervalSeconds()).isEqualTo(1L);
    assertThat(retention.maxPurgesPerPass()).isEqualTo(1);
    assertThat(retention.maxConcurrentPurges()).isEqualTo(1);
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
  void detailMaxTimelineEvents_unset_usesDefault() {
    assertThat(SagaServerConfig.load(new Properties()).detailMaxTimelineEvents())
        .isEqualTo(SagaServerConfig.DEFAULT_DETAIL_MAX_TIMELINE_EVENTS);
  }

  @Test
  void detailMaxTimelineEvents_configured_isParsed() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.DETAIL_MAX_TIMELINE_EVENTS_KEY, "250");

    assertThat(SagaServerConfig.load(props).detailMaxTimelineEvents()).isEqualTo(250);
  }

  @Test
  void detailMaxTimelineEvents_zero_throwsIllegalArgumentException() {
    // 0 would make every detail read return an empty timeline; an operator who wants the endpoint
    // gone should gate it with RBAC instead, so the config rejects the value.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.DETAIL_MAX_TIMELINE_EVENTS_KEY, "0");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void detailMaxTimelineEvents_notANumber_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.DETAIL_MAX_TIMELINE_EVENTS_KEY, "many");

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
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "12051");

    assertThat(SagaServerConfig.load(props).httpPort()).isEqualTo(12_051);
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
  void load_securityProviderKeysGiven_areAccepted() {
    // The provider parses these, but their names are fixed, so the unknown-key check knows them.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_JWT_PREFIX + "issuer", "https://issuer.example");
    props.setProperty(SagaServerConfig.SECURITY_JWT_PREFIX + "principal_claim", "email");
    props.setProperty(SagaServerConfig.SECURITY_APIKEY_HEADER_KEY, "X-Key");
    props.setProperty(SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + "svc.roles", "saga:read");

    assertThat(SagaServerConfig.load(props).securityProvider())
        .isEqualTo(SagaServerConfig.DEFAULT_SECURITY_PROVIDER);
  }

  @Test
  void load_misspelledOptionalJwtKey_throwsIllegalArgumentException() {
    // principal_claim defaults to sub, so the provider cannot tell a typo from an unset key: it
    // would authenticate on a claim the operator did not choose. Nothing parses this namespace at
    // all under another provider, which is the case here.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_JWT_PREFIX + "principal_clam", "email");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_misspelledApiKeyHeaderKey_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_APIKEY_PREFIX + "headers", "X-Key");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_apiKeySettingsGiven_areAccepted() {
    Properties props = new Properties();
    props.setProperty(
        SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + "writer.secret", "${env:WRITER_KEY}");
    props.setProperty(SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + "writer.roles", "saga:write");
    props.setProperty(
        SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + "writer.principal", "writer@example.com");

    assertThat(SagaServerConfig.load(props).securityProvider())
        .isEqualTo(SagaServerConfig.DEFAULT_SECURITY_PROVIDER);
  }

  @Test
  void load_apiKeyNameContainingDotGiven_isAccepted() {
    // The provider derives <name> by stripping the prefix and the suffix, so a dotted name is a
    // legal key id. The setting check has to split at the last dot, not the first.
    Properties props = new Properties();
    props.setProperty(
        SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + "billing.svc.secret", "${env:BILLING_KEY}");

    assertThat(SagaServerConfig.load(props).securityProvider())
        .isEqualTo(SagaServerConfig.DEFAULT_SECURITY_PROVIDER);
  }

  @Test
  void load_unknownApiKeySetting_throwsIllegalArgumentException() {
    // principal is optional, so a typo of it would record the key's own name for audit instead of
    // the principal configured.
    Properties props = new Properties();
    props.setProperty(
        SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + "writer.principle", "writer@example.com");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_apiKeyWithoutSetting_throwsIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + "writer", "${env:WRITER_KEY}");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void load_apiKeyWithBlankName_throwsIllegalArgumentException() {
    // The provider skips a blank name, so the key would configure nothing at all.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.SECURITY_APIKEY_KEY_PREFIX + ".secret", "${env:WRITER_KEY}");

    assertThatThrownBy(() -> SagaServerConfig.load(props))
        .isInstanceOf(IllegalArgumentException.class);
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
