package com.scalar.db.saga.invoker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import org.junit.jupiter.api.Test;

class OutboundHttpPolicyTest {

  @Test
  void allowAll_anyHost_isAllowed() {
    OutboundHttpPolicy policy = OutboundHttpPolicy.allowAll();

    assertThat(policy.isAllowed(URI.create("http://anything.internal/x"))).isTrue();
    assertThat(policy.maxBodyBytes()).isEqualTo(OutboundHttpPolicy.DEFAULT_MAX_BODY_BYTES);
  }

  @Test
  void isAllowed_withAllowlist_permitsOnlyListedHosts() {
    OutboundHttpPolicy policy =
        OutboundHttpPolicy.newBuilder().allowedHosts("payment.internal", "ledger.internal").build();

    assertThat(policy.isAllowed(URI.create("http://payment.internal:8080/debit"))).isTrue();
    assertThat(policy.isAllowed(URI.create("http://evil.example.com/probe"))).isFalse();
  }

  @Test
  void isAllowed_hostMatchIsCaseInsensitive() {
    OutboundHttpPolicy policy =
        OutboundHttpPolicy.newBuilder().allowedHosts("Payment.Internal").build();

    assertThat(policy.isAllowed(URI.create("http://payment.internal/x"))).isTrue();
  }

  @Test
  void maxBodyBytes_customGiven_overridesDefault() {
    OutboundHttpPolicy policy = OutboundHttpPolicy.newBuilder().maxBodyBytes(2048).build();

    assertThat(policy.maxBodyBytes()).isEqualTo(2048);
  }

  @Test
  void maxBodyBytes_nonPositiveGiven_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> OutboundHttpPolicy.newBuilder().maxBodyBytes(0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
