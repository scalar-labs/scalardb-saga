package com.scalar.db.saga.daemon.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HmacUtilsTest {

  @Test
  void hmacSha256Hex_knownVector_matchesExpected() {
    // The canonical HMAC-SHA256 test vector (UTF-8 key "key").
    String hex = HmacUtils.hmacSha256Hex("key", "The quick brown fox jumps over the lazy dog");
    assertThat(hex).isEqualTo("f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8");
  }

  @Test
  void hmacSha256Hex_always_returns64LowercaseHexChars() {
    assertThat(HmacUtils.hmacSha256Hex("secret", "saga-1:debit:1000")).matches("[0-9a-f]{64}");
  }

  @Test
  void hmacSha256Hex_sameInputs_areDeterministic() {
    assertThat(HmacUtils.hmacSha256Hex("s", "d")).isEqualTo(HmacUtils.hmacSha256Hex("s", "d"));
  }

  @Test
  void hmacSha256Hex_differentData_producesDifferentSignature() {
    assertThat(HmacUtils.hmacSha256Hex("s", "a")).isNotEqualTo(HmacUtils.hmacSha256Hex("s", "b"));
  }

  @Test
  void verify_matchingToken_returnsTrue() {
    String token = HmacUtils.hmacSha256Hex("secret", "saga-1:debit:1000");
    assertThat(HmacUtils.verify("secret", "saga-1:debit:1000", token)).isTrue();
  }

  @Test
  void verify_tamperedToken_returnsFalse() {
    String token = HmacUtils.hmacSha256Hex("secret", "saga-1:debit:1000");
    String tampered = (token.charAt(0) == '0' ? '1' : '0') + token.substring(1);
    assertThat(HmacUtils.verify("secret", "saga-1:debit:1000", tampered)).isFalse();
  }

  @Test
  void verify_wrongSecret_returnsFalse() {
    String token = HmacUtils.hmacSha256Hex("secret", "saga-1:debit:1000");
    assertThat(HmacUtils.verify("other-secret", "saga-1:debit:1000", token)).isFalse();
  }

  @Test
  void verify_differentData_returnsFalse() {
    String token = HmacUtils.hmacSha256Hex("secret", "saga-1:debit:1000");
    assertThat(HmacUtils.verify("secret", "saga-1:debit:9999", token)).isFalse();
  }

  @Test
  void verify_differentLengthToken_returnsFalse() {
    String token = HmacUtils.hmacSha256Hex("secret", "saga-1:debit:1000");
    assertThat(HmacUtils.verify("secret", "saga-1:debit:1000", token + "ab")).isFalse();
  }

  @Test
  void callbackSignedData_lengthPrefixesEachVariableField() {
    assertThat(HmacUtils.callbackSignedData("saga-1", "debit", "1000"))
        .isEqualTo("6:saga-1:5:debit:1000");
  }

  @Test
  void callbackSignedData_boundaryAmbiguousTuples_produceDistinctData() {
    // Without length prefixing, (a:b, c) and (a, b:c) both serialize to "a:b:c:1", so a token
    // minted for one saga or step would validate for the other. The prefix keeps them distinct.
    String data1 = HmacUtils.callbackSignedData("a:b", "c", "1");
    String data2 = HmacUtils.callbackSignedData("a", "b:c", "1");
    assertThat(data1).isNotEqualTo(data2);
    assertThat(HmacUtils.hmacSha256Hex("secret", data1))
        .isNotEqualTo(HmacUtils.hmacSha256Hex("secret", data2));
  }

  @Test
  void callbackSignedData_sameTuple_isDeterministic() {
    assertThat(HmacUtils.callbackSignedData("a:b", "c", "1"))
        .isEqualTo(HmacUtils.callbackSignedData("a:b", "c", "1"));
  }
}
