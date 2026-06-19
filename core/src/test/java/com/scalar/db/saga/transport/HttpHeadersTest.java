package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class HttpHeadersTest {

  @Test
  void charsetOf_nullGiven_returnsUtf8() {
    assertThat(HttpHeaders.charsetOf(null)).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_noCharsetParameterGiven_returnsUtf8() {
    assertThat(HttpHeaders.charsetOf("text/plain")).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_explicitCharsetGiven_returnsThatCharset() {
    assertThat(HttpHeaders.charsetOf("text/plain; charset=ISO-8859-1"))
        .isEqualTo(StandardCharsets.ISO_8859_1);
  }

  @Test
  void charsetOf_uppercaseAndQuotedGiven_returnsThatCharset() {
    assertThat(HttpHeaders.charsetOf("text/plain; CHARSET=\"utf-8\""))
        .isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_charsetWithTrailingParameterGiven_returnsThatCharset() {
    assertThat(HttpHeaders.charsetOf("multipart/form-data; charset=US-ASCII; boundary=xyz"))
        .isEqualTo(StandardCharsets.US_ASCII);
  }

  @Test
  void charsetOf_unknownCharsetGiven_returnsUtf8() {
    assertThat(HttpHeaders.charsetOf("text/plain; charset=no-such-charset"))
        .isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_blankCharsetGiven_returnsUtf8() {
    assertThat(HttpHeaders.charsetOf("text/plain; charset=")).isEqualTo(StandardCharsets.UTF_8);
  }
}
