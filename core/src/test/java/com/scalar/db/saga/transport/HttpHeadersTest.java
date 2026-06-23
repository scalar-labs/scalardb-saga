package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class HttpHeadersTest {

  @Test
  void charsetOf_nullGiven_returnsUtf8() {
    // Act
    Charset charset = HttpHeaders.charsetOf(null);

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_noCharsetParameterGiven_returnsUtf8() {
    // Act
    Charset charset = HttpHeaders.charsetOf("text/plain");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_explicitCharsetGiven_returnsThatCharset() {
    // Act
    Charset charset = HttpHeaders.charsetOf("text/plain; charset=ISO-8859-1");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.ISO_8859_1);
  }

  @Test
  void charsetOf_uppercaseAndQuotedGiven_returnsThatCharset() {
    // Act
    Charset charset = HttpHeaders.charsetOf("text/plain; CHARSET=\"utf-8\"");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_charsetWithTrailingParameterGiven_returnsThatCharset() {
    // Act
    Charset charset = HttpHeaders.charsetOf("multipart/form-data; charset=US-ASCII; boundary=xyz");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.US_ASCII);
  }

  @Test
  void charsetOf_unknownCharsetGiven_returnsUtf8() {
    // Act
    Charset charset = HttpHeaders.charsetOf("text/plain; charset=no-such-charset");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_blankCharsetGiven_returnsUtf8() {
    // Act
    Charset charset = HttpHeaders.charsetOf("text/plain; charset=");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_whitespaceAroundEqualsGiven_returnsThatCharset() {
    // Act
    Charset charset = HttpHeaders.charsetOf("text/plain; charset = ISO-8859-1");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.ISO_8859_1);
  }

  @Test
  void charsetOf_quotedCharsetWithInnerWhitespaceGiven_returnsThatCharset() {
    // Act
    Charset charset = HttpHeaders.charsetOf("text/plain; charset=\" ISO-8859-1 \"");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.ISO_8859_1);
  }

  @Test
  void charsetOf_charsetInsideUnquotedBoundaryGiven_isIgnored() {
    // Act
    Charset charset = HttpHeaders.charsetOf("multipart/form-data; boundary=--charset=ISO-8859-1--");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_charsetInsideQuotedParameterValueGiven_isIgnored() {
    // Act
    Charset charset =
        HttpHeaders.charsetOf("multipart/form-data; boundary=\"x; charset=ISO-8859-1\"");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.UTF_8);
  }

  @Test
  void charsetOf_charsetAfterQuotedParameterGiven_returnsThatCharset() {
    // Act
    Charset charset =
        HttpHeaders.charsetOf("multipart/form-data; boundary=\"x;y\"; charset=ISO-8859-1");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.ISO_8859_1);
  }

  @Test
  void charsetOf_escapedQuoteInParameterValueGiven_stillFindsCharset() {
    // Act
    Charset charset =
        HttpHeaders.charsetOf("multipart/form-data; boundary=\"a\\\"b\"; charset=ISO-8859-1");

    // Assert
    assertThat(charset).isEqualTo(StandardCharsets.ISO_8859_1);
  }
}
