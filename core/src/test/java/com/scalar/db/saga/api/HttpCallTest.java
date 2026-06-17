package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HttpCallTest {

  @Test
  void newBuilder_pathOnlyGiven_defaultsToPostWithEmptyMaps() {
    // Act
    HttpCall call = HttpCall.newBuilder("/debit").build();

    // Assert
    assertThat(call.transport()).isEqualTo(CallSpec.Transport.HTTP);
    assertThat(call.getMethod()).isEqualTo(HttpMethod.POST);
    assertThat(call.getPath()).isEqualTo("/debit");
    assertThat(call.getQuery()).isEmpty();
    assertThat(call.getJsonBody()).isEmpty();
    assertThat(call.getOutput()).isEmpty();
  }

  @Test
  void build_withAllFieldsGiven_setsAllFields() {
    // Act
    HttpCall call =
        HttpCall.newBuilder("/accounts/${accountId}/debit")
            .method(HttpMethod.PUT)
            .query(Map.of("dryRun", "${dryRun}"))
            .jsonBody(Map.of("amount", "${amount}"))
            .output(Map.of("debitId", "$.debit_id"))
            .build();

    // Assert
    assertThat(call.getMethod()).isEqualTo(HttpMethod.PUT);
    assertThat(call.getPath()).isEqualTo("/accounts/${accountId}/debit");
    assertThat(call.getQuery()).containsExactly(Map.entry("dryRun", "${dryRun}"));
    assertThat(call.getJsonBody()).containsExactly(Map.entry("amount", "${amount}"));
    assertThat(call.getOutput()).containsExactly(Map.entry("debitId", "$.debit_id"));
  }

  @SuppressWarnings("NullAway")
  @Test
  void newBuilder_nullPathGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> HttpCall.newBuilder(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void newBuilder_blankPathGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> HttpCall.newBuilder("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_getWithRequestBodyGiven_throwsException() {
    // Act & Assert — the body-less-verb rule fires at build(), not when jsonBody() is set.
    assertThatThrownBy(
            () ->
                HttpCall.newBuilder("/users")
                    .method(HttpMethod.GET)
                    .jsonBody(Map.of("a", "${b}"))
                    .build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_deleteWithRequestBodyGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                HttpCall.newBuilder("/users/${id}")
                    .method(HttpMethod.DELETE)
                    .jsonBody(Map.of("a", "${b}"))
                    .build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_getWithQueryButNoBody_succeeds() {
    // Act
    HttpCall call =
        HttpCall.newBuilder("/users")
            .method(HttpMethod.GET)
            .query(Map.of("id", "${userId}"))
            .output(Map.of("name", "$.name"))
            .build();

    // Assert
    assertThat(call.getMethod()).isEqualTo(HttpMethod.GET);
    assertThat(call.getJsonBody()).isEmpty();
    assertThat(call.getQuery()).containsExactly(Map.entry("id", "${userId}"));
  }

  @Test
  void build_withBodyAndContentType_setsBothFields() {
    // Act
    HttpCall call =
        HttpCall.newBuilder("/notify")
            .method(HttpMethod.POST)
            .stringBody("<msg>${text}</msg>")
            .contentType("application/xml")
            .output(Map.of("raw", HttpCall.BODY_OUTPUT))
            .build();

    // Assert
    assertThat(call.getStringBody()).isEqualTo("<msg>${text}</msg>");
    assertThat(call.getContentType()).isEqualTo("application/xml");
    assertThat(call.getJsonBody()).isEmpty();
    assertThat(call.getOutput()).containsExactly(Map.entry("raw", "$body"));
  }

  @Test
  void build_pathOnlyGiven_bodyAndContentTypeDefaultToNull() {
    // Act
    HttpCall call = HttpCall.newBuilder("/debit").build();

    // Assert
    assertThat(call.getStringBody()).isNull();
    assertThat(call.getContentType()).isNull();
  }

  @Test
  void build_requestAndBodyBothGiven_throwsException() {
    // Act & Assert — a flat-map body and a raw string body are mutually exclusive.
    assertThatThrownBy(
            () ->
                HttpCall.newBuilder("/debit")
                    .jsonBody(Map.of("amount", "${amount}"))
                    .stringBody("raw")
                    .build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_getWithStringBodyGiven_throwsException() {
    // Act & Assert — the body-less-verb rule also rejects a raw string body.
    assertThatThrownBy(
            () -> HttpCall.newBuilder("/users").method(HttpMethod.GET).stringBody("raw").build())
        .isInstanceOf(IllegalStateException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void stringBody_nullGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> HttpCall.newBuilder("/debit").stringBody(null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void contentType_nullGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> HttpCall.newBuilder("/debit").contentType(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equals_differentBody_areNotEqual() {
    // Arrange
    HttpCall a = HttpCall.newBuilder("/x").stringBody("a").build();
    HttpCall b = HttpCall.newBuilder("/x").stringBody("b").build();

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void build_withMutableSourceMap_copiesDefensively() {
    // Arrange
    Map<String, String> jsonBody = new HashMap<>();
    jsonBody.put("amount", "${amount}");
    HttpCall call = HttpCall.newBuilder("/debit").jsonBody(jsonBody).build();

    // Act
    jsonBody.put("injected", "x");

    // Assert
    assertThat(call.getJsonBody()).containsOnlyKeys("amount");
  }

  @Test
  void getQuery_returnsUnmodifiableMap() {
    // Arrange
    HttpCall call = HttpCall.newBuilder("/debit").query(Map.of("a", "b")).build();

    // Act & Assert
    assertThatThrownBy(() -> call.getQuery().put("c", "d"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void equals_sameFields_areEqual() {
    // Arrange
    HttpCall a = HttpCall.newBuilder("/debit").jsonBody(Map.of("amount", "${amount}")).build();
    HttpCall b = HttpCall.newBuilder("/debit").jsonBody(Map.of("amount", "${amount}")).build();

    // Act & Assert
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }

  @Test
  void equals_differentMethod_areNotEqual() {
    // Arrange
    HttpCall post = HttpCall.newBuilder("/debit").method(HttpMethod.POST).build();
    HttpCall put = HttpCall.newBuilder("/debit").method(HttpMethod.PUT).build();

    // Act & Assert
    assertThat(post).isNotEqualTo(put);
  }
}
