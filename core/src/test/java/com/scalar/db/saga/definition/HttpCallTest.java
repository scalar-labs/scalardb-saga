package com.scalar.db.saga.definition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.HttpMethod;
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

  @Test
  void build_outputBodyTokenGiven_succeeds() {
    // Act
    HttpCall call =
        HttpCall.newBuilder("/debit").output(Map.of("raw", HttpCall.BODY_OUTPUT)).build();

    // Assert
    assertThat(call.getOutput()).containsExactly(Map.entry("raw", HttpCall.BODY_OUTPUT));
  }

  @Test
  void build_outputPathWithoutDollarPrefixGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> HttpCall.newBuilder("/debit").output(Map.of("id", "debit_id")).build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_outputPathWithEmptySegmentGiven_throwsException() {
    // Act & Assert — a doubled dot yields an empty middle segment.
    assertThatThrownBy(
            () -> HttpCall.newBuilder("/debit").output(Map.of("name", "$.profile..name")).build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_outputPathTrailingDotGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> HttpCall.newBuilder("/debit").output(Map.of("name", "$.profile.")).build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_bareDollarDotGiven_throwsException() {
    // Act & Assert — "$." has a single empty segment.
    assertThatThrownBy(() -> HttpCall.newBuilder("/debit").output(Map.of("x", "$.")).build())
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

  @Test
  void referencedContextKeys_acrossPathQueryAndBody_collectsAllKeys() {
    // Arrange
    HttpCall call =
        HttpCall.newBuilder("/accounts/${accountId}/debit")
            .query(Map.of("trace", "${traceId}"))
            .jsonBody(Map.of("amount", "${amount}", "note", "literal-${memo}-suffix"))
            .build();

    // Act & Assert
    assertThat(call.referencedContextKeys())
        .containsExactlyInAnyOrder("accountId", "traceId", "amount", "memo");
  }

  @Test
  void referencedContextKeys_fromStringBody_collectsKeys() {
    // Arrange
    HttpCall call =
        HttpCall.newBuilder("/notify").stringBody("<msg>${text} for ${userId}</msg>").build();

    // Act & Assert
    assertThat(call.referencedContextKeys()).containsExactlyInAnyOrder("text", "userId");
  }

  @Test
  void referencedContextKeys_noPlaceholdersOrEmptyPlaceholder_isEmpty() {
    // Arrange — a literal path, a literal body, and an empty ${} placeholder (ignored).
    HttpCall call =
        HttpCall.newBuilder("/static").jsonBody(Map.of("k", "v", "blank", "${}")).build();

    // Act & Assert
    assertThat(call.referencedContextKeys()).isEmpty();
  }

  @Test
  void producedContextKeys_returnsOutputKeys() {
    // Arrange
    HttpCall call =
        HttpCall.newBuilder("/debit")
            .output(Map.of("debitId", "$.debit_id", "raw", HttpCall.BODY_OUTPUT))
            .build();

    // Act & Assert
    assertThat(call.producedContextKeys()).containsExactlyInAnyOrder("debitId", "raw");
  }

  @Test
  void producedContextKeys_noOutput_isEmpty() {
    assertThat(HttpCall.newBuilder("/debit").build().producedContextKeys()).isEmpty();
  }

  @Test
  void build_asyncGiven_setsAsync() {
    // Act
    HttpCall call = HttpCall.newBuilder("/debit").async(true).build();

    // Assert
    assertThat(call.isAsync()).isTrue();
  }

  @Test
  void build_pathOnlyGiven_asyncDefaultsToFalse() {
    assertThat(HttpCall.newBuilder("/debit").build().isAsync()).isFalse();
  }

  @Test
  void equals_asyncDiffers_notEqual() {
    // Arrange
    HttpCall async = HttpCall.newBuilder("/debit").async(true).build();
    HttpCall sync = HttpCall.newBuilder("/debit").build();

    // Assert
    assertThat(async).isNotEqualTo(sync);
  }

  @Test
  void build_callbackTimeoutMillisGiven_setsCallbackTimeout() {
    // Act
    HttpCall call =
        HttpCall.newBuilder("/debit").async(true).callbackTimeoutMillis(600_000).build();

    // Assert
    assertThat(call.callbackTimeoutMillis()).isEqualTo(600_000);
  }

  @Test
  void build_pathOnlyGiven_callbackTimeoutDefaultsToZero() {
    assertThat(HttpCall.newBuilder("/debit").build().callbackTimeoutMillis()).isZero();
  }

  @Test
  void build_negativeCallbackTimeoutMillisGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> HttpCall.newBuilder("/debit").callbackTimeoutMillis(-1).build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void equals_callbackTimeoutDiffers_notEqual() {
    // Arrange
    HttpCall a = HttpCall.newBuilder("/debit").async(true).callbackTimeoutMillis(1000).build();
    HttpCall b = HttpCall.newBuilder("/debit").async(true).callbackTimeoutMillis(2000).build();

    // Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void build_callbackTimeoutWithoutAsyncGiven_throwsException() {
    // Act & Assert — a callback timeout is meaningless without async; reject it at build time
    assertThatThrownBy(() -> HttpCall.newBuilder("/debit").callbackTimeoutMillis(1000).build())
        .isInstanceOf(IllegalStateException.class);
  }
}
