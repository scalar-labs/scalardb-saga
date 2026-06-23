package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeclarativeExpressionsTest {

  private static SagaContext ctx(Map<String, Object> data) {
    return new FakeSagaContext("saga-1", data);
  }

  // --- resolveObject -------------------------------------------------------

  @Test
  void resolveObject_singlePlaceholder_preservesValueType() throws Exception {
    // Act
    Object value = DeclarativeExpressions.resolveObject("${amount}", ctx(Map.of("amount", 1234)));

    // Assert
    assertThat(value).isEqualTo(1234);
  }

  @Test
  void resolveObject_literal_passesThrough() throws Exception {
    // Act
    Object value = DeclarativeExpressions.resolveObject("USD", ctx(Map.of()));

    // Assert
    assertThat(value).isEqualTo("USD");
  }

  @Test
  void resolveObject_embeddedPlaceholder_resolvesToString() throws Exception {
    // Act
    Object value = DeclarativeExpressions.resolveObject("acct-${id}", ctx(Map.of("id", 7)));

    // Assert
    assertThat(value).isEqualTo("acct-7");
  }

  @Test
  void resolveObject_missingKey_throwsNonRetryable() {
    // Act
    Throwable thrown =
        org.assertj.core.api.Assertions.catchThrowable(
            () -> DeclarativeExpressions.resolveObject("${missing}", ctx(Map.of())));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
  }

  // --- resolveString -------------------------------------------------------

  @Test
  void resolveString_multiplePlaceholders_substitutesAll() throws Exception {
    // Act
    String value =
        DeclarativeExpressions.resolveString(
            "/users/${userId}/orders/${orderId}", ctx(Map.of("userId", "U1", "orderId", 9)));

    // Assert
    assertThat(value).isEqualTo("/users/U1/orders/9");
  }

  @Test
  void resolveString_missingKey_throwsNonRetryable() {
    // Act & Assert
    assertThatThrownBy(() -> DeclarativeExpressions.resolveString("/u/${missing}", ctx(Map.of())))
        .isInstanceOf(TransportException.class);
  }

  // --- resolvePath ---------------------------------------------------------

  @Test
  void resolvePath_plainValue_substitutesAndKeepsSeparators() throws Exception {
    // Act
    String path = DeclarativeExpressions.resolvePath("/orders/${id}", ctx(Map.of("id", 7)));

    // Assert — literal '/' separators preserved, value substituted
    assertThat(path).isEqualTo("/orders/7");
  }

  @Test
  void resolvePath_valueWithReservedChars_percentEncodesThem() throws Exception {
    // Act — a value carrying ?, #, space, and & must not alter the request target
    String path =
        DeclarativeExpressions.resolvePath("/orders/${id}", ctx(Map.of("id", "7?x=1#f &y")));

    // Assert
    assertThat(path).isEqualTo("/orders/7%3Fx%3D1%23f%20%26y");
  }

  @Test
  void resolvePath_valueWithSlash_encodesToSingleSegment() throws Exception {
    // Act — a '/' in the value stays one segment and cannot traverse the path
    String path =
        DeclarativeExpressions.resolvePath("/orders/${id}", ctx(Map.of("id", "../../admin")));

    // Assert
    assertThat(path).isEqualTo("/orders/..%2F..%2Fadmin");
  }

  @Test
  void resolvePath_missingKey_throwsNonRetryable() {
    // Act
    Throwable thrown =
        org.assertj.core.api.Assertions.catchThrowable(
            () -> DeclarativeExpressions.resolvePath("/u/${missing}", ctx(Map.of())));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
  }

  // --- resolveObjectMap / resolveStringMap ---------------------------------

  @Test
  void resolveObjectMap_mixedValues_resolvesEach() throws Exception {
    // Arrange
    Map<String, String> templates = new LinkedHashMap<>();
    templates.put("amount", "${amount}");
    templates.put("currency", "USD");

    // Act
    Map<String, Object> resolved =
        DeclarativeExpressions.resolveObjectMap(templates, ctx(Map.of("amount", 50)));

    // Assert
    assertThat(resolved).containsEntry("amount", 50).containsEntry("currency", "USD");
  }

  @Test
  void resolveStringMap_resolvesValuesToStrings() throws Exception {
    // Act
    Map<String, String> resolved =
        DeclarativeExpressions.resolveStringMap(Map.of("id", "${id}"), ctx(Map.of("id", 42)));

    // Assert
    assertThat(resolved).containsEntry("id", "42");
  }

  // --- extractOutput -------------------------------------------------------

  @Test
  void extractOutput_topLevelField_extractsValue() throws Exception {
    // Act
    Map<String, Object> output =
        DeclarativeExpressions.extractOutput(
            Map.of("debitId", "$.debit_id"), Map.of("debit_id", "DBT-1"));

    // Assert
    assertThat(output).containsEntry("debitId", "DBT-1");
  }

  @Test
  void extractOutput_nestedField_navigatesDottedPath() throws Exception {
    // Arrange
    Map<String, Object> response = Map.of("profile", Map.of("name", "Ann"));

    // Act
    Map<String, Object> output =
        DeclarativeExpressions.extractOutput(Map.of("name", "$.profile.name"), response);

    // Assert
    assertThat(output).containsEntry("name", "Ann");
  }

  @Test
  void extractOutput_bodyTokenWithJsonResponse_capturesRawString() throws Exception {
    // Arrange
    HttpCallResponse response = jsonResponse("{\"k\":\"v\"}");

    // Act
    Map<String, Object> output =
        DeclarativeExpressions.extractOutput(Map.of("raw", "$body"), response);

    // Assert — the whole raw body is captured verbatim as a String.
    assertThat(output).containsEntry("raw", "{\"k\":\"v\"}");
  }

  @Test
  void extractOutput_bodyTokenWithNonJsonResponse_capturesRawString() throws Exception {
    // Arrange — a plain-text (non-JSON) response body.
    HttpCallResponse response = textResponse("plain text body");

    // Act — $body must not require JSON decoding.
    Map<String, Object> output =
        DeclarativeExpressions.extractOutput(Map.of("raw", "$body"), response);

    // Assert
    assertThat(output).containsEntry("raw", "plain text body");
  }

  @Test
  void extractOutput_pathAndBodyTokensMixed_capturesBoth() throws Exception {
    // Arrange
    HttpCallResponse response = jsonResponse("{\"id\":\"X-1\"}");

    // Act
    Map<String, Object> output =
        DeclarativeExpressions.extractOutput(Map.of("id", "$.id", "raw", "$body"), response);

    // Assert
    assertThat(output).containsEntry("id", "X-1").containsEntry("raw", "{\"id\":\"X-1\"}");
  }

  @Test
  void extractOutput_dollarPathWithNonJsonResponseBody_throwsNonRetryable() {
    // Arrange — a plain-text body that is not a JSON object.
    HttpCallResponse response = textResponse("plain text body");

    // Act — a $.path expression forces a JSON decode, which fails on the non-JSON body.
    Throwable thrown =
        org.assertj.core.api.Assertions.catchThrowable(
            () -> DeclarativeExpressions.extractOutput(Map.of("id", "$.id"), response));

    // Assert — a contract/definition error (non-JSON where JSON was promised), not a transient one.
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
  }

  private static HttpCallResponse jsonResponse(String body) {
    return new HttpCallResponse(
        200,
        Map.of(HttpHeaders.CONTENT_TYPE, java.util.List.of("application/json")),
        body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
        new com.fasterxml.jackson.databind.ObjectMapper());
  }

  private static HttpCallResponse textResponse(String body) {
    return new HttpCallResponse(
        200,
        Map.of(HttpHeaders.CONTENT_TYPE, java.util.List.of("text/plain")),
        body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
        new com.fasterxml.jackson.databind.ObjectMapper());
  }

  @Test
  void extractOutput_missingField_throwsNonRetryable() {
    // Act
    Throwable thrown =
        org.assertj.core.api.Assertions.catchThrowable(
            () ->
                DeclarativeExpressions.extractOutput(
                    Map.of("x", "$.missing"), Map.of("present", 1)));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
  }

  @Test
  void extractOutput_navigateIntoNonObject_throwsNonRetryable() {
    // Arrange — $.a.b but a is a scalar.
    Map<String, Object> response = Map.of("a", "scalar");

    // Act & Assert
    assertThatThrownBy(() -> DeclarativeExpressions.extractOutput(Map.of("x", "$.a.b"), response))
        .isInstanceOf(TransportException.class);
  }

  @Test
  void extractOutput_pathWithoutDollarPrefix_throwsNonRetryable() {
    // Act & Assert
    assertThatThrownBy(
            () -> DeclarativeExpressions.extractOutput(Map.of("x", "debit_id"), Map.of()))
        .isInstanceOf(TransportException.class);
  }

  @Test
  void extractOutput_emptySegment_throwsNonRetryable() {
    // Arrange — a doubled dot yields an empty middle segment (defense in depth; HttpCall.build()
    // rejects this up front, but extractPath is reachable directly).
    Map<String, Object> response = Map.of("profile", Map.of("name", "alice"));

    // Act
    Throwable thrown =
        org.assertj.core.api.Assertions.catchThrowable(
            () ->
                DeclarativeExpressions.extractOutput(Map.of("name", "$.profile..name"), response));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
  }
}
