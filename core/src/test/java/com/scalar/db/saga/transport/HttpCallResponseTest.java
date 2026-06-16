package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HttpCallResponseTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static HttpCallResponse response(
      int status, Map<String, List<String>> headers, byte[] body) {
    return new HttpCallResponse(status, headers, body, MAPPER);
  }

  private static HttpCallResponse response(int status, String body) {
    return response(status, Map.of(), body.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void status_returnsStatusCode() {
    assertThat(response(201, "{}").status()).isEqualTo(201);
  }

  @Test
  void isSuccess_2xx_returnsTrue() {
    assertThat(response(200, "{}").isSuccess()).isTrue();
  }

  @Test
  void isSuccess_non2xx_returnsFalse() {
    assertThat(response(404, "{}").isSuccess()).isFalse();
  }

  @Test
  void bodyJsonObject_objectBody_returnsMap() throws Exception {
    assertThat(response(200, "{\"k\":\"v\"}").bodyJsonObject()).containsEntry("k", "v");
  }

  @Test
  void bodyJsonObject_emptyBody_returnsEmptyMap() throws Exception {
    assertThat(response(200, "").bodyJsonObject()).isEmpty();
  }

  @Test
  void bodyJsonObject_arrayBody_throwsNonRetryable() {
    Throwable t = catchThrowable(() -> response(200, "[1,2,3]").bodyJsonObject());

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void bodyJsonArray_arrayBody_returnsList() throws Exception {
    assertThat(response(200, "[1,2,3]").bodyJsonArray()).containsExactly(1, 2, 3);
  }

  @Test
  void bodyJsonArray_emptyBody_returnsEmptyList() throws Exception {
    assertThat(response(200, "").bodyJsonArray()).isEmpty();
  }

  @Test
  void bodyJsonArray_objectBody_throwsNonRetryable() {
    Throwable t = catchThrowable(() -> response(200, "{\"k\":\"v\"}").bodyJsonArray());

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void header_caseInsensitive_returnsValue() {
    HttpCallResponse response = response(200, Map.of("Location", List.of("/x")), new byte[0]);

    assertThat(response.header("location")).hasValue("/x");
  }

  @Test
  void header_missing_returnsEmpty() {
    assertThat(response(200, "{}").header("Missing")).isEmpty();
  }

  @Test
  void headers_multiValued_returnsAll() {
    HttpCallResponse response =
        response(200, Map.of("Set-Cookie", List.of("a=1", "b=2")), new byte[0]);

    assertThat(response.headers("set-cookie")).containsExactly("a=1", "b=2");
  }

  @Test
  void bodyString_noCharset_defaultsToUtf8() {
    byte[] body = "café".getBytes(StandardCharsets.UTF_8);

    assertThat(response(200, Map.of(), body).bodyString()).isEqualTo("café");
  }

  @Test
  void bodyString_charsetInContentType_usesThatCharset() {
    byte[] body = "café".getBytes(StandardCharsets.ISO_8859_1);
    HttpCallResponse response =
        response(200, Map.of("Content-Type", List.of("text/plain; charset=ISO-8859-1")), body);

    assertThat(response.bodyString()).isEqualTo("café");
  }

  @Test
  void bodyBytes_returnsBodyContent() {
    byte[] body = {1, 2, 3};

    assertThat(response(200, Map.of(), body).bodyBytes()).containsExactly(1, 2, 3);
  }

  @Test
  void bodyBytes_isDefensiveCopy() {
    byte[] body = {1, 2, 3};
    HttpCallResponse response = response(200, Map.of(), body);

    response.bodyBytes()[0] = 9; // mutate the returned array

    assertThat(response.bodyBytes()).containsExactly(1, 2, 3); // internal state unchanged
  }
}
