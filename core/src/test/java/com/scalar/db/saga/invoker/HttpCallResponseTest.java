package com.scalar.db.saga.invoker;

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
  void jsonObject_objectBody_returnsMap() throws Exception {
    assertThat(response(200, "{\"k\":\"v\"}").jsonObject()).containsEntry("k", "v");
  }

  @Test
  void jsonObject_emptyBody_returnsEmptyMap() throws Exception {
    assertThat(response(200, "").jsonObject()).isEmpty();
  }

  @Test
  void jsonObject_arrayBody_throwsNonRetryable() {
    Throwable t = catchThrowable(() -> response(200, "[1,2,3]").jsonObject());

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void jsonArray_arrayBody_returnsList() throws Exception {
    assertThat(response(200, "[1,2,3]").jsonArray()).containsExactly(1, 2, 3);
  }

  @Test
  void jsonArray_emptyBody_returnsEmptyList() throws Exception {
    assertThat(response(200, "").jsonArray()).isEmpty();
  }

  @Test
  void jsonArray_objectBody_throwsNonRetryable() {
    Throwable t = catchThrowable(() -> response(200, "{\"k\":\"v\"}").jsonArray());

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
  void rawString_noCharset_defaultsToUtf8() {
    byte[] body = "café".getBytes(StandardCharsets.UTF_8);

    assertThat(response(200, Map.of(), body).rawString()).isEqualTo("café");
  }

  @Test
  void rawString_charsetInContentType_usesThatCharset() {
    byte[] body = "café".getBytes(StandardCharsets.ISO_8859_1);
    HttpCallResponse response =
        response(200, Map.of("Content-Type", List.of("text/plain; charset=ISO-8859-1")), body);

    assertThat(response.rawString()).isEqualTo("café");
  }

  @Test
  void rawBytes_returnsBodyContent() {
    byte[] body = {1, 2, 3};

    assertThat(response(200, Map.of(), body).rawBytes()).containsExactly(1, 2, 3);
  }

  @Test
  void rawBytes_isDefensiveCopy() {
    byte[] body = {1, 2, 3};
    HttpCallResponse response = response(200, Map.of(), body);

    response.rawBytes()[0] = 9; // mutate the returned array

    assertThat(response.rawBytes()).containsExactly(1, 2, 3); // internal state unchanged
  }
}
