package com.scalar.db.saga.server.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SagaAuthRequestTest {

  @Test
  void fromHeaders_headerLookupIsCaseInsensitive() {
    // Arrange
    SagaAuthRequest request =
        SagaAuthRequest.fromHeaders(
            "GET /sagas/x", "10.0.0.1", Map.of("Authorization", "Bearer token"));

    // Assert — any casing resolves the same header
    assertThat(request.header("authorization")).contains("Bearer token");
    assertThat(request.header("AUTHORIZATION")).contains("Bearer token");
    assertThat(request.header("Authorization")).contains("Bearer token");
  }

  @Test
  void fromHeaders_absentHeader_returnsEmpty() {
    // Arrange
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("GET /sagas/x", null, Map.of());

    // Assert
    assertThat(request.header("Authorization")).isEmpty();
  }

  @Test
  void fromHeaders_copiesSourceMap_laterMutationIgnored() {
    // Arrange
    Map<String, String> source = new HashMap<>();
    source.put("X-Api-Key", "secret");
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("GET /sagas/x", null, source);

    // Act — mutate the source map after construction
    source.put("X-Api-Key", "changed");
    source.remove("X-Api-Key");

    // Assert — the request kept a snapshot
    assertThat(request.header("x-api-key")).contains("secret");
  }

  @Test
  void metadata_operationAndRemoteAddressExposed() {
    // Arrange
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("POST /sagas", "192.168.1.5", Map.of());

    // Assert
    assertThat(request.operation()).isEqualTo("POST /sagas");
    assertThat(request.remoteAddress()).contains("192.168.1.5");
  }

  @Test
  void remoteAddress_nullGiven_returnsEmpty() {
    // Arrange
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("POST /sagas", null, Map.of());

    // Assert
    assertThat(request.remoteAddress()).isEmpty();
  }

  @Test
  void fromHeaderLookup_delegatesToTheGivenFunction() {
    // Arrange — a lookup that only knows one header, case-insensitively
    SagaAuthRequest request =
        SagaAuthRequest.fromHeaderLookup(
            "GET /sagas/x",
            "10.0.0.2",
            name -> name.equalsIgnoreCase("Authorization") ? "Bearer abc" : null);

    // Assert
    assertThat(request.header("authorization")).contains("Bearer abc");
    assertThat(request.header("X-Other")).isEmpty();
    assertThat(request.remoteAddress()).contains("10.0.0.2");
  }
}
