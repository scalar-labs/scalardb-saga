package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HttpMethodTest {

  @Test
  void hasBody_postPutPatch_returnsTrue() {
    // Act & Assert
    assertThat(HttpMethod.POST.hasBody()).isTrue();
    assertThat(HttpMethod.PUT.hasBody()).isTrue();
    assertThat(HttpMethod.PATCH.hasBody()).isTrue();
  }

  @Test
  void hasBody_getDelete_returnsFalse() {
    // Act & Assert
    assertThat(HttpMethod.GET.hasBody()).isFalse();
    assertThat(HttpMethod.DELETE.hasBody()).isFalse();
  }
}
