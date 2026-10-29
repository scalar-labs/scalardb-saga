package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SagaOverloadedExceptionTest {

  @Test
  public void constructor_carriesTheEngineOverloadedCode() {
    SagaOverloadedException exception = new SagaOverloadedException();

    assertThat(exception.getErrorCode()).isEqualTo(SagaErrorCode.ENGINE_OVERLOADED);
  }

  @Test
  public void constructor_carriesNoMetadata() {
    // The code declares ErrorMetadataSchema.none() so the cap never reaches a caller; an empty map
    // is what keeps that true on the wire.
    SagaOverloadedException exception = new SagaOverloadedException();

    assertThat(exception.getMetadata()).isEmpty();
  }

  @Test
  public void constructor_isASagaRuntimeException() {
    // Callers that key on the retryable category rather than the concrete type rely on this.
    assertThat(new SagaOverloadedException()).isInstanceOf(SagaRuntimeException.class);
  }
}
