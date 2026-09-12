package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class SagaInvalidRequestExceptionTest {

  @Test
  void constructor_detailGiven_carriesInvalidRequestCodeAndDetail() {
    // Act
    SagaInvalidRequestException e = new SagaInvalidRequestException("'name' is required");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_REQUEST);
    assertThat(e.getMetadata()).containsExactly(entry("detail", "'name' is required"));
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-10001: Request is invalid [detail='name' is required]");
  }

  @Test
  @SuppressWarnings("NullAway") // deliberately passing null to exercise the runtime guard
  void constructor_nullDetailGiven_throwsNpe() {
    assertThatThrownBy(() -> new SagaInvalidRequestException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void fromWire_metadataWithDetailGiven_reconstructsTheSameCodeAndDetail() {
    // Act
    SagaInvalidRequestException e =
        SagaInvalidRequestException.fromWire(
            Collections.singletonMap("detail", "malformed request body"));

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_REQUEST);
    assertThat(e.getMetadata()).containsExactly(entry("detail", "malformed request body"));
  }

  @Test
  void fromWire_metadataMissingDetailGiven_throwsNpe() {
    assertThatThrownBy(() -> SagaInvalidRequestException.fromWire(Collections.emptyMap()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void carriesADifferentCodeThanSagaIllegalArgumentException() {
    // The two are deliberately distinct: this one is the wire message failing validation, which
    // only a remote caller can produce; the other is a caller value both modes can reject.
    assertThat(new SagaInvalidRequestException("x").getErrorCode())
        .isNotEqualTo(new SagaIllegalArgumentException("x").getErrorCode());
  }
}
