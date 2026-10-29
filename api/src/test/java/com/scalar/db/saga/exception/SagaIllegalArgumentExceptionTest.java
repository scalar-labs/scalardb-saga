package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class SagaIllegalArgumentExceptionTest {

  @Test
  void constructor_detailGiven_carriesInvalidArgumentCodeAndDetail() {
    // Act
    SagaIllegalArgumentException e = new SagaIllegalArgumentException("malformed page token");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_ARGUMENT);
    assertThat(e.getMetadata()).containsExactly(entry("detail", "malformed page token"));
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-10002: Argument is invalid [detail=malformed page token]");
  }

  @Test
  void constructor_detailAndCauseGiven_setsCauseToo() {
    // Arrange
    RuntimeException cause = new RuntimeException("inner");

    // Act
    SagaIllegalArgumentException e = new SagaIllegalArgumentException("bad token", cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_ARGUMENT);
    assertThat(e.getCause()).isSameAs(cause);
  }

  @Test
  @SuppressWarnings("NullAway") // deliberately passing null to exercise the runtime guard
  void constructor_nullDetailGiven_throwsNpe() {
    assertThatThrownBy(() -> new SagaIllegalArgumentException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  @SuppressWarnings("NullAway") // deliberately passing null to exercise the runtime guard
  void constructor_nullCauseGiven_throwsNpe() {
    assertThatThrownBy(() -> new SagaIllegalArgumentException("bad token", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void fromWire_metadataWithDetailGiven_reconstructsTheSameCodeAndDetail() {
    // Act
    SagaIllegalArgumentException e =
        SagaIllegalArgumentException.fromWire(Collections.singletonMap("detail", "blank reason"));

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_ARGUMENT);
    assertThat(e.getMetadata()).containsExactly(entry("detail", "blank reason"));
  }

  @Test
  void fromWire_metadataMissingDetailGiven_throwsNpe() {
    // A wire map that doesn't satisfy the code's schema is protocol drift; ExceptionRegistry
    // catches this and degrades to UNRECOGNIZED_SERVER_ERROR rather than shimming a partial state.
    assertThatThrownBy(() -> SagaIllegalArgumentException.fromWire(Collections.emptyMap()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void isNotAnIllegalArgumentException_soCallersCatchTheSagaFamilyInstead() {
    // The name describes the condition, not a superclass: it extends SagaRuntimeException so that
    // catch (SagaRuntimeException) covers it and getErrorCode() is always reachable.
    SagaIllegalArgumentException e = new SagaIllegalArgumentException("bad token");

    assertThat(e).isInstanceOf(SagaRuntimeException.class);
    assertThat(e).isNotInstanceOf(IllegalArgumentException.class);
  }
}
