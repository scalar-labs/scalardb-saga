package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SagaDefinitionExceptionTest {

  @Test
  void definitionInvalid_always_carriesCodeSagaNameAndDetail() {
    // Arrange & Act
    SagaDefinitionException e =
        SagaDefinitionException.definitionInvalid("transfer", "duplicate step name 'debit'");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_DEFINITION);
    assertThat(e.getMetadata())
        .containsEntry("saga_name", "transfer")
        .containsEntry("detail", "duplicate step name 'debit'")
        .hasSize(2);
    assertThat(e.getCause()).isNull();
  }

  @Test
  void declarativeStepInvalid_always_carriesStepDefinitionCodeWithStepName() {
    // Arrange & Act
    SagaDefinitionException e =
        SagaDefinitionException.declarativeStepInvalid("debit", "missing 'path'");

    // Assert — its own code with the step name as a real metadata field. It used to route through
    // INVALID_DEFINITION, faking the required saga_name as "" (no throw site knows the saga name)
    // and smuggling the step name into the detail text.
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_STEP_DEFINITION);
    assertThat(e.getMetadata())
        .containsEntry("step_name", "debit")
        .containsEntry("detail", "missing 'path'")
        .hasSize(2);
  }

  @Test
  void definitionMalformed_always_carriesCodeSourceDetailAndCause() {
    // Arrange
    RuntimeException cause = new RuntimeException("bad json");

    // Act
    SagaDefinitionException e = SagaDefinitionException.definitionMalformed("JSON", cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.MALFORMED_DEFINITION);
    assertThat(e.getMetadata())
        .containsEntry("source", "inline JSON")
        .containsEntry("detail", "failed to parse JSON definition")
        .hasSize(2);
    assertThat(e.getCause()).isSameAs(cause);
  }

  @Test
  void sourceUnreadable_noCauseGiven_carriesCodeAndSource() {
    // Arrange & Act
    SagaDefinitionException e = SagaDefinitionException.sourceUnreadable("missing.json");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNREADABLE_DEFINITION_SOURCE);
    assertThat(e.getMetadata()).containsEntry("source", "missing.json").hasSize(1);
    assertThat(e.getCause()).isNull();
  }

  @Test
  void sourceUnreadable_causeGiven_carriesCodeSourceAndCause() {
    // Arrange
    RuntimeException cause = new RuntimeException("io");

    // Act
    SagaDefinitionException e = SagaDefinitionException.sourceUnreadable("f.json", cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNREADABLE_DEFINITION_SOURCE);
    assertThat(e.getMetadata()).containsEntry("source", "f.json").hasSize(1);
    assertThat(e.getCause()).isSameAs(cause);
  }

  @Test
  void versionContentConflict_always_carriesCodeNameAndVersion() {
    // Arrange & Act
    SagaDefinitionException e = SagaDefinitionException.versionContentConflict("transfer", "1.0");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT);
    // saga_name, not name: every sibling code that carries a definition name uses saga_name, and
    // the key freezes at first release, so a client dispatching on metadata keys never
    // special-cases this one code.
    assertThat(e.getMetadata())
        .containsEntry("saga_name", "transfer")
        .containsEntry("version", "1.0")
        .hasSize(2);
  }

  @Test
  void stepClassInvalid_noCauseGiven_carriesCodeClassAndDetail() {
    // Arrange & Act
    SagaDefinitionException e =
        SagaDefinitionException.stepClassInvalid("com.example.Foo", "is abstract");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_STEP_CLASS);
    assertThat(e.getMetadata())
        .containsEntry("step_class", "com.example.Foo")
        .containsEntry("detail", "is abstract")
        .hasSize(2);
    assertThat(e.getCause()).isNull();
  }

  @Test
  void stepClassInvalid_causeGiven_carriesCodeClassDetailAndCause() {
    // Arrange
    ClassNotFoundException cause = new ClassNotFoundException("com.example.Foo");

    // Act
    SagaDefinitionException e =
        SagaDefinitionException.stepClassInvalid(
            "com.example.Foo", "not found on classpath", cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_STEP_CLASS);
    assertThat(e.getMetadata())
        .containsEntry("step_class", "com.example.Foo")
        .containsEntry("detail", "not found on classpath")
        .hasSize(2);
    assertThat(e.getCause()).isSameAs(cause);
  }

  @Test
  void stepClassNotSupportedOnServer_always_carriesCodeAndBothNames() {
    // Arrange & Act
    SagaDefinitionException e =
        SagaDefinitionException.stepClassNotSupportedOnServer("transfer", "debit");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER);
    assertThat(e.getMetadata())
        .containsEntry("saga_name", "transfer")
        .containsEntry("step_name", "debit")
        .hasSize(2);
  }

  @Test
  void httpEndpointLookupFailed_always_carriesCodeAndDetail() {
    // Arrange & Act
    SagaDefinitionException e =
        SagaDefinitionException.httpEndpointLookupFailed(
            "no HTTP endpoint registered under name 'billing'");

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED);
    assertThat(e.getMetadata())
        .containsEntry("detail", "no HTTP endpoint registered under name 'billing'")
        .hasSize(1);
  }

  // ── Cause-required guards ────────────────────────────────────────────

  @SuppressWarnings("NullAway")
  @Test
  void sourceUnreadable_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaDefinitionException.sourceUnreadable("f", null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void definitionMalformed_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaDefinitionException.definitionMalformed("JSON", null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void stepClassInvalid_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaDefinitionException.stepClassInvalid("Foo", "detail", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaDefinitionException.class);
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaDefinitionException.class);
  }

  @Test
  void fromWire_definitionCodeGiven_reconstructsThatCode() {
    // Act
    Map<String, String> wire = Map.of("saga_name", "orders", "detail", "duplicate step 'debit'");
    SagaDefinitionException e =
        SagaDefinitionException.fromWire(SagaErrorCode.INVALID_DEFINITION, wire);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_DEFINITION);
    assertThat(e.getMetadata()).containsEntry("saga_name", "orders");
  }

  @Test
  void fromWire_unrelatedCodeGiven_throwsIllegalState() {
    // A code this type does not represent is a registry wiring bug, not caller error.
    assertThatThrownBy(
            () ->
                SagaDefinitionException.fromWire(
                    SagaErrorCode.SAGA_NOT_FOUND, Collections.emptyMap()))
        .isInstanceOf(IllegalStateException.class);
  }
}
