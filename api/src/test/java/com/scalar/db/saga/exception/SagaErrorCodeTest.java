package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class SagaErrorCodeTest {

  /** Matches the documented format {@code DB-SAGA-<CATEGORY-1-digit><4-digit-id>}. */
  private static final Pattern CODE_FORMAT = Pattern.compile("^DB-SAGA-[1-9]\\d{4}$");

  @Test
  void values_every_hasNonBlankFields() {
    // Assert — each constant carries the six fields the docs generator + logs need
    for (SagaErrorCode code : SagaErrorCode.values()) {
      assertThat(code.code()).as("code string of %s", code.name()).isNotBlank();
      assertThat(code.category()).as("category of %s", code.name()).isNotNull();
      assertThat(code.message()).as("message of %s", code.name()).isNotBlank();
      assertThat(code.schema()).as("schema of %s", code.name()).isNotNull();
      assertThat(code.cause()).as("cause of %s", code.name()).isNotBlank();
      assertThat(code.action()).as("action of %s", code.name()).isNotBlank();
    }
  }

  @Test
  void code_every_matchesDocumentedFormat() {
    // Assert — DB-SAGA-<category-digit><4-digit-id>
    for (SagaErrorCode code : SagaErrorCode.values()) {
      assertThat(code.code()).as("code string of %s", code.name()).matches(CODE_FORMAT);
    }
  }

  @Test
  void code_every_startsWithItsCategoryDigit() {
    // Assert — the first digit after DB-SAGA- must match the category's declared id
    for (SagaErrorCode code : SagaErrorCode.values()) {
      String digit = code.code().substring("DB-SAGA-".length(), "DB-SAGA-".length() + 1);
      assertThat(digit)
          .as("category digit of %s (%s)", code.name(), code.code())
          .isEqualTo(code.category().id());
    }
  }

  @Test
  void code_every_isUnique() {
    // Assert — no duplicate wire codes across the enum
    Set<String> seen = new HashSet<>();
    for (SagaErrorCode code : SagaErrorCode.values()) {
      assertThat(seen.add(code.code())).as("duplicate wire code: %s", code.code()).isTrue();
    }
  }

  @Test
  void fromCode_knownCodeGiven_roundTripsToTheSameConstant() {
    // Assert — fromCode(c.code()) returns c for every enum constant
    for (SagaErrorCode code : SagaErrorCode.values()) {
      assertThat(SagaErrorCode.fromCode(code.code())).contains(code);
    }
  }

  @Test
  void fromCode_unknownCodeGiven_returnsEmpty() {
    // Arrange & Act & Assert
    assertThat(SagaErrorCode.fromCode("DB-SAGA-99999")).isEqualTo(Optional.empty());
    assertThat(SagaErrorCode.fromCode("nonsense")).isEqualTo(Optional.empty());
  }

  @Test
  void buildMessage_schemalessCodeGiven_rendersCodeAndMessageOnly() {
    // Arrange
    SagaErrorCode code = SagaErrorCode.INTERNAL_ERROR;

    // Act
    String rendered = code.buildMessage(Collections.emptyMap());

    // Assert
    assertThat(rendered).isEqualTo("DB-SAGA-30099: Internal error");
  }

  @Test
  void buildMessage_singleKeyMetadataGiven_rendersInSchemaOrder() {
    // Arrange
    SagaErrorCode code = SagaErrorCode.SAGA_NOT_FOUND;

    // Act
    String rendered = code.buildMessage(Collections.singletonMap("saga_id", "s-1"));

    // Assert
    assertThat(rendered).isEqualTo("DB-SAGA-11000: Saga not found [saga_id=s-1]");
  }

  @Test
  void buildMessage_multiKeyMetadataGiven_rendersInSchemaOrderNotMapOrder() {
    // Arrange — insertion order deliberately reverses schema order to prove ordering isn't from
    // the map. The schema declares (saga_name, step_name); the map here inserts step_name first.
    SagaErrorCode code = SagaErrorCode.DEFINITION_DUPLICATE_STEP_NAME;
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("step_name", "debit");
    metadata.put("saga_name", "orders");

    // Act
    String rendered = code.buildMessage(metadata);

    // Assert — saga_name comes first because that's the declared schema order
    assertThat(rendered)
        .isEqualTo("DB-SAGA-10005: Duplicate step name [saga_name=orders, step_name=debit]");
  }

  @Test
  void buildMessage_threeKeyMetadataGiven_rendersInSchemaOrder() {
    // Arrange
    SagaErrorCode code = SagaErrorCode.SAGA_WRONG_STATE;
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_id", "s-1");
    metadata.put("current_state", "COMPENSATING");
    metadata.put("requested_operation", "resume");

    // Act
    String rendered = code.buildMessage(metadata);

    // Assert
    assertThat(rendered)
        .isEqualTo(
            "DB-SAGA-11200: Operation not allowed in the saga's current state "
                + "[saga_id=s-1, current_state=COMPENSATING, requested_operation=resume]");
  }

  @Test
  void category_id_matchesDocumentedDigits() {
    // Assert — pin the wire discriminator so a future rename doesn't silently shift codes
    assertThat(SagaErrorCode.Category.USER_ERROR.id()).isEqualTo("1");
    assertThat(SagaErrorCode.Category.RETRYABLE_SERVER_ERROR.id()).isEqualTo("2");
    assertThat(SagaErrorCode.Category.NON_RETRYABLE_SERVER_ERROR.id()).isEqualTo("3");
    assertThat(SagaErrorCode.Category.CLIENT_ERROR.id()).isEqualTo("4");
  }
}
