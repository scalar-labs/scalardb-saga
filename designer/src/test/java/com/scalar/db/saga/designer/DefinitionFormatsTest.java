package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class DefinitionFormatsTest {

  @Test
  void ofFileName_definitionExtensionsGiven_returnsTheirFormat() {
    // Act Assert
    assertThat(Format.ofFileName("a.json")).isEqualTo(Format.JSON);
    assertThat(Format.ofFileName("a.YAML")).isEqualTo(Format.YAML);
    assertThat(Format.ofFileName("a.yml")).isEqualTo(Format.YAML);
  }

  @Test
  void ofFileName_otherExtensionGiven_throwsIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(() -> Format.ofFileName("a.txt"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void extension_eachFormat_returnsTheExtensionNewFilesGet() {
    // Act Assert
    assertThat(Format.JSON.extension()).isEqualTo(".json");
    assertThat(Format.YAML.extension()).isEqualTo(".yaml");
  }

  @Test
  void read_decimalsGiven_keepsTrailingZerosInBothFormats() throws Exception {
    // Act
    JsonNode json = DefinitionFormats.read("{\"a\": 100.50}", Format.JSON);
    JsonNode yaml = DefinitionFormats.read("a: 100.50\n", Format.YAML);

    // Assert
    assertThat(json.get("a").asText()).isEqualTo("100.50");
    assertThat(yaml.get("a").asText()).isEqualTo("100.50");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "  ", "{\"a\": 1} {\"b\": 2}", "{\"a\": "})
  void read_malformedJsonGiven_throwsJsonProcessingException(String text) {
    // Act Assert
    assertThatThrownBy(() -> DefinitionFormats.read(text, Format.JSON))
        .isInstanceOf(JsonProcessingException.class);
  }

  @Test
  void read_malformedYamlGiven_throwsJsonProcessingException() {
    // Act Assert
    assertThatThrownBy(() -> DefinitionFormats.read("a: [1, 2\n", Format.YAML))
        .isInstanceOf(JsonProcessingException.class);
  }

  @Test
  void write_json_indentsByTwoSpacesAndEndsWithANewline() throws Exception {
    // Arrange
    JsonNode tree = DefinitionFormats.read("{\"a\": {\"b\": [1]}}", Format.JSON);

    // Act
    String text = DefinitionFormats.write(tree, Format.JSON);

    // Assert
    assertThat(text).isEqualTo("{\n  \"a\" : {\n    \"b\" : [\n      1\n    ]\n  }\n}\n");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1.10",
        "n",
        "y",
        "on",
        "off",
        "1",
        "true",
        "null",
        "yes",
        "no",
        "~",
        "",
        "${orderId}",
        "a: b",
        "# x",
        "- a",
        "[1]",
        "{a}",
        "line\nbreak",
        " padded ",
        "0x1F",
        "1e3",
        "@at",
        "`tick`"
      })
  void write_yamlStringsThatLookLikeOtherValues_readBackAsTheSameString(String value)
      throws Exception {
    // Arrange
    ObjectNode tree = DefinitionFormats.json().createObjectNode();
    tree.put("version", value);

    // Act
    String yaml = DefinitionFormats.write(tree, Format.YAML);

    // Assert
    JsonNode back = DefinitionFormats.read(yaml, Format.YAML);
    assertThat(back.get("version").isTextual()).as(yaml).isTrue();
    assertThat(back.get("version").asText()).isEqualTo(value);
  }

  @Test
  void write_yaml_hasNoDocumentStartMarker() throws Exception {
    // Arrange
    JsonNode tree = DefinitionFormats.read("{\"name\": \"order\"}", Format.JSON);

    // Act Assert
    assertThat(DefinitionFormats.write(tree, Format.YAML)).isEqualTo("name: order\n");
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "{\"a\": 100.5}|{\"a\": 100.50}",
        "{\"a\": 1, \"b\": 2}|{\"b\": 2, \"a\": 1}",
        "[1, 2]|[2, 1]",
        "{\"a\": \"1\"}|{\"a\": 1}",
        "{\"a\": 1}|{\"a\": 1, \"b\": null}"
      })
  void identical_treesThatWriteDifferentlyGiven_returnsFalse(String left, String right)
      throws Exception {
    // Act Assert
    assertThat(
            DefinitionFormats.identical(
                DefinitionFormats.read(left, Format.JSON),
                DefinitionFormats.read(right, Format.JSON)))
        .isFalse();
  }

  @Test
  void identical_treesThatWriteTheSameGiven_returnsTrue() throws Exception {
    // Arrange
    String text = "{\"a\": [1, 100.50, \"x\", true, null], \"b\": {\"c\": 0.00000001}}";

    // Act Assert
    assertThat(
            DefinitionFormats.identical(
                DefinitionFormats.read(text, Format.JSON),
                DefinitionFormats.read(text.replace(" ", ""), Format.YAML)))
        .isTrue();
  }

  @Test
  void numberText_decimalsGiven_keepsTheirScaleInPlainNotation() throws Exception {
    // Arrange
    JsonNode tree = DefinitionFormats.read("[100.50, 1E-8, 12345678901234567890]", Format.JSON);

    // Act Assert
    assertThat(DefinitionFormats.numberText(tree.get(0))).isEqualTo("100.50");
    assertThat(DefinitionFormats.numberText(tree.get(1))).isEqualTo("0.00000001");
    assertThat(DefinitionFormats.numberText(tree.get(2))).isEqualTo("12345678901234567890");
  }
}
