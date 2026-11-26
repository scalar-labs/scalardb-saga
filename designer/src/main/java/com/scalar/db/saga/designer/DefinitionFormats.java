package com.scalar.db.saga.designer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * Reads and writes definition files in either format without changing a single number.
 *
 * <p>Both mappers read decimals as {@code BigDecimal} and keep their trailing zeros, so {@code
 * 100.50} stays {@code 100.50} through a read and a write. Jackson otherwise reads it as a double,
 * or strips the zero when building a tree node. They also write decimals in plain notation, so
 * {@code 0.00000001} never turns into {@code 1E-8}.
 */
final class DefinitionFormats {

  /** A definition file's format, chosen by its extension the same way the server chooses. */
  enum Format {
    JSON,
    YAML;

    /**
     * Returns the format a file name implies.
     *
     * @throws IllegalArgumentException for an extension other than .json, .yaml or .yml
     */
    static Format ofFileName(String fileName) {
      String lower = fileName.toLowerCase(Locale.ROOT);
      if (lower.endsWith(".json")) {
        return JSON;
      }
      if (lower.endsWith(".yaml") || lower.endsWith(".yml")) {
        return YAML;
      }
      throw new IllegalArgumentException("Not a definition file name: " + fileName);
    }

    /** Returns the extension a new file in this format gets, with its dot. */
    String extension() {
      return this == JSON ? ".json" : ".yaml";
    }
  }

  private static final ObjectMapper JSON_MAPPER = configure(JsonMapper.builder().build());

  // MINIMIZE_QUOTES alone would write the string "1.10" unquoted, which reads back as the number
  // 1.1; ALWAYS_QUOTE_NUMBERS_AS_STRINGS keeps any number-like string quoted. No "---" marker,
  // since a definition file is a single document.
  private static final ObjectMapper YAML_MAPPER =
      configure(
          new YAMLMapper(
              YAMLFactory.builder()
                  .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                  .enable(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS)
                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                  .build()));

  private static final DefaultPrettyPrinter JSON_PRINTER =
      new DefaultPrettyPrinter()
          .withObjectIndenter(new DefaultIndenter("  ", "\n"))
          .withArrayIndenter(new DefaultIndenter("  ", "\n"));

  private DefinitionFormats() {}

  private static ObjectMapper configure(ObjectMapper mapper) {
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    mapper.configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
    mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    mapper.enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
    mapper.deactivateDefaultTyping();
    return mapper;
  }

  /** The JSON mapper, also used for the designer's own API bodies. */
  static ObjectMapper json() {
    return JSON_MAPPER;
  }

  /**
   * Reads a document in the given format into a tree.
   *
   * @throws JsonProcessingException if the text is not well-formed, or holds more than one value
   */
  static JsonNode read(String text, Format format) throws JsonProcessingException {
    JsonNode node = mapper(format).readTree(text);
    // An empty document reads as a missing node; callers treat it like any other malformed file.
    if (node == null || node.isMissingNode()) {
      throw new JsonProcessingException("The document is empty") {
        private static final long serialVersionUID = 1L;
      };
    }
    return node;
  }

  /** Writes a tree in the given format: two-space indents, and a trailing newline. */
  static String write(JsonNode node, Format format) {
    try {
      if (format == Format.JSON) {
        return JSON_MAPPER.writer(JSON_PRINTER).writeValueAsString(node) + "\n";
      }
      return YAML_MAPPER.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      // A tree built in memory always serializes; failing here is a bug, not bad input.
      throw new IllegalStateException("Failed to write a definition tree", e);
    }
  }

  /**
   * The text of a number as a file would hold it: {@code 100.50} keeps its zero, and a small or
   * large decimal stays in plain notation rather than {@code 1E-8}.
   */
  static String numberText(JsonNode number) {
    return number.isBigDecimal() ? number.decimalValue().toPlainString() : number.asText();
  }

  /**
   * Whether two trees would be written as the same file. Stricter than {@link JsonNode#equals},
   * which treats {@code 100.5} and {@code 100.50} as equal and ignores the order of fields: either
   * difference is a real edit that has to be saved.
   */
  static boolean identical(JsonNode a, JsonNode b) {
    if (a.getNodeType() != b.getNodeType()) {
      return false;
    }
    if (a.isNumber()) {
      return numberText(a).equals(numberText(b));
    }
    if (a.isObject()) {
      if (a.size() != b.size()) {
        return false;
      }
      Iterator<Map.Entry<String, JsonNode>> left = a.properties().iterator();
      Iterator<Map.Entry<String, JsonNode>> right = b.properties().iterator();
      while (left.hasNext()) {
        Map.Entry<String, JsonNode> l = left.next();
        Map.Entry<String, JsonNode> r = right.next();
        if (!l.getKey().equals(r.getKey()) || !identical(l.getValue(), r.getValue())) {
          return false;
        }
      }
      return true;
    }
    if (a.isArray()) {
      if (a.size() != b.size()) {
        return false;
      }
      for (int i = 0; i < a.size(); i++) {
        if (!identical(a.get(i), b.get(i))) {
          return false;
        }
      }
      return true;
    }
    return a.equals(b);
  }

  private static ObjectMapper mapper(Format format) {
    return format == Format.JSON ? JSON_MAPPER : YAML_MAPPER;
  }
}
