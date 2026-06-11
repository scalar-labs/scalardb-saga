package com.scalar.db.saga.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.scalar.db.saga.api.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Parses saga definitions from JSON or YAML. Detects format by file extension.
 *
 * <p>Unrecognized fields cause parsing to fail (validated by {@link #checkUnknownFields}).
 *
 * <p>Example JSON:
 *
 * <pre>{@code
 * {
 *   "name": "transferMoney",
 *   "mode": "SAGA",
 *   "version": "1.0",
 *   "recoveryStrategy": "BACKWARD",
 *   "timeoutMillis": 30000,
 *   "steps": [
 *     { "name": "debit", "stepClass": "com.example.DebitStep" },
 *     { "name": "credit", "stepClass": "com.example.CreditStep" }
 *   ]
 * }
 * }</pre>
 */
public final class SagaDefinitionParser {

  private static final ObjectMapper JSON_MAPPER = createMapper(null);
  private static final ObjectMapper YAML_MAPPER = createMapper(new YAMLFactory());

  private SagaDefinitionParser() {}

  /**
   * Parses a saga definition from a file. Detects JSON or YAML by extension ({@code .json} for
   * JSON; {@code .yaml} or {@code .yml} for YAML).
   *
   * @throws SagaDefinitionException if the file cannot be parsed or fails validation
   */
  public static SagaDefinition parseFile(Path path) {
    Path fileNamePath = path.getFileName();
    if (fileNamePath == null) {
      throw new SagaDefinitionException("Path has no file name: " + path);
    }
    String fileName = fileNamePath.toString().toLowerCase(java.util.Locale.ROOT);
    ObjectMapper mapper = resolveMapper(fileName);
    try (InputStream in = Files.newInputStream(path)) {
      return parse(mapper, in);
    } catch (IOException e) {
      throw new SagaDefinitionException("Failed to read definition file: " + path, e);
    }
  }

  /**
   * Parses a saga definition from a classpath resource.
   *
   * @throws SagaDefinitionException if the resource cannot be parsed or fails validation
   */
  public static SagaDefinition parseResource(String resourcePath) {
    ObjectMapper mapper = resolveMapper(resourcePath.toLowerCase(java.util.Locale.ROOT));
    try (InputStream in =
        SagaDefinitionParser.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new SagaDefinitionException("Resource not found on classpath: " + resourcePath);
      }
      return parse(mapper, in);
    } catch (IOException e) {
      throw new SagaDefinitionException("Failed to read resource: " + resourcePath, e);
    }
  }

  /**
   * Parses a saga definition from a JSON string.
   *
   * @throws SagaDefinitionException if the string cannot be parsed or fails validation
   */
  public static SagaDefinition parseJson(String json) {
    try {
      JsonNode root = JSON_MAPPER.readTree(json);
      return buildDefinition(root);
    } catch (IOException e) {
      throw new SagaDefinitionException("Failed to parse JSON definition", e);
    }
  }

  /**
   * Parses a saga definition from a YAML string.
   *
   * @throws SagaDefinitionException if the string cannot be parsed or fails validation
   */
  public static SagaDefinition parseYaml(String yaml) {
    try {
      JsonNode root = YAML_MAPPER.readTree(yaml);
      return buildDefinition(root);
    } catch (IOException e) {
      throw new SagaDefinitionException("Failed to parse YAML definition", e);
    }
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  private static SagaDefinition parse(ObjectMapper mapper, InputStream in) throws IOException {
    JsonNode root = mapper.readTree(in);
    return buildDefinition(root);
  }

  private static SagaDefinition buildDefinition(JsonNode root) {
    String name = requireText(root, "name");
    SagaMode mode = parseEnum(root, "mode", SagaMode.class, SagaMode.SAGA);

    SagaDefinition.Builder builder = SagaDefinition.newBuilder(name, mode);

    if (root.has("version") && !root.get("version").isNull()) {
      builder.version(root.get("version").asText());
    }
    if (root.has("recoveryStrategy") && !root.get("recoveryStrategy").isNull()) {
      builder.recoveryStrategy(parseEnum(root, "recoveryStrategy", RecoveryStrategy.class, null));
    }
    if (root.has("timeoutMillis") && !root.get("timeoutMillis").isNull()) {
      builder.timeoutMillis(root.get("timeoutMillis").asLong());
    }
    if (root.has("defaultRetryPolicy") && !root.get("defaultRetryPolicy").isNull()) {
      builder.defaultRetryPolicy(parseRetryPolicy(root.get("defaultRetryPolicy")));
    }

    if (!root.has("steps") || !root.get("steps").isArray()) {
      throw new SagaDefinitionException("Definition '" + name + "' must have a 'steps' array");
    }

    for (JsonNode stepNode : root.get("steps")) {
      String stepName = requireText(stepNode, "name");
      SagaDefinition.StepBuilder stepBuilder = newStepBuilder(builder, stepNode, stepName);

      if (stepNode.has("timeoutMillis") && !stepNode.get("timeoutMillis").isNull()) {
        stepBuilder.timeoutMillis(stepNode.get("timeoutMillis").asLong());
      }
      if (stepNode.has("retryPolicy") && !stepNode.get("retryPolicy").isNull()) {
        stepBuilder.retryPolicy(parseRetryPolicy(stepNode.get("retryPolicy")));
      }
      if (stepNode.has("pivot") && !stepNode.get("pivot").isNull()) {
        stepBuilder.pivot(stepNode.get("pivot").asBoolean());
      }
      stepBuilder.add();
    }

    checkUnknownFields(root, name);

    return builder.build();
  }

  private static SagaDefinition.StepBuilder newStepBuilder(
      SagaDefinition.Builder builder, JsonNode stepNode, String stepName) {
    boolean hasStepClass = isPresent(stepNode, "stepClass");
    boolean hasService = isPresent(stepNode, "service");
    boolean hasOperation = isPresent(stepNode, "operation");

    if (hasStepClass) {
      if (hasService || hasOperation) {
        throw new SagaDefinitionException(
            "Step '"
                + stepName
                + "' must define either 'stepClass' or 'service'/'operation', not both");
      }
      return builder.step(stepName, stepNode.get("stepClass").asText());
    }
    if (hasService || hasOperation) {
      if (!hasService) {
        throw new SagaDefinitionException(
            "Step '" + stepName + "' has 'operation' but is missing 'service'");
      }
      if (!hasOperation) {
        throw new SagaDefinitionException(
            "Step '" + stepName + "' has 'service' but is missing 'operation'");
      }
      return builder.serviceStep(
          stepName, stepNode.get("service").asText(), stepNode.get("operation").asText());
    }
    throw new SagaDefinitionException(
        "Step '" + stepName + "' must define one of: 'stepClass' or 'service'/'operation'");
  }

  private static boolean isPresent(JsonNode node, String field) {
    return node.has(field) && !node.get(field).isNull();
  }

  private static RetryPolicy parseRetryPolicy(JsonNode node) {
    RetryPolicy.Builder builder = RetryPolicy.newBuilder();
    if (node.has("maxAttempts")) {
      builder.maxAttempts(node.get("maxAttempts").asInt());
    }
    if (node.has("initialIntervalMillis")) {
      builder.initialIntervalMillis(node.get("initialIntervalMillis").asLong());
    }
    if (node.has("backoffMultiplier")) {
      builder.backoffMultiplier(node.get("backoffMultiplier").asDouble());
    }
    if (node.has("maxIntervalMillis")) {
      builder.maxIntervalMillis(node.get("maxIntervalMillis").asLong());
    }
    return builder.build();
  }

  private static String requireText(JsonNode node, String field) {
    if (!node.has(field) || !node.get(field).isTextual()) {
      throw new SagaDefinitionException("Missing or non-text field: '" + field + "'");
    }
    return node.get(field).asText();
  }

  private static <T extends Enum<T>> T parseEnum(
      JsonNode node, String field, Class<T> enumClass, @Nullable T defaultValue) {
    if (!node.has(field)) {
      if (defaultValue != null) {
        return defaultValue;
      }
      throw new SagaDefinitionException("Missing required field: '" + field + "'");
    }
    String value = node.get(field).asText();
    try {
      return Enum.valueOf(enumClass, value);
    } catch (IllegalArgumentException e) {
      throw new SagaDefinitionException(
          "Invalid value '"
              + value
              + "' for field '"
              + field
              + "'; expected one of: "
              + Arrays.toString(enumClass.getEnumConstants()));
    }
  }

  private static void checkUnknownFields(JsonNode root, String definitionName) {
    Set<String> known =
        Set.of(
            "name",
            "mode",
            "version",
            "recoveryStrategy",
            "timeoutMillis",
            "defaultRetryPolicy",
            "steps");
    Set<String> stepKnown =
        Set.of(
            "name", "stepClass", "service", "operation", "timeoutMillis", "retryPolicy", "pivot");

    root.fieldNames()
        .forEachRemaining(
            f -> {
              if (!known.contains(f)) {
                throw new SagaDefinitionException(
                    "Unknown field '" + f + "' in definition '" + definitionName + "'");
              }
            });

    if (root.has("steps")) {
      for (JsonNode stepNode : root.get("steps")) {
        stepNode
            .fieldNames()
            .forEachRemaining(
                f -> {
                  if (!stepKnown.contains(f)) {
                    throw new SagaDefinitionException(
                        "Unknown field '" + f + "' in step of definition '" + definitionName + "'");
                  }
                });
      }
    }
  }

  private static ObjectMapper resolveMapper(String fileNameOrPath) {
    if (fileNameOrPath.endsWith(".yaml") || fileNameOrPath.endsWith(".yml")) {
      return YAML_MAPPER;
    }
    if (fileNameOrPath.endsWith(".json")) {
      return JSON_MAPPER;
    }
    throw new SagaDefinitionException(
        "Unsupported file extension: '" + fileNameOrPath + "'; expected .json, .yaml, or .yml");
  }

  private static ObjectMapper createMapper(@Nullable JsonFactory factory) {
    ObjectMapper mapper = factory != null ? new ObjectMapper(factory) : new ObjectMapper();
    // Defense in depth against polymorphic-deserialization gadgets (off by default in Jackson 2.x).
    mapper.deactivateDefaultTyping();
    return mapper;
  }
}
