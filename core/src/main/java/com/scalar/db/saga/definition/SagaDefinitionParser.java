package com.scalar.db.saga.definition;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.scalar.db.saga.definition.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.definition.SagaDefinition.SagaMode;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
      throw SagaDefinitionException.sourceUnreadable(path.toString());
    }
    String fileName = fileNamePath.toString().toLowerCase(java.util.Locale.ROOT);
    ObjectMapper mapper = resolveMapper(fileName);
    try (InputStream in = Files.newInputStream(path)) {
      return parse(mapper, in);
    } catch (JsonProcessingException e) {
      // Before the IOException arm, which it extends: a syntactically malformed file is a
      // malformed definition, not an unreadable source.
      throw SagaDefinitionException.definitionMalformed(formatOf(fileName), path.toString(), e);
    } catch (IOException e) {
      throw SagaDefinitionException.sourceUnreadable(path.toString(), e);
    }
  }

  /**
   * Parses a saga definition from a classpath resource.
   *
   * @throws SagaDefinitionException if the resource cannot be parsed or fails validation
   */
  public static SagaDefinition parseResource(String resourcePath) {
    String lowerPath = resourcePath.toLowerCase(java.util.Locale.ROOT);
    ObjectMapper mapper = resolveMapper(lowerPath);
    try (InputStream in =
        SagaDefinitionParser.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw SagaDefinitionException.sourceUnreadable(resourcePath);
      }
      return parse(mapper, in);
    } catch (JsonProcessingException e) {
      // Before the IOException arm, which it extends; see parseFile.
      throw SagaDefinitionException.definitionMalformed(formatOf(lowerPath), resourcePath, e);
    } catch (IOException e) {
      throw SagaDefinitionException.sourceUnreadable(resourcePath, e);
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
      throw SagaDefinitionException.definitionMalformed("JSON", e);
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
      throw SagaDefinitionException.definitionMalformed("YAML", e);
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
    String name = requireText(root, "", "name");
    SagaMode mode = parseEnum(root, name, "mode", SagaMode.class, SagaMode.SAGA);
    if (!root.has("steps") || !root.get("steps").isArray()) {
      throw SagaDefinitionException.definitionInvalid(name, "must have a 'steps' array");
    }
    checkUnknownFields(root, name);
    return mode == SagaMode.TCC ? buildTcc(name, root) : buildSaga(name, root);
  }

  private static SagaDefinition buildSaga(String name, JsonNode root) {
    SagaDefinition.SagaBuilder builder = SagaDefinition.newBuilder(name).saga();
    applyCommon(builder, root);
    if (isPresent(root, "recoveryStrategy")) {
      builder.recoveryStrategy(
          parseEnum(root, name, "recoveryStrategy", RecoveryStrategy.class, null));
    }
    for (JsonNode stepNode : root.get("steps")) {
      addSagaStep(builder, stepNode);
    }
    return builder.build();
  }

  private static SagaDefinition buildTcc(String name, JsonNode root) {
    if (isPresent(root, "recoveryStrategy")) {
      throw SagaDefinitionException.definitionInvalid(
          name,
          "TCC definition must not specify 'recoveryStrategy' — recovery is predefined (the cancel"
              + " phase)");
    }
    SagaDefinition.TccBuilder builder = SagaDefinition.newBuilder(name).tcc();
    applyCommon(builder, root);
    for (JsonNode stepNode : root.get("steps")) {
      addTccStep(name, builder, stepNode);
    }
    return builder.build();
  }

  private static void applyCommon(SagaDefinition.AbstractSagaBuilder<?> builder, JsonNode root) {
    if (isPresent(root, "version")) {
      builder.version(root.get("version").asText());
    }
    if (isPresent(root, "timeoutMillis")) {
      builder.timeoutMillis(root.get("timeoutMillis").asLong());
    }
    if (isPresent(root, "defaultRetryPolicy")) {
      builder.defaultRetryPolicy(parseRetryPolicy(root.get("defaultRetryPolicy")));
    }
    if (isPresent(root, "disabled")) {
      builder.disabled(root.get("disabled").asBoolean());
    }
  }

  private static void addSagaStep(SagaDefinition.SagaBuilder builder, JsonNode stepNode) {
    String stepName = requireText(stepNode, "", "name");
    String stepClass =
        CallSpecCodec.classStepOrNull(
            stepNode,
            stepName,
            msg -> SagaDefinitionException.declarativeStepInvalid(stepName, msg));
    if (stepClass != null) {
      SagaDefinition.SagaClassStepBuilder sb = builder.step(stepName, stepClass);
      applyStepCommon(sb, stepNode);
      if (isPresent(stepNode, "pivot")) {
        sb.pivot(stepNode.get("pivot").asBoolean());
      }
      sb.add();
      return;
    }
    String service = stepNode.get("service").asText();
    CallSpecCodec.requireSagaPhases(
        stepNode, stepName, msg -> SagaDefinitionException.declarativeStepInvalid(stepName, msg));
    CallSpecCodec.rejectAsyncOnBackwardPhase(
        stepNode, stepName, msg -> SagaDefinitionException.declarativeStepInvalid(stepName, msg));
    CallSpec.Transport transport = CallSpecCodec.parseTransport(stepNode, stepName);
    SagaDefinition.DeclarativeStepBuilder sb =
        builder
            .serviceStep(stepName, service)
            .execution(CallSpecCodec.parseCallSpec(transport, stepNode.get("execution"), stepName))
            .compensation(
                CallSpecCodec.parseCallSpec(transport, stepNode.get("compensation"), stepName));
    applyStepCommon(sb, stepNode);
    if (isPresent(stepNode, "pivot")) {
      sb.pivot(stepNode.get("pivot").asBoolean());
    }
    sb.add();
  }

  private static void addTccStep(
      String sagaName, SagaDefinition.TccBuilder builder, JsonNode stepNode) {
    String stepName = requireText(stepNode, sagaName, "name");
    if (isPresent(stepNode, "pivot")) {
      throw SagaDefinitionException.definitionInvalid(
          sagaName,
          "TCC step '" + stepName + "' must not specify 'pivot' — TCC recovery is predefined");
    }
    String stepClass =
        CallSpecCodec.classStepOrNull(
            stepNode,
            stepName,
            msg -> SagaDefinitionException.declarativeStepInvalid(stepName, msg));
    if (stepClass != null) {
      SagaDefinition.TccClassStepBuilder sb = builder.step(stepName, stepClass);
      applyStepCommon(sb, stepNode);
      sb.add();
      return;
    }
    String service = stepNode.get("service").asText();
    CallSpecCodec.requireTccPhases(
        stepNode, stepName, msg -> SagaDefinitionException.declarativeStepInvalid(stepName, msg));
    CallSpecCodec.rejectAsyncOnBackwardPhase(
        stepNode, stepName, msg -> SagaDefinitionException.declarativeStepInvalid(stepName, msg));
    CallSpec.Transport transport = CallSpecCodec.parseTransport(stepNode, stepName);
    SagaDefinition.TccDeclarativeStepBuilder sb =
        builder
            .serviceStep(stepName, service)
            .reservation(
                CallSpecCodec.parseCallSpec(transport, stepNode.get("reservation"), stepName))
            .confirmation(
                CallSpecCodec.parseCallSpec(transport, stepNode.get("confirmation"), stepName))
            .cancellation(
                CallSpecCodec.parseCallSpec(transport, stepNode.get("cancellation"), stepName));
    applyStepCommon(sb, stepNode);
    sb.add();
  }

  private static void applyStepCommon(
      SagaDefinition.AbstractStepBuilder<?, ?> stepBuilder, JsonNode stepNode) {
    if (isPresent(stepNode, "timeoutMillis")) {
      stepBuilder.timeoutMillis(stepNode.get("timeoutMillis").asLong());
    }
    if (isPresent(stepNode, "retryPolicy")) {
      stepBuilder.retryPolicy(parseRetryPolicy(stepNode.get("retryPolicy")));
    }
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

  private static String requireText(JsonNode node, String definitionName, String field) {
    if (!node.has(field) || !node.get(field).isTextual()) {
      throw SagaDefinitionException.definitionInvalid(
          definitionName, "missing required field: '" + field + "'");
    }
    return node.get(field).asText();
  }

  private static <T extends Enum<T>> T parseEnum(
      JsonNode node,
      String definitionName,
      String field,
      Class<T> enumClass,
      @Nullable T defaultValue) {
    if (!node.has(field)) {
      if (defaultValue != null) {
        return defaultValue;
      }
      throw SagaDefinitionException.definitionInvalid(
          definitionName, "missing required field: '" + field + "'");
    }
    String value = node.get(field).asText();
    try {
      return Enum.valueOf(enumClass, value);
    } catch (IllegalArgumentException e) {
      throw SagaDefinitionException.definitionInvalid(
          definitionName, "invalid value '" + value + "' for field '" + field + "'");
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
            "disabled",
            "steps");
    Set<String> stepKnown =
        Set.of(
            "name",
            "stepClass",
            "service",
            "transport",
            "execution",
            "compensation",
            "reservation",
            "confirmation",
            "cancellation",
            "timeoutMillis",
            "retryPolicy",
            "pivot");

    root.fieldNames()
        .forEachRemaining(
            f -> {
              if (!known.contains(f)) {
                throw SagaDefinitionException.definitionInvalid(
                    definitionName, "unknown field '" + f + "' in definition");
              }
            });

    if (root.has("steps")) {
      for (JsonNode stepNode : root.get("steps")) {
        stepNode
            .fieldNames()
            .forEachRemaining(
                f -> {
                  if (!stepKnown.contains(f)) {
                    throw SagaDefinitionException.definitionInvalid(
                        definitionName, "unknown field '" + f + "' in step");
                  }
                });
      }
    }
  }

  /** The format label for a path {@link #resolveMapper} accepted; used in failure metadata. */
  private static String formatOf(String lowerFileNameOrPath) {
    return lowerFileNameOrPath.endsWith(".json") ? "JSON" : "YAML";
  }

  private static ObjectMapper resolveMapper(String fileNameOrPath) {
    if (fileNameOrPath.endsWith(".yaml") || fileNameOrPath.endsWith(".yml")) {
      return YAML_MAPPER;
    }
    if (fileNameOrPath.endsWith(".json")) {
      return JSON_MAPPER;
    }
    throw SagaDefinitionException.sourceUnreadable(fileNameOrPath);
  }

  private static ObjectMapper createMapper(@Nullable JsonFactory factory) {
    ObjectMapper mapper = factory != null ? new ObjectMapper(factory) : new ObjectMapper();
    // Defense in depth against polymorphic-deserialization gadgets (off by default in Jackson 2.x).
    mapper.deactivateDefaultTyping();
    return mapper;
  }
}
