package com.scalar.db.saga.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.exception.SagaPersistenceException;

/**
 * Serializes and deserializes {@link SagaDefinition} to/from JSON.
 *
 * <p>This is an infrastructure concern — it isolates the Jackson dependency from the API layer and
 * keeps {@link SagaDefinition} framework-agnostic.
 */
final class SagaDefinitionSerializer {

  // Root-level JSON keys
  private static final String NAME = "name";
  private static final String MODE = "mode";
  private static final String VERSION = "version";
  private static final String RECOVERY_STRATEGY = "recoveryStrategy";
  private static final String TIMEOUT_MILLIS = "timeoutMillis";
  private static final String DEFAULT_RETRY_POLICY = "defaultRetryPolicy";
  private static final String STEPS = "steps";

  // Step-level JSON keys
  private static final String STEP_CLASS = "stepClass";
  private static final String PIVOT = "pivot";
  private static final String RETRY_POLICY = "retryPolicy";

  // RetryPolicy JSON keys
  private static final String MAX_ATTEMPTS = "maxAttempts";
  private static final String INITIAL_INTERVAL_MILLIS = "initialIntervalMillis";
  private static final String BACKOFF_MULTIPLIER = "backoffMultiplier";
  private static final String MAX_INTERVAL_MILLIS = "maxIntervalMillis";

  private final ObjectMapper objectMapper;

  SagaDefinitionSerializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /**
   * Serializes a {@link SagaDefinition} to a JSON string.
   *
   * @param def the definition to serialize
   * @return the JSON representation
   */
  String serialize(SagaDefinition def) {
    ObjectNode root = objectMapper.createObjectNode();
    root.put(NAME, def.getName());
    root.put(VERSION, def.getVersion());
    root.put(MODE, def.getMode().name());
    root.put(RECOVERY_STRATEGY, def.getRecoveryStrategy().name());
    root.put(TIMEOUT_MILLIS, def.getTimeoutMillis());
    if (def.getDefaultRetryPolicy() != null) {
      root.set(DEFAULT_RETRY_POLICY, serializeRetryPolicy(def.getDefaultRetryPolicy()));
    }
    ArrayNode steps = root.putArray(STEPS);
    for (SagaDefinition.StepDefinition step : def.getSteps()) {
      ObjectNode s = steps.addObject();
      s.put(NAME, step.getName());
      s.put(STEP_CLASS, step.getStepClass());
      s.put(TIMEOUT_MILLIS, step.getTimeoutMillis());
      s.put(PIVOT, step.isPivot());
      if (step.getRetryPolicy() != null) {
        s.set(RETRY_POLICY, serializeRetryPolicy(step.getRetryPolicy()));
      }
    }
    return root.toString();
  }

  /**
   * Deserializes a JSON string to a {@link SagaDefinition}.
   *
   * @param json the JSON representation
   * @return the deserialized definition
   * @throws SagaPersistenceException if the JSON is malformed or missing required fields
   */
  SagaDefinition deserialize(String json) {
    try {
      JsonNode root = objectMapper.readTree(json);
      requireFields(root, NAME, MODE, VERSION, RECOVERY_STRATEGY, TIMEOUT_MILLIS, STEPS);

      SagaDefinition.Builder builder =
          SagaDefinition.newBuilder(root.get(NAME).asText(), parseEnum(root, MODE, SagaMode.class))
              .version(root.get(VERSION).asText())
              .recoveryStrategy(parseEnum(root, RECOVERY_STRATEGY, RecoveryStrategy.class))
              .timeoutMillis(root.get(TIMEOUT_MILLIS).asLong());

      if (root.has(DEFAULT_RETRY_POLICY) && !root.get(DEFAULT_RETRY_POLICY).isNull()) {
        builder.defaultRetryPolicy(deserializeRetryPolicy(root.get(DEFAULT_RETRY_POLICY)));
      }

      for (JsonNode stepNode : root.get(STEPS)) {
        requireFields(stepNode, NAME, STEP_CLASS, TIMEOUT_MILLIS, PIVOT);
        SagaDefinition.StepBuilder stepBuilder =
            builder.step(stepNode.get(NAME).asText(), stepNode.get(STEP_CLASS).asText());
        stepBuilder.timeoutMillis(stepNode.get(TIMEOUT_MILLIS).asLong());
        stepBuilder.pivot(stepNode.get(PIVOT).asBoolean());
        if (stepNode.has(RETRY_POLICY) && !stepNode.get(RETRY_POLICY).isNull()) {
          stepBuilder.retryPolicy(deserializeRetryPolicy(stepNode.get(RETRY_POLICY)));
        }
        stepBuilder.add();
      }

      return builder.build();
    } catch (JsonProcessingException | RuntimeException e) {
      throw new SagaPersistenceException("Failed to deserialize definition", e);
    }
  }

  private ObjectNode serializeRetryPolicy(RetryPolicy policy) {
    ObjectNode node = objectMapper.createObjectNode();
    node.put(MAX_ATTEMPTS, policy.getMaxAttempts());
    node.put(INITIAL_INTERVAL_MILLIS, policy.getInitialIntervalMillis());
    node.put(BACKOFF_MULTIPLIER, policy.getBackoffMultiplier());
    node.put(MAX_INTERVAL_MILLIS, policy.getMaxIntervalMillis());
    return node;
  }

  private RetryPolicy deserializeRetryPolicy(JsonNode node) {
    requireFields(
        node, MAX_ATTEMPTS, INITIAL_INTERVAL_MILLIS, BACKOFF_MULTIPLIER, MAX_INTERVAL_MILLIS);
    return RetryPolicy.newBuilder()
        .maxAttempts(node.get(MAX_ATTEMPTS).asInt())
        .initialIntervalMillis(node.get(INITIAL_INTERVAL_MILLIS).asLong())
        .backoffMultiplier(node.get(BACKOFF_MULTIPLIER).asDouble())
        .maxIntervalMillis(node.get(MAX_INTERVAL_MILLIS).asLong())
        .build();
  }

  private static void requireFields(JsonNode node, String... fieldNames) {
    for (String fieldName : fieldNames) {
      if (!node.has(fieldName) || node.get(fieldName).isNull()) {
        throw new IllegalArgumentException(
            "Missing required field '" + fieldName + "' in definition JSON");
      }
    }
  }

  private static <T extends Enum<T>> T parseEnum(
      JsonNode node, String fieldName, Class<T> enumType) {
    String value = node.get(fieldName).asText();
    for (T constant : enumType.getEnumConstants()) {
      if (constant.name().equals(value)) {
        return constant;
      }
    }
    throw new IllegalArgumentException(
        "Invalid value for field '" + fieldName + "' in definition JSON");
  }
}
