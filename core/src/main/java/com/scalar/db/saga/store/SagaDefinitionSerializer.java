package com.scalar.db.saga.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.definition.CallSpec.Transport;
import com.scalar.db.saga.definition.CallSpecCodec;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.ClassStep;
import com.scalar.db.saga.definition.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.definition.SagaDefinition.SagaMode;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.util.Locale;

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
  private static final String SERVICE = "service";
  private static final String TRANSPORT = "transport";
  private static final String EXECUTION = "execution";
  private static final String COMPENSATION = "compensation";
  private static final String RESERVATION = "reservation";
  private static final String CONFIRMATION = "confirmation";
  private static final String CANCELLATION = "cancellation";
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
    // TCC recovery (PREDEFINED) and step pivots are implicit and must not be re-specified, so they
    // are omitted for TCC; the deserializer rejects them if present, mirroring the parser.
    boolean tcc = def.getMode() == SagaMode.TCC;
    if (!tcc) {
      root.put(RECOVERY_STRATEGY, def.getRecoveryStrategy().name());
    }
    root.put(TIMEOUT_MILLIS, def.getTimeoutMillis());
    RetryPolicy defaultRetryPolicy = def.getDefaultRetryPolicy();
    if (defaultRetryPolicy != null) {
      root.set(DEFAULT_RETRY_POLICY, serializeRetryPolicy(defaultRetryPolicy));
    }
    ArrayNode steps = root.putArray(STEPS);
    for (SagaDefinition.StepDefinition step : def.getSteps()) {
      ObjectNode s = steps.addObject();
      s.put(NAME, step.getName());
      switch (step) {
        case ClassStep cs -> s.put(STEP_CLASS, cs.getStepClass());
        case ServiceStep ss -> {
          s.put(SERVICE, ss.getService());
          s.put(TRANSPORT, ss.getTransport().name());
          ss.getPhases()
              .forEach(
                  (phase, spec) ->
                      s.set(phaseKey(phase), CallSpecCodec.serializeCallSpec(objectMapper, spec)));
        }
      }
      s.put(TIMEOUT_MILLIS, step.getTimeoutMillis());
      if (!tcc) {
        s.put(PIVOT, step.isPivot());
      }
      RetryPolicy retryPolicy = step.getRetryPolicy();
      if (retryPolicy != null) {
        s.set(RETRY_POLICY, serializeRetryPolicy(retryPolicy));
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
      requireFields(root, NAME, MODE, VERSION, TIMEOUT_MILLIS, STEPS);
      SagaMode mode = parseEnum(root, MODE, SagaMode.class);
      return mode == SagaMode.TCC ? deserializeTcc(root) : deserializeSaga(root);
    } catch (JsonProcessingException | RuntimeException e) {
      throw SagaPersistenceException.nonRetryable("Failed to deserialize definition", e);
    }
  }

  private SagaDefinition deserializeSaga(JsonNode root) {
    requireFields(root, RECOVERY_STRATEGY);
    SagaDefinition.SagaBuilder builder =
        SagaDefinition.newBuilder(root.get(NAME).asText())
            .saga()
            .version(root.get(VERSION).asText())
            .recoveryStrategy(parseEnum(root, RECOVERY_STRATEGY, RecoveryStrategy.class))
            .timeoutMillis(root.get(TIMEOUT_MILLIS).asLong());
    applyDefaultRetry(builder, root);
    for (JsonNode stepNode : root.get(STEPS)) {
      addSagaStep(builder, stepNode);
    }
    return builder.build();
  }

  private SagaDefinition deserializeTcc(JsonNode root) {
    if (has(root, RECOVERY_STRATEGY)) {
      throw new IllegalArgumentException(
          "TCC definition must not specify '"
              + RECOVERY_STRATEGY
              + "' — recovery is predefined (the cancel phase)");
    }
    SagaDefinition.TccBuilder builder =
        SagaDefinition.newBuilder(root.get(NAME).asText())
            .tcc()
            .version(root.get(VERSION).asText())
            .timeoutMillis(root.get(TIMEOUT_MILLIS).asLong());
    applyDefaultRetry(builder, root);
    for (JsonNode stepNode : root.get(STEPS)) {
      addTccStep(builder, stepNode);
    }
    return builder.build();
  }

  private void applyDefaultRetry(SagaDefinition.AbstractSagaBuilder<?> builder, JsonNode root) {
    if (root.has(DEFAULT_RETRY_POLICY) && !root.get(DEFAULT_RETRY_POLICY).isNull()) {
      builder.defaultRetryPolicy(deserializeRetryPolicy(root.get(DEFAULT_RETRY_POLICY)));
    }
  }

  private void addSagaStep(SagaDefinition.SagaBuilder builder, JsonNode stepNode) {
    requireFields(stepNode, NAME, TIMEOUT_MILLIS, PIVOT);
    String name = stepNode.get(NAME).asText();
    String stepClass = CallSpecCodec.classStepOrNull(stepNode, name, IllegalArgumentException::new);
    if (stepClass != null) {
      SagaDefinition.SagaClassStepBuilder sb = builder.step(name, stepClass);
      applyStepCommon(sb, stepNode);
      sb.pivot(stepNode.get(PIVOT).asBoolean());
      sb.add();
      return;
    }
    String service = stepNode.get(SERVICE).asText();
    CallSpecCodec.requireSagaPhases(stepNode, name, IllegalArgumentException::new);
    Transport transport = CallSpecCodec.parseTransport(stepNode, name);
    SagaDefinition.DeclarativeStepBuilder sb =
        builder
            .serviceStep(name, service)
            .execution(CallSpecCodec.parseCallSpec(transport, stepNode.get(EXECUTION), name))
            .compensation(CallSpecCodec.parseCallSpec(transport, stepNode.get(COMPENSATION), name));
    applyStepCommon(sb, stepNode);
    sb.pivot(stepNode.get(PIVOT).asBoolean());
    sb.add();
  }

  private void addTccStep(SagaDefinition.TccBuilder builder, JsonNode stepNode) {
    requireFields(stepNode, NAME, TIMEOUT_MILLIS);
    String name = stepNode.get(NAME).asText();
    if (has(stepNode, PIVOT)) {
      throw new IllegalArgumentException(
          "TCC step '"
              + name
              + "' must not specify '"
              + PIVOT
              + "' — TCC recovery is predefined (cancel-based), so the pivot is fixed at the last"
              + " try step");
    }
    String stepClass = CallSpecCodec.classStepOrNull(stepNode, name, IllegalArgumentException::new);
    if (stepClass != null) {
      SagaDefinition.TccClassStepBuilder sb = builder.step(name, stepClass);
      applyStepCommon(sb, stepNode);
      sb.add();
      return;
    }
    String service = stepNode.get(SERVICE).asText();
    CallSpecCodec.requireTccPhases(stepNode, name, IllegalArgumentException::new);
    Transport transport = CallSpecCodec.parseTransport(stepNode, name);
    SagaDefinition.TccDeclarativeStepBuilder sb =
        builder
            .serviceStep(name, service)
            .reservation(CallSpecCodec.parseCallSpec(transport, stepNode.get(RESERVATION), name))
            .confirmation(CallSpecCodec.parseCallSpec(transport, stepNode.get(CONFIRMATION), name))
            .cancellation(CallSpecCodec.parseCallSpec(transport, stepNode.get(CANCELLATION), name));
    applyStepCommon(sb, stepNode);
    sb.add();
  }

  private void applyStepCommon(
      SagaDefinition.AbstractStepBuilder<?, ?> stepBuilder, JsonNode stepNode) {
    stepBuilder.timeoutMillis(stepNode.get(TIMEOUT_MILLIS).asLong());
    if (stepNode.has(RETRY_POLICY) && !stepNode.get(RETRY_POLICY).isNull()) {
      stepBuilder.retryPolicy(deserializeRetryPolicy(stepNode.get(RETRY_POLICY)));
    }
  }

  private static String phaseKey(Phase phase) {
    return phase.name().toLowerCase(Locale.ROOT);
  }

  private static boolean has(JsonNode node, String field) {
    return node.has(field) && !node.get(field).isNull();
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
