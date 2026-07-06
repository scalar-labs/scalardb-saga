package com.scalar.db.saga.definition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * The single JSON (de)serializer for a declaratively-defined service step's {@link CallSpec} (Layer
 * 2b), used by both {@link SagaDefinitionParser} (user-authored YAML/JSON) and the store's
 * definition serializer (round-trip persistence) so the two parsing paths cannot drift. Reads a
 * Jackson {@link JsonNode} into a {@link CallSpec} and writes a {@link CallSpec} back to an {@link
 * ObjectNode}; the two directions are inverses, so adding a transport or field is a one-class
 * change.
 *
 * <p>Throws {@link SagaDefinitionException} on a malformed spec; each caller adapts that to its own
 * error contract (the store wraps it as a persistence error).
 *
 * <p>This type is {@code public} solely for cross-package access within the module (the store
 * serializer lives in another package); it is not part of the user-facing API.
 */
public final class CallSpecCodec {

  private static final String TRANSPORT = "transport";
  private static final String METHOD = "method";
  private static final String PATH = "path";
  private static final String QUERY = "query";
  private static final String JSON_BODY = "jsonBody";
  private static final String STRING_BODY = "stringBody";
  private static final String CONTENT_TYPE = "contentType";
  private static final String OUTPUT = "output";
  private static final String ASYNC = "async";
  private static final String CALLBACK_TIMEOUT_MILLIS = "callbackTimeoutMillis";
  private static final String STEP_CLASS = "stepClass";
  private static final String SERVICE = "service";
  private static final String EXECUTION = "execution";
  private static final String COMPENSATION = "compensation";
  private static final String RESERVATION = "reservation";
  private static final String CONFIRMATION = "confirmation";
  private static final String CANCELLATION = "cancellation";

  // The HTTP call spec's own keys. The values of QUERY/JSON_BODY/OUTPUT are free-form user maps and
  // are not validated here.
  private static final Set<String> KNOWN_HTTP_FIELDS =
      Set.of(
          METHOD,
          PATH,
          QUERY,
          JSON_BODY,
          STRING_BODY,
          CONTENT_TYPE,
          OUTPUT,
          ASYNC,
          CALLBACK_TIMEOUT_MILLIS);

  private CallSpecCodec() {}

  /**
   * Reads the {@code transport} field of a declaratively-defined service step node, defaulting to
   * {@link CallSpec.Transport#HTTP} when absent. Rejects an unknown or not-yet-supported transport.
   */
  public static CallSpec.Transport parseTransport(JsonNode stepNode, String stepName) {
    if (!isPresent(stepNode, TRANSPORT)) {
      return CallSpec.Transport.HTTP;
    }
    String value = stepNode.get(TRANSPORT).asText();
    CallSpec.Transport transport;
    try {
      transport = CallSpec.Transport.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new SagaDefinitionException(
          "Declarative service step '"
              + stepName
              + "' has invalid transport '"
              + value
              + "'; expected one of: "
              + Arrays.toString(CallSpec.Transport.values()));
    }
    if (transport != CallSpec.Transport.HTTP) {
      throw new SagaDefinitionException(
          "Declarative service step '"
              + stepName
              + "' transport '"
              + transport
              + "' is not yet supported");
    }
    return transport;
  }

  /** Reads one phase's {@link CallSpec} for {@code transport} from its JSON node. */
  public static CallSpec parseCallSpec(
      CallSpec.Transport transport, JsonNode node, String stepName) {
    // Only HTTP is supported today; parseTransport rejects anything else first.
    if (transport != CallSpec.Transport.HTTP) {
      throw new SagaDefinitionException(
          "Declarative service step '"
              + stepName
              + "' transport '"
              + transport
              + "' is not yet supported");
    }
    node.fieldNames()
        .forEachRemaining(
            field -> {
              if (!KNOWN_HTTP_FIELDS.contains(field)) {
                throw new SagaDefinitionException(
                    "Declarative service step '"
                        + stepName
                        + "' call spec has unknown field '"
                        + field
                        + "'; expected one of: "
                        + KNOWN_HTTP_FIELDS);
              }
            });
    if (!isPresent(node, PATH)) {
      throw new SagaDefinitionException(
          "Declarative service step '" + stepName + "' call spec is missing 'path'");
    }
    HttpCall.Builder callBuilder = HttpCall.newBuilder(node.get(PATH).asText());
    if (isPresent(node, METHOD)) {
      String method = node.get(METHOD).asText();
      try {
        callBuilder.method(HttpMethod.valueOf(method));
      } catch (IllegalArgumentException e) {
        throw new SagaDefinitionException(
            "Declarative service step '"
                + stepName
                + "' has invalid HTTP method '"
                + method
                + "'; expected one of: "
                + Arrays.toString(HttpMethod.values()));
      }
    }
    if (isPresent(node, QUERY)) {
      callBuilder.query(readStringMap(node.get(QUERY), stepName, QUERY));
    }
    if (isPresent(node, JSON_BODY)) {
      callBuilder.jsonBody(readStringMap(node.get(JSON_BODY), stepName, JSON_BODY));
    }
    if (isPresent(node, STRING_BODY)) {
      callBuilder.stringBody(node.get(STRING_BODY).asText());
    }
    if (isPresent(node, CONTENT_TYPE)) {
      callBuilder.contentType(node.get(CONTENT_TYPE).asText());
    }
    if (isPresent(node, OUTPUT)) {
      callBuilder.output(readStringMap(node.get(OUTPUT), stepName, OUTPUT));
    }
    if (isPresent(node, ASYNC)) {
      callBuilder.async(node.get(ASYNC).asBoolean());
    }
    if (isPresent(node, CALLBACK_TIMEOUT_MILLIS)) {
      callBuilder.callbackTimeoutMillis(node.get(CALLBACK_TIMEOUT_MILLIS).asLong());
    }
    try {
      return callBuilder.build();
    } catch (IllegalStateException e) {
      // e.g. a body-less verb (GET/DELETE) that declares a request body — a definition error.
      throw new SagaDefinitionException(
          "Declarative service step '"
              + stepName
              + "' has an invalid call spec: "
              + e.getMessage());
    }
  }

  /** Writes {@code spec} as a JSON object — the inverse of {@link #parseCallSpec}. */
  public static ObjectNode serializeCallSpec(ObjectMapper mapper, CallSpec spec) {
    ObjectNode node = mapper.createObjectNode();
    switch (spec) {
      case HttpCall http -> {
        node.put(METHOD, http.getMethod().name());
        node.put(PATH, http.getPath());
        if (!http.getQuery().isEmpty()) {
          node.set(QUERY, toJsonMap(mapper, http.getQuery()));
        }
        if (!http.getJsonBody().isEmpty()) {
          node.set(JSON_BODY, toJsonMap(mapper, http.getJsonBody()));
        }
        if (http.getStringBody() != null) {
          node.put(STRING_BODY, http.getStringBody());
        }
        if (http.getContentType() != null) {
          node.put(CONTENT_TYPE, http.getContentType());
        }
        if (!http.getOutput().isEmpty()) {
          node.set(OUTPUT, toJsonMap(mapper, http.getOutput()));
        }
        if (http.isAsync()) {
          node.put(ASYNC, true);
        }
        if (http.callbackTimeoutMillis() > 0) {
          node.put(CALLBACK_TIMEOUT_MILLIS, http.callbackTimeoutMillis());
        }
      }
    }
    return node;
  }

  private static ObjectNode toJsonMap(ObjectMapper mapper, Map<String, String> map) {
    ObjectNode node = mapper.createObjectNode();
    map.forEach(node::put);
    return node;
  }

  private static Map<String, String> readStringMap(JsonNode node, String stepName, String field) {
    if (!node.isObject()) {
      throw new SagaDefinitionException(
          "Declarative service step '"
              + stepName
              + "' field '"
              + field
              + "' must be a JSON object");
    }
    Map<String, String> map = new LinkedHashMap<>();
    node.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
    return map;
  }

  private static boolean isPresent(JsonNode node, String field) {
    return node.has(field) && !node.get(field).isNull();
  }

  /**
   * Returns the {@code stepClass} for a class step, or {@code null} for a (validated) service step,
   * rejecting a step that mixes a {@code stepClass} with {@code service}/phases or defines neither.
   * Shared by {@link SagaDefinitionParser} and the store serializer; each passes {@code error} so
   * the failure type matches its layer (a public definition error vs an internal one).
   */
  public static @Nullable String classStepOrNull(
      JsonNode stepNode, String name, Function<String, RuntimeException> error) {
    if (isPresent(stepNode, STEP_CLASS)) {
      if (isPresent(stepNode, SERVICE) || hasSagaPhase(stepNode) || hasTccPhase(stepNode)) {
        throw error.apply(
            "Step '" + name + "' must not mix 'stepClass' with 'service'/declarative phases");
      }
      return stepNode.get(STEP_CLASS).asText();
    }
    if (!isPresent(stepNode, SERVICE)) {
      throw error.apply(
          "Step '"
              + name
              + "' must define either 'stepClass' or a declarative service step ('service' +"
              + " phases)");
    }
    return null;
  }

  /** Rejects a SAGA-mode service step that carries TCC phases or omits a SAGA phase. */
  public static void requireSagaPhases(
      JsonNode stepNode, String name, Function<String, RuntimeException> error) {
    if (hasTccPhase(stepNode)) {
      throw error.apply(
          "SAGA definition's service step '"
              + name
              + "' must use SAGA phases (execution/compensation), not TCC phases"
              + " (reservation/confirmation/cancellation)");
    }
    if (!isPresent(stepNode, EXECUTION) || !isPresent(stepNode, COMPENSATION)) {
      throw error.apply(
          "SAGA declarative service step '"
              + name
              + "' must define both 'execution' and 'compensation'");
    }
  }

  /** Rejects a TCC-mode service step that carries SAGA phases or omits a TCC phase. */
  public static void requireTccPhases(
      JsonNode stepNode, String name, Function<String, RuntimeException> error) {
    if (hasSagaPhase(stepNode)) {
      throw error.apply(
          "TCC definition's service step '"
              + name
              + "' must use TCC phases (reservation/confirmation/cancellation), not SAGA phases"
              + " (execution/compensation)");
    }
    if (!isPresent(stepNode, RESERVATION)
        || !isPresent(stepNode, CONFIRMATION)
        || !isPresent(stepNode, CANCELLATION)) {
      throw error.apply(
          "TCC declarative service step '"
              + name
              + "' must define 'reservation', 'confirmation', and 'cancellation'");
    }
  }

  /**
   * Rejects an {@code async} marker on a backward phase (compensation / cancellation). Async
   * completion — the participant parking the saga and calling back — applies only to forward phases
   * (execution / reservation / confirmation); a compensation/cancellation always runs
   * synchronously. Shared by the parser and the store serializer via {@code error} so the failure
   * type matches the layer.
   */
  public static void rejectAsyncOnBackwardPhase(
      JsonNode stepNode, String name, Function<String, RuntimeException> error) {
    for (String phase : new String[] {COMPENSATION, CANCELLATION}) {
      JsonNode phaseNode = stepNode.get(phase);
      if (phaseNode != null && isPresent(phaseNode, ASYNC) && phaseNode.get(ASYNC).asBoolean()) {
        throw error.apply(
            "Step '"
                + name
                + "' phase '"
                + phase
                + "' must not be async; async completion applies only to forward phases"
                + " (execution/reservation/confirmation)");
      }
    }
  }

  /** Whether the step node declares any SAGA phase (execution/compensation). */
  public static boolean hasSagaPhase(JsonNode stepNode) {
    return isPresent(stepNode, EXECUTION) || isPresent(stepNode, COMPENSATION);
  }

  /** Whether the step node declares any TCC phase (reservation/confirmation/cancellation). */
  public static boolean hasTccPhase(JsonNode stepNode) {
    return isPresent(stepNode, RESERVATION)
        || isPresent(stepNode, CONFIRMATION)
        || isPresent(stepNode, CANCELLATION);
  }
}
