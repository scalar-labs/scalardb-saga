package com.scalar.db.saga.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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

  // The HTTP call spec's own keys. The values of QUERY/JSON_BODY/OUTPUT are free-form user maps and
  // are not validated here.
  private static final Set<String> KNOWN_HTTP_FIELDS =
      Set.of(METHOD, PATH, QUERY, JSON_BODY, STRING_BODY, CONTENT_TYPE, OUTPUT);

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
              + "' is not yet supported (Task 2.1b)");
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
}
