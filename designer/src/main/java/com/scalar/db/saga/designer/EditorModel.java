package com.scalar.db.saga.designer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Converts between a definition file's tree and the editor model the UI edits.
 *
 * <p>The model exists for two reasons. First, the browser must never parse a definition's numbers,
 * because JavaScript turns {@code 100.50} into {@code 100.5} and loses integers above 2^53: every
 * number in the model is its source text, written back unchanged. Second, the form can only edit
 * what it knows, and opening a file must never lose the rest: an unknown field, or a known field
 * holding a value of a type the form does not edit, is kept in the object's {@code extras} as its
 * JSON text and written back where it was.
 *
 * <p>The model, as JSON:
 *
 * <pre>
 * definition: name, version, mode, recoveryStrategy (text or null); timeoutMillis (number text or
 *             null); defaultRetryPolicy (retry or null); steps (array of step); extras
 * step:       id (editor-only, never written); name, stepClass, service, transport (text or null);
 *             pivot (boolean or null); timeoutMillis (number text or null); retryPolicy (retry or
 *             null); phases (object of phase name to call, holding only the phases present);
 *             extras. A step that is not a JSON object is {id, raw: its JSON text}.
 * retry:      maxAttempts, initialIntervalMillis, backoffMultiplier, maxIntervalMillis (number
 *             text or null); extras
 * call:       method, path, stringBody, contentType (text or null); query ([{name, value}] or
 *             null); jsonBody (an object body value, or null); output ([{name, value}] or null);
 *             async (boolean or null); callbackTimeoutMillis (number text or null); extras
 * body value: {kind: context, key} | {kind: template, text} | {kind: fixed, type: string, number,
 *             boolean or null, text} | {kind: object, fields: [{name, value}]} | {kind: list,
 *             items: [value]}
 * extras:     [{name, json}], in the order they appeared
 * </pre>
 *
 * <p>A {@code null} model field means the field is absent from the file. Export writes known fields
 * in the order of the definition reference, then the extras.
 */
final class EditorModel {

  private static final JsonNodeFactory NODES = DefinitionFormats.json().getNodeFactory();

  // A body string that is exactly one ${key} keeps the context value's type; anything else
  // containing ${ is text with keys. Same placeholder syntax as HttpCall.PLACEHOLDER.
  private static final Pattern WHOLE_PLACEHOLDER = Pattern.compile("\\$\\{([^}]*)\\}");

  static final List<String> PHASES =
      List.of("execution", "compensation", "reservation", "confirmation", "cancellation");

  private enum Kind {
    TEXT,
    NUMBER,
    BOOLEAN,
    RETRY,
    STEPS,
    CALL,
    STRING_MAP,
    BODY
  }

  // Field order here is the order export writes them in.
  private static final Map<String, Kind> DEFINITION_FIELDS =
      ordered(
          field("name", Kind.TEXT),
          field("version", Kind.TEXT),
          field("mode", Kind.TEXT),
          field("recoveryStrategy", Kind.TEXT),
          field("timeoutMillis", Kind.NUMBER),
          field("defaultRetryPolicy", Kind.RETRY),
          field("steps", Kind.STEPS));

  private static final Map<String, Kind> STEP_FIELDS =
      ordered(
          field("name", Kind.TEXT),
          field("stepClass", Kind.TEXT),
          field("service", Kind.TEXT),
          field("transport", Kind.TEXT),
          field("pivot", Kind.BOOLEAN),
          field("execution", Kind.CALL),
          field("compensation", Kind.CALL),
          field("reservation", Kind.CALL),
          field("confirmation", Kind.CALL),
          field("cancellation", Kind.CALL),
          field("retryPolicy", Kind.RETRY),
          field("timeoutMillis", Kind.NUMBER));

  private static final Map<String, Kind> RETRY_FIELDS =
      ordered(
          field("maxAttempts", Kind.NUMBER),
          field("initialIntervalMillis", Kind.NUMBER),
          field("backoffMultiplier", Kind.NUMBER),
          field("maxIntervalMillis", Kind.NUMBER));

  private static final Map<String, Kind> CALL_FIELDS =
      ordered(
          field("method", Kind.TEXT),
          field("path", Kind.TEXT),
          field("query", Kind.STRING_MAP),
          field("jsonBody", Kind.BODY),
          field("stringBody", Kind.TEXT),
          field("contentType", Kind.TEXT),
          field("output", Kind.STRING_MAP),
          field("async", Kind.BOOLEAN),
          field("callbackTimeoutMillis", Kind.NUMBER));

  private EditorModel() {}

  // ---------------------------------------------------------------------------------------------
  // Import: file tree to model
  // ---------------------------------------------------------------------------------------------

  /**
   * Builds the editor model of a definition file's tree. Never fails on a well-formed object:
   * whatever the form cannot hold goes to {@code extras}.
   *
   * @throws BadRequestException if the document is not a JSON object
   */
  static ObjectNode fromDefinition(JsonNode root) {
    if (!root.isObject()) {
      throw new BadRequestException("A saga definition must be a JSON object");
    }
    ObjectNode model = importObject(root, DEFINITION_FIELDS);
    // A new definition starts with an empty step list. A "steps" that is there but not a list is
    // kept as it is in extras, and model.steps stays null: setting both would make the field
    // "edited and kept as is" at once, and every later export of the model would fail.
    if (!root.has("steps")) {
      model.set("steps", NODES.arrayNode());
    }
    return model;
  }

  private static ObjectNode importObject(JsonNode source, Map<String, Kind> fields) {
    ObjectNode model = NODES.objectNode();
    fields.forEach(
        (name, kind) -> {
          if (kind == Kind.CALL) {
            return; // grouped under "phases" below
          }
          model.set(name, NullNode.getInstance());
        });
    ObjectNode phases = NODES.objectNode();
    ArrayNode extras = NODES.arrayNode();
    for (Map.Entry<String, JsonNode> field : source.properties()) {
      String name = field.getKey();
      JsonNode value = field.getValue();
      Kind kind = fields.get(name);
      JsonNode converted = kind == null ? null : importValue(kind, value);
      if (converted == null) {
        extras.add(extra(name, value));
      } else if (kind == Kind.CALL) {
        phases.set(name, converted);
      } else {
        model.set(name, converted);
      }
    }
    if (fields.containsValue(Kind.CALL)) {
      model.set("phases", phases);
    }
    model.set("extras", extras);
    return model;
  }

  /** Converts one known field's value, or returns null when the form cannot hold it. */
  private static @Nullable JsonNode importValue(Kind kind, JsonNode value) {
    return switch (kind) {
      case TEXT -> value.isTextual() ? value : null;
      case NUMBER -> value.isNumber() ? NODES.textNode(DefinitionFormats.numberText(value)) : null;
      case BOOLEAN -> value.isBoolean() ? value : null;
      case RETRY -> value.isObject() ? importObject(value, RETRY_FIELDS) : null;
      case STEPS -> value.isArray() ? importSteps(value) : null;
      case CALL -> value.isObject() ? importObject(value, CALL_FIELDS) : null;
      case STRING_MAP -> importStringMap(value);
      case BODY -> value.isObject() ? importBody(value) : null;
    };
  }

  private static ArrayNode importSteps(JsonNode steps) {
    ArrayNode result = NODES.arrayNode();
    int index = 0;
    for (JsonNode step : steps) {
      index++;
      ObjectNode model;
      if (step.isObject()) {
        model = importObject(step, STEP_FIELDS);
      } else {
        model = NODES.objectNode();
        model.put("raw", toJson(step));
      }
      // Ids only have to be unique within this model; the UI mints its own for new steps.
      ObjectNode withId = NODES.objectNode();
      withId.put("id", "s" + index);
      withId.setAll(model);
      result.add(withId);
    }
    return result;
  }

  /** A query or output map: kept as name and value rows only if every value is a string. */
  private static @Nullable JsonNode importStringMap(JsonNode value) {
    if (!value.isObject()) {
      return null;
    }
    ArrayNode rows = NODES.arrayNode();
    for (Map.Entry<String, JsonNode> entry : value.properties()) {
      if (!entry.getValue().isTextual()) {
        return null;
      }
      ObjectNode row = rows.addObject();
      row.put("name", entry.getKey());
      row.put("value", entry.getValue().asText());
    }
    return rows;
  }

  private static ObjectNode importBody(JsonNode value) {
    ObjectNode body = NODES.objectNode();
    if (value.isObject()) {
      body.put("kind", "object");
      ArrayNode fields = body.putArray("fields");
      for (Map.Entry<String, JsonNode> entry : value.properties()) {
        ObjectNode field = fields.addObject();
        field.put("name", entry.getKey());
        field.set("value", importBody(entry.getValue()));
      }
    } else if (value.isArray()) {
      body.put("kind", "list");
      ArrayNode items = body.putArray("items");
      value.forEach(item -> items.add(importBody(item)));
    } else if (value.isTextual()) {
      String text = value.asText();
      Matcher whole = WHOLE_PLACEHOLDER.matcher(text);
      if (whole.matches()) {
        body.put("kind", "context");
        body.put("key", whole.group(1));
      } else if (text.contains("${")) {
        body.put("kind", "template");
        body.put("text", text);
      } else {
        fixed(body, "string", text);
      }
    } else if (value.isNumber()) {
      fixed(body, "number", DefinitionFormats.numberText(value));
    } else if (value.isBoolean()) {
      fixed(body, "boolean", value.asText());
    } else {
      fixed(body, "null", "null");
    }
    return body;
  }

  private static void fixed(ObjectNode body, String type, String text) {
    body.put("kind", "fixed");
    body.put("type", type);
    body.put("text", text);
  }

  private static ObjectNode extra(String name, JsonNode value) {
    ObjectNode extra = NODES.objectNode();
    extra.put("name", name);
    extra.put("json", toJson(value));
    return extra;
  }

  private static String toJson(JsonNode value) {
    try {
      return DefinitionFormats.json().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to write a JSON value", e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Export: model to file tree
  // ---------------------------------------------------------------------------------------------

  /**
   * A definition tree built from the model, and the values the form held that could not be written
   * as their declared type. Those are still written, as text, so the parser runs on the rest of the
   * definition.
   */
  record Export(ObjectNode definition, List<Problem> problems) {}

  /**
   * Builds the definition tree a model describes.
   *
   * @throws BadRequestException if the model is not shaped as documented on this class; that is a
   *     defect in the UI, not a problem with the definition
   */
  static Export toDefinition(JsonNode model) {
    Exporter exporter = new Exporter();
    ObjectNode definition = exporter.object(requireObject(model, "definition"), DEFINITION_FIELDS);
    return new Export(definition, List.copyOf(exporter.problems));
  }

  /** Returns the editor id of each step, in order, as the model holds them. */
  static List<String> stepIds(JsonNode model) {
    List<String> ids = new ArrayList<>();
    JsonNode steps = model.path("steps");
    for (JsonNode step : steps) {
      ids.add(requireText(step, "id"));
    }
    return ids;
  }

  private static final class Exporter {
    private final List<Problem> problems = new ArrayList<>();
    private @Nullable String stepId;
    private String where = "";

    ObjectNode object(JsonNode model, Map<String, Kind> fields) {
      ObjectNode result = NODES.objectNode();
      for (Map.Entry<String, Kind> field : fields.entrySet()) {
        String name = field.getKey();
        JsonNode value =
            field.getValue() == Kind.CALL ? model.path("phases").get(name) : model.get(name);
        if (value == null || value.isNull()) {
          continue;
        }
        String outer = where;
        where = where.isEmpty() ? name : where + "." + name;
        result.set(name, exportValue(field.getValue(), value));
        where = outer;
      }
      JsonNode extras = model.get("extras");
      if (extras != null && !extras.isNull()) {
        if (!extras.isArray()) {
          throw new BadRequestException("'extras' must be an array");
        }
        for (JsonNode extra : extras) {
          String name = requireText(extra, "name");
          if (result.has(name)) {
            throw new BadRequestException("Field '" + name + "' is both edited and kept as is");
          }
          result.set(name, parseJson(requireText(extra, "json")));
        }
      }
      return result;
    }

    private JsonNode exportValue(Kind kind, JsonNode value) {
      return switch (kind) {
        case TEXT -> {
          if (!value.isTextual()) {
            throw new BadRequestException("'" + where + "' must be a string");
          }
          yield value;
        }
        case NUMBER -> number(requireTextValue(value), where);
        case BOOLEAN -> {
          if (!value.isBoolean()) {
            throw new BadRequestException("'" + where + "' must be a boolean");
          }
          yield value;
        }
        case RETRY -> object(requireObject(value, where), RETRY_FIELDS);
        case STEPS -> steps(value);
        case CALL -> object(requireObject(value, where), CALL_FIELDS);
        case STRING_MAP -> stringMap(value);
        case BODY -> body(value);
      };
    }

    private ArrayNode steps(JsonNode steps) {
      if (!steps.isArray()) {
        throw new BadRequestException("'steps' must be an array");
      }
      ArrayNode result = NODES.arrayNode();
      Set<String> ids = new HashSet<>();
      for (JsonNode step : steps) {
        String id = requireText(step, "id");
        if (!ids.add(id)) {
          throw new BadRequestException("Duplicate step id '" + id + "'");
        }
        stepId = id;
        String outer = where;
        where = "step '" + step.path("name").asText(id) + "'";
        JsonNode raw = step.get("raw");
        result.add(
            raw != null && !raw.isNull()
                ? parseJson(requireTextValue(raw))
                : object(step, STEP_FIELDS));
        where = outer;
        stepId = null;
      }
      return result;
    }

    private ObjectNode stringMap(JsonNode rows) {
      if (!rows.isArray()) {
        throw new BadRequestException("'" + where + "' must be an array of rows");
      }
      ObjectNode result = NODES.objectNode();
      for (JsonNode row : rows) {
        String name = requireText(row, "name");
        String value = requireText(row, "value");
        if (result.has(name)) {
          problem("'" + where + "' has '" + name + "' twice; only the last one is kept");
        }
        result.put(name, value);
      }
      return result;
    }

    private JsonNode body(JsonNode value) {
      requireObject(value, where);
      String kind = requireText(value, "kind");
      return switch (kind) {
        case "context" -> NODES.textNode("${" + requireText(value, "key") + "}");
        case "template" -> NODES.textNode(requireText(value, "text"));
        case "fixed" -> fixedValue(requireText(value, "type"), requireText(value, "text"));
        case "object" -> {
          ObjectNode result = NODES.objectNode();
          for (JsonNode field : requireArray(value, "fields")) {
            String name = requireText(field, "name");
            if (result.has(name)) {
              problem("'" + where + "' has '" + name + "' twice; only the last one is kept");
            }
            String outer = where;
            where = where + "." + name;
            result.set(name, body(requireObject(field.path("value"), where)));
            where = outer;
          }
          yield result;
        }
        case "list" -> {
          ArrayNode result = NODES.arrayNode();
          for (JsonNode item : requireArray(value, "items")) {
            result.add(body(item));
          }
          yield result;
        }
        default -> throw new BadRequestException("Unknown body value kind '" + kind + "'");
      };
    }

    private JsonNode fixedValue(String type, String text) {
      return switch (type) {
        case "string" -> NODES.textNode(text);
        case "number" -> number(text, where);
        case "boolean" -> {
          if (text.equals("true") || text.equals("false")) {
            yield NODES.booleanNode(Boolean.parseBoolean(text));
          }
          problem("'" + where + "' is not true or false: " + text);
          yield NODES.textNode(text);
        }
        case "null" -> NullNode.getInstance();
        default -> throw new BadRequestException("Unknown fixed value type '" + type + "'");
      };
    }

    /**
     * Reads number text through the same mapper that reads files, so the node is exactly the one
     * reading the file back produces. Text that is not a JSON number is a problem, and is written
     * as a string so that the rest of the definition still validates.
     */
    private JsonNode number(String text, String field) {
      JsonNode parsed = null;
      try {
        parsed = DefinitionFormats.json().readTree(text.strip());
      } catch (JsonProcessingException e) {
        // reported below
      }
      if (parsed != null && parsed.isNumber()) {
        return parsed;
      }
      problem("'" + field + "' is not a number: " + text);
      return NODES.textNode(text);
    }

    private void problem(String message) {
      problems.add(new Problem(Problem.FORM, stepId, message));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Model shape checks: a failure is a defect in the UI, answered with 400
  // ---------------------------------------------------------------------------------------------

  private static JsonNode requireObject(JsonNode node, String where) {
    if (!node.isObject()) {
      throw new BadRequestException("'" + where + "' must be an object");
    }
    return node;
  }

  private static JsonNode requireArray(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || !value.isArray()) {
      throw new BadRequestException("'" + field + "' must be an array");
    }
    return value;
  }

  private static String requireText(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual()) {
      throw new BadRequestException("'" + field + "' must be a string");
    }
    return value.asText();
  }

  private static String requireTextValue(JsonNode value) {
    if (!value.isTextual()) {
      throw new BadRequestException("Expected a string, got " + value.getNodeType());
    }
    return value.asText();
  }

  private static JsonNode parseJson(String json) {
    try {
      return DefinitionFormats.read(json, DefinitionFormats.Format.JSON);
    } catch (JsonProcessingException e) {
      throw new BadRequestException("A kept value is not valid JSON: " + e.getOriginalMessage());
    }
  }

  private record Field(String name, Kind kind) {}

  private static Field field(String name, Kind kind) {
    return new Field(name, kind);
  }

  /** The fields of one object, in the order export writes them. */
  private static Map<String, Kind> ordered(Field... fields) {
    Map<String, Kind> map = new LinkedHashMap<>();
    for (Field field : fields) {
      map.put(field.name(), field.kind());
    }
    return Collections.unmodifiableMap(map);
  }
}
