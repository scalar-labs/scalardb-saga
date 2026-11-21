package com.scalar.db.saga.designer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * The designer-only data of one definition: its sample saga input and each step's sample response.
 * None of it is part of the definition, and none of it is ever sent to a server.
 *
 * <p>Saved as {@code .designer/<definition file name>}, in the definition's own format, so both
 * read alike in a diff. The file is keyed by step name, so that it means something on its own; the
 * UI keys the same data by step id, so that renaming a step needs no bookkeeping. The conversion
 * happens here, against the model's steps.
 *
 * <p>The UI's form: {@code {sampleInput: text or null, steps: {<step id>: {sampleResponse:
 * text}}}}. The file's form: {@code {formatVersion: 1, sampleInput, steps: {<step name>:
 * {sampleResponse}}}}. Samples are kept as the text the user pasted, never re-encoded.
 */
final class DesignerData {

  static final int FORMAT_VERSION = 1;

  private DesignerData() {}

  /** The data of a file, for the UI; data for steps the definition no longer has is dropped. */
  static ObjectNode read(String fileText, Format format, JsonNode model) {
    JsonNode file;
    try {
      file = DefinitionFormats.read(fileText, format);
    } catch (JsonProcessingException e) {
      throw new BadRequestException(
          "The designer data file is not valid " + format + ": " + e.getOriginalMessage());
    }
    checkFormatVersion(file);
    Map<String, String> idByName = new HashMap<>();
    for (JsonNode step : model.path("steps")) {
      JsonNode name = step.get("name");
      if (name != null && name.isTextual()) {
        idByName.putIfAbsent(name.asText(), step.path("id").asText());
      }
    }
    ObjectNode data = empty();
    copyText(file, data, "sampleInput");
    ObjectNode steps = (ObjectNode) data.get("steps");
    for (Map.Entry<String, JsonNode> entry : file.path("steps").properties()) {
      String id = idByName.get(entry.getKey());
      if (id != null && entry.getValue().isObject()) {
        ObjectNode step = steps.putObject(id);
        copyText(entry.getValue(), step, "sampleResponse");
      }
    }
    return data;
  }

  /** The data of a definition with none saved yet. */
  static ObjectNode empty() {
    ObjectNode data = DefinitionFormats.json().createObjectNode();
    data.putNull("sampleInput");
    data.putObject("steps");
    return data;
  }

  /**
   * The file text for the UI's data.
   *
   * @param existing the file as it is now, or {@code null}; a file written by a newer designer is
   *     never overwritten, since this version would drop what it does not know
   * @throws BadRequestException if a step name is blank or repeated, since the file is keyed by
   *     name, or if the existing file is from a newer designer
   */
  static String write(JsonNode data, JsonNode model, Format format, @Nullable String existing) {
    if (existing != null) {
      try {
        checkFormatVersion(DefinitionFormats.read(existing, format));
      } catch (JsonProcessingException e) {
        // A damaged file is replaced: what it held cannot be read either way.
      }
    }
    ObjectNode file = DefinitionFormats.json().createObjectNode();
    file.put("formatVersion", FORMAT_VERSION);
    copyText(data, file, "sampleInput");
    ObjectNode steps = file.putObject("steps");
    Set<String> names = new HashSet<>();
    // In definition order, so the file reads in the same order as the definition.
    for (JsonNode step : model.path("steps")) {
      if (step.has("raw")) {
        continue; // not an object, so it has no name and the UI keeps no samples for it
      }
      String name = step.path("name").asText("");
      if (name.isBlank() || !names.add(name)) {
        throw new BadRequestException(
            "Sample data is saved by step name, so every step needs a unique name");
      }
      JsonNode stepData = data.path("steps").get(step.path("id").asText());
      if (stepData != null && stepData.isObject()) {
        ObjectNode out = steps.objectNode();
        copyText(stepData, out, "sampleResponse");
        if (!out.isEmpty()) {
          steps.set(name, out);
        }
      }
    }
    return DefinitionFormats.write(file, format);
  }

  private static void checkFormatVersion(JsonNode file) {
    int version = file.path("formatVersion").asInt(FORMAT_VERSION);
    if (version > FORMAT_VERSION) {
      throw new BadRequestException(
          422,
          "The designer data was saved by a newer designer (format "
              + version
              + "); update the designer to edit it");
    }
  }

  private static void copyText(JsonNode from, ObjectNode to, String field) {
    JsonNode value = from.get(field);
    if (value != null && value.isTextual()) {
      to.put(field, value.asText());
    }
  }
}
