package com.scalar.db.saga.designer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
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

  /** The file text to write, and the steps whose samples have to wait for a definition save. */
  record Written(String text, List<String> deferred) {}

  /**
   * The file text for the UI's data, when the model is what the definition file holds.
   *
   * @param existing the file as it is now, or {@code null}; a file written by a newer designer is
   *     never overwritten, since this version would drop what it does not know
   * @throws BadRequestException if a step name is blank or repeated, since the file is keyed by
   *     name, or if the existing file is from a newer designer
   */
  static String write(JsonNode data, JsonNode model, Format format, @Nullable String existing) {
    return write(data, model, format, existing, null).text();
  }

  /**
   * The file text for the UI's data.
   *
   * <p>When the definition was not saved, as when it has problems, the file on disk still names its
   * steps the old way, and the samples have to stay keyed to those names or the next open drops
   * them. {@code savedNames} then lists the step names on disk:
   *
   * <ul>
   *   <li>a model step with a saved name gets the model's samples;
   *   <li>a saved name no model step has any more, such as the old name of a renamed step, keeps
   *       its samples from {@code existing};
   *   <li>a model step whose name is not saved yet is listed in {@link Written#deferred}, and its
   *       samples stay in the editor until the definition can be saved.
   * </ul>
   *
   * @param savedNames the step names in the definition file on disk if the model was not saved
   *     there, or {@code null} if the file holds exactly the model
   * @throws BadRequestException as {@link #write(JsonNode, JsonNode, Format, String)} does
   */
  static Written write(
      JsonNode data,
      JsonNode model,
      Format format,
      @Nullable String existing,
      @Nullable List<String> savedNames) {
    JsonNode existingFile = null;
    if (existing != null) {
      try {
        existingFile = DefinitionFormats.read(existing, format);
        checkFormatVersion(existingFile);
      } catch (JsonProcessingException e) {
        // A damaged file is replaced: what it held cannot be read either way.
      }
    }
    // The model's samples by step name, in definition order.
    Map<String, ObjectNode> fromModel = new LinkedHashMap<>();
    for (JsonNode step : model.path("steps")) {
      if (step.has("raw")) {
        continue; // not an object, so it has no name and the UI keeps no samples for it
      }
      String name = step.path("name").asText("");
      if (name.isBlank() || fromModel.containsKey(name)) {
        throw new BadRequestException(
            "Sample data is saved by step name, so every step needs a unique name");
      }
      ObjectNode out = DefinitionFormats.json().createObjectNode();
      JsonNode stepData = data.path("steps").get(step.path("id").asText());
      if (stepData != null && stepData.isObject()) {
        copyText(stepData, out, "sampleResponse");
      }
      fromModel.put(name, out);
    }

    ObjectNode file = DefinitionFormats.json().createObjectNode();
    file.put("formatVersion", FORMAT_VERSION);
    copyText(data, file, "sampleInput");
    ObjectNode steps = file.putObject("steps");
    List<String> deferred = new ArrayList<>();
    if (savedNames == null) {
      fromModel.forEach(
          (name, out) -> {
            if (!out.isEmpty()) {
              steps.set(name, out);
            }
          });
    } else {
      for (String name : savedNames) {
        ObjectNode out = fromModel.get(name);
        JsonNode kept = existingFile == null ? null : existingFile.path("steps").get(name);
        if (out != null) {
          if (!out.isEmpty()) {
            steps.set(name, out);
          }
        } else if (kept != null && kept.isObject()) {
          ObjectNode copy = steps.putObject(name);
          copyText(kept, copy, "sampleResponse");
        }
      }
      Set<String> saved = new HashSet<>(savedNames);
      fromModel.forEach(
          (name, out) -> {
            if (!saved.contains(name) && !out.isEmpty()) {
              deferred.add(name);
            }
          });
    }
    return new Written(DefinitionFormats.write(file, format), List.copyOf(deferred));
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
