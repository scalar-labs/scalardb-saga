package com.scalar.db.saga.designer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Validates a definition with {@link SagaDefinitionParser}, and places each problem on the step it
 * belongs to.
 *
 * <p>The parser stops at the first problem and does not always say which step it was in. So each
 * step is first parsed on its own, wrapped in a one-step definition, which finds every step-local
 * problem of every step at once (a body on {@code GET}, a missing phase, a compensation reading its
 * own output, an unknown field) and places it on that step. The whole definition is then parsed for
 * the cross-step rules (duplicate names, pivot placement, definition fields); an error a probe
 * already reported is not repeated.
 *
 * <p>No rule is reimplemented here: every problem comes from the parser.
 */
final class DefinitionValidator {

  private DefinitionValidator() {}

  /**
   * Returns the parser's problems with a definition.
   *
   * @param definition the definition tree, as {@link EditorModel#toDefinition} built it
   * @param stepIds the editor id of each step, in the same order as the definition's steps
   */
  static List<Problem> validate(ObjectNode definition, List<String> stepIds) {
    List<Problem> problems = new ArrayList<>();
    JsonNode steps = definition.path("steps");
    if (steps.isArray()) {
      for (int i = 0; i < steps.size() && i < stepIds.size(); i++) {
        JsonNode step = steps.get(i);
        if (step.isObject()) {
          Problem problem = parse(probe(definition, (ObjectNode) step), stepIds.get(i));
          if (problem != null) {
            problems.add(problem);
          }
        }
      }
    }
    Problem whole = parse(definition, null);
    if (whole != null && problems.stream().noneMatch(p -> sameError(p, whole))) {
      problems.add(whole);
    }
    return problems;
  }

  /**
   * Wraps one step in the smallest definition the parser accepts around it. Nothing of the
   * enclosing definition is copied except a valid {@code mode}, so a problem with the definition's
   * own fields never shows up on every step. The step keeps every field except, in SAGA mode,
   * {@code pivot}: a pivot is valid only relative to the other steps, so the full parse judges it.
   * A TCC step keeps it, because a pivot is wrong on any TCC step and the error belongs there.
   */
  private static ObjectNode probe(ObjectNode definition, ObjectNode step) {
    ObjectNode probe = definition.objectNode();
    probe.put("name", "probe");
    boolean tcc = "TCC".equals(definition.path("mode").asText());
    if (tcc) {
      probe.put("mode", "TCC");
    }
    ObjectNode probeStep = step.deepCopy();
    if (!tcc) {
      probeStep.remove("pivot");
    }
    probe.putArray("steps").add(probeStep);
    return probe;
  }

  private static @Nullable Problem parse(ObjectNode definition, @Nullable String stepId) {
    String json;
    try {
      json = DefinitionFormats.json().writeValueAsString(definition);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to write a definition tree", e);
    }
    try {
      SagaDefinitionParser.parseJson(json);
      return null;
    } catch (SagaDefinitionException e) {
      String detail = e.getMetadata().get("detail");
      return new Problem(e.getErrorCode().name(), stepId, detail != null ? detail : e.getMessage());
    } catch (RuntimeException e) {
      // The parser is meant to report every bad input as a SagaDefinitionException; anything else
      // escaping it is still a reason the definition cannot be used, so it is shown rather than
      // turned into a 500 that hides which definition caused it.
      return new Problem(
          "INVALID_DEFINITION",
          stepId,
          e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }
  }

  private static boolean sameError(Problem a, Problem b) {
    return a.code().equals(b.code()) && a.message().equals(b.message());
  }
}
