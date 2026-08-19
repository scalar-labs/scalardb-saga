package com.scalar.db.saga.exception;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.Immutable;

/**
 * Declares the required metadata keys of a {@link SagaErrorCode}, in the order they render in the
 * assembled message. The base exception constructor validates every metadata map against its code's
 * {@code ErrorMetadataSchema} at construction time so a wrong or missing key fails fast rather than
 * shipping to logs and the wire.
 *
 * <p>Codes with alternate context shapes (e.g., "user identified by name OR id") are modeled as two
 * separate codes rather than one code with an either-or schema. Keeps {@code ErrorMetadataSchema}
 * trivial and each generated docs page unambiguous.
 */
@Immutable
public final class ErrorMetadataSchema {

  private static final ErrorMetadataSchema EMPTY = new ErrorMetadataSchema(Collections.emptyList());

  private final List<String> requiredKeys;
  private final Set<String> requiredKeySet;

  private ErrorMetadataSchema(List<String> requiredKeys) {
    // Defensive copy in the constructor (not the factory) so SpotBugs's EI_EXPOSE_REP is satisfied
    // seeing the copy in the ctor's bytecode; also lets the getter return the field directly.
    this.requiredKeys = Collections.unmodifiableList(new ArrayList<>(requiredKeys));
    // Precomputed once: validate runs in every exception constructor, and Set.equals is
    // order-insensitive, so a per-construction copy added nothing.
    this.requiredKeySet = Collections.unmodifiableSet(new LinkedHashSet<>(requiredKeys));
  }

  /**
   * Declares the metadata keys the code requires, in the order they render in the message. Keys
   * must be non-blank and unique.
   *
   * @throws IllegalArgumentException if any key is null, blank, or duplicated
   */
  static ErrorMetadataSchema of(String... keys) {
    Set<String> seen = new LinkedHashSet<>();
    for (String key : keys) {
      if (key == null || key.trim().isEmpty()) {
        throw new IllegalArgumentException("schema key must not be null or blank");
      }
      if (!seen.add(key)) {
        throw new IllegalArgumentException("duplicate schema key: " + key);
      }
    }
    return new ErrorMetadataSchema(new ArrayList<>(seen));
  }

  /** For codes whose failure carries no per-invocation context. */
  static ErrorMetadataSchema none() {
    return EMPTY;
  }

  /** The declared keys, in insertion order. */
  public List<String> requiredKeys() {
    return requiredKeys;
  }

  /**
   * Fails fast on programming bugs: the provided {@code metadata} must have exactly the declared
   * keys (no extras, none missing) and every value must be non-null.
   *
   * @throws IllegalArgumentException if the key set differs from {@link #requiredKeys()} or any
   *     value is null
   */
  void validate(SagaErrorCode code, Map<String, String> metadata) {
    if (!metadata.keySet().equals(requiredKeySet)) {
      throw new IllegalArgumentException(
          code.name()
              + " expects metadata keys "
              + requiredKeySet
              + " but got "
              + metadata.keySet());
    }
    for (String key : requiredKeys) {
      if (metadata.get(key) == null) {
        throw new IllegalArgumentException(code.name() + " metadata key '" + key + "' is null");
      }
    }
  }
}
