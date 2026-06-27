package com.scalar.db.saga.api;

import java.util.Objects;

/**
 * Identifies a specific version of a saga definition. Use this with the versioned {@link
 * SagaOrchestrator#start(SagaDefinitionId, java.util.Map)} overloads to skip the store round-trip
 * that name-only start requires for "latest version" resolution.
 *
 * @param name the saga definition name
 * @param version the saga definition version
 */
public record SagaDefinitionId(String name, String version) {

  /** Compact constructor — validates both fields. */
  public SagaDefinitionId {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(version, "version must not be null");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (version.isBlank()) {
      throw new IllegalArgumentException("version must not be blank");
    }
    if (name.contains(":")) {
      throw new IllegalArgumentException("name must not contain ':': '" + name + "'");
    }
    if (version.contains(":")) {
      throw new IllegalArgumentException("version must not contain ':': '" + version + "'");
    }
  }

  @Override
  public String toString() {
    return name + " (v" + version + ")";
  }
}
