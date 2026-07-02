package com.scalar.db.saga.api;

import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Identifies a specific version of a saga definition. Use this with the versioned {@link
 * SagaOrchestrator#start(SagaDefinitionId, java.util.Map)} overloads to skip the store round-trip
 * that name-only start requires for "latest version" resolution.
 */
@Immutable
public final class SagaDefinitionId {

  private final String name;
  private final String version;

  /**
   * Creates an id, validating both fields.
   *
   * @param name the saga definition name
   * @param version the saga definition version
   */
  public SagaDefinitionId(String name, String version) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(version, "version must not be null");
    // Reject empty or all-whitespace (Java 8 equivalent of String.isBlank(), which is Java 11+):
    // allMatch returns true for an empty code-point stream, so this also rejects "".
    if (name.codePoints().allMatch(Character::isWhitespace)) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (version.codePoints().allMatch(Character::isWhitespace)) {
      throw new IllegalArgumentException("version must not be blank");
    }
    if (name.contains(":")) {
      throw new IllegalArgumentException("name must not contain ':': '" + name + "'");
    }
    if (version.contains(":")) {
      throw new IllegalArgumentException("version must not contain ':': '" + version + "'");
    }
    this.name = name;
    this.version = version;
  }

  /** The saga definition name. */
  public String name() {
    return name;
  }

  /** The saga definition version. */
  public String version() {
    return version;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaDefinitionId)) return false;
    SagaDefinitionId that = (SagaDefinitionId) o;
    return name.equals(that.name) && version.equals(that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version);
  }

  @Override
  public String toString() {
    return name + " (v" + version + ")";
  }
}
