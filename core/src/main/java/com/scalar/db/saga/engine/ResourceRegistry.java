package com.scalar.db.saga.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Immutable registry of typed resources for constructor injection during step resolution.
 *
 * <p>Resources are registered by type and optional qualifier name. The {@link
 * ReflectiveStepResolver} uses this registry to match constructor parameters against available
 * resources.
 */
@Immutable
class ResourceRegistry {

  private final Map<ResourceKey, Object> resources;

  private ResourceRegistry(Map<ResourceKey, Object> resources) {
    this.resources = Map.copyOf(resources);
  }

  /**
   * Looks up a resource by exact type and qualifier name.
   *
   * @param type the resource type (exact match)
   * @param name the qualifier name, or {@code null} for unnamed lookup
   * @return the resource instance
   * @throws IllegalArgumentException if no matching resource is found
   */
  Object get(Class<?> type, @Nullable String name) {
    ResourceKey key = new ResourceKey(type, name);
    Object resource = resources.get(key);
    if (resource != null) {
      return resource;
    }
    String desc =
        name != null
            ? "type " + type.getName() + " with name '" + name + "'"
            : "type " + type.getName() + " (unnamed)";
    throw new IllegalArgumentException("No resource registered for " + desc);
  }

  /**
   * Checks whether a resource matching the given type and qualifier exists.
   *
   * @param type the resource type (exact match)
   * @param name the qualifier name, or {@code null} for unnamed lookup
   * @return {@code true} if a matching resource exists
   */
  boolean contains(Class<?> type, @Nullable String name) {
    return resources.containsKey(new ResourceKey(type, name));
  }

  /** Returns {@code true} if no resources are registered. */
  boolean isEmpty() {
    return resources.isEmpty();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  /** Builder for constructing an immutable {@link ResourceRegistry}. */
  static final class Builder {

    private final Map<ResourceKey, Object> resources = new HashMap<>();

    private Builder() {}

    /**
     * Registers a named resource.
     *
     * @throws IllegalArgumentException if a resource with the same type and name is already
     *     registered
     */
    <T> Builder add(Class<T> type, T instance, String name) {
      return addInternal(new ResourceKey(type, name), instance);
    }

    /**
     * Registers an unnamed resource.
     *
     * @throws IllegalArgumentException if an unnamed resource of the same type is already
     *     registered
     */
    <T> Builder add(Class<T> type, T instance) {
      return addInternal(new ResourceKey(type, null), instance);
    }

    ResourceRegistry build() {
      return new ResourceRegistry(resources);
    }

    private Builder addInternal(ResourceKey key, Object instance) {
      Object existing = resources.putIfAbsent(key, instance);
      if (existing != null) {
        String desc =
            key.name() != null
                ? "type " + key.type().getName() + " with name '" + key.name() + "'"
                : "type " + key.type().getName() + " (unnamed)";
        throw new IllegalArgumentException("Resource already registered for " + desc);
      }
      return this;
    }
  }

  /** Composite key for resource lookup: (type, optional name). */
  private record ResourceKey(Class<?> type, @Nullable String name) {

    @Override
    public String toString() {
      StringJoiner sj = new StringJoiner(", ", "ResourceKey[", "]");
      sj.add("type=" + type.getName());
      if (name != null) {
        sj.add("name='" + name + "'");
      }
      return sj.toString();
    }
  }
}
