package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ResourceRegistryTest {

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  @Test
  void build_withSingleUnnamedResource_returnsRegistry() {
    // Arrange & Act
    ResourceRegistry registry = ResourceRegistry.newBuilder().add(String.class, "hello").build();

    // Assert
    assertThat(registry.isEmpty()).isFalse();
  }

  @Test
  void build_withNamedResources_returnsRegistry() {
    // Arrange & Act
    ResourceRegistry registry =
        ResourceRegistry.newBuilder()
            .add(String.class, "hello", "first")
            .add(String.class, "world", "second")
            .build();

    // Assert
    assertThat(registry.isEmpty()).isFalse();
  }

  @Test
  void build_withNoResources_returnsEmptyRegistry() {
    // Arrange & Act
    ResourceRegistry registry = ResourceRegistry.newBuilder().build();

    // Assert
    assertThat(registry.isEmpty()).isTrue();
  }

  @Test
  void build_withDuplicateTypeAndName_throwsIllegalArgumentException() {
    // Arrange
    ResourceRegistry.Builder builder =
        ResourceRegistry.newBuilder().add(String.class, "first", "dup");

    // Act & Assert
    assertThatThrownBy(() -> builder.add(String.class, "second", "dup"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withDuplicateUnnamedType_throwsIllegalArgumentException() {
    // Arrange
    ResourceRegistry.Builder builder = ResourceRegistry.newBuilder().add(String.class, "first");

    // Act & Assert
    assertThatThrownBy(() -> builder.add(String.class, "second"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // get
  // ---------------------------------------------------------------------------

  @Test
  void get_typeAndNameGiven_returnsMatchingResource() {
    // Arrange
    ResourceRegistry registry =
        ResourceRegistry.newBuilder()
            .add(String.class, "hello", "first")
            .add(String.class, "world", "second")
            .build();

    // Act & Assert
    assertThat(registry.get(String.class, "first")).isEqualTo("hello");
    assertThat(registry.get(String.class, "second")).isEqualTo("world");
  }

  @Test
  void get_typeOnlyGivenWithUniqueType_returnsResource() {
    // Arrange
    ResourceRegistry registry = ResourceRegistry.newBuilder().add(String.class, "only-one").build();

    // Act
    Object result = registry.get(String.class, null);

    // Assert
    assertThat(result).isEqualTo("only-one");
  }

  @Test
  void get_unnamedLookupWithOnlyNamedResources_throwsIllegalArgumentException() {
    // Arrange — only named resources registered, no unnamed resource of this type
    ResourceRegistry registry =
        ResourceRegistry.newBuilder().add(String.class, "the-value", "named").build();

    // Act & Assert — unnamed lookup does not fall back to named resources
    assertThatThrownBy(() -> registry.get(String.class, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void get_missingType_throwsIllegalArgumentException() {
    // Arrange
    ResourceRegistry registry = ResourceRegistry.newBuilder().build();

    // Act & Assert
    assertThatThrownBy(() -> registry.get(String.class, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void get_missingName_throwsIllegalArgumentException() {
    // Arrange
    ResourceRegistry registry =
        ResourceRegistry.newBuilder().add(String.class, "value", "existing").build();

    // Act & Assert
    assertThatThrownBy(() -> registry.get(String.class, "nonexistent"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // contains
  // ---------------------------------------------------------------------------

  @Test
  void contains_existingNamedResource_returnsTrue() {
    // Arrange
    ResourceRegistry registry =
        ResourceRegistry.newBuilder().add(String.class, "value", "name").build();

    // Act & Assert
    assertThat(registry.contains(String.class, "name")).isTrue();
  }

  @Test
  void contains_existingTypeUnnamed_returnsTrue() {
    // Arrange
    ResourceRegistry registry = ResourceRegistry.newBuilder().add(String.class, "value").build();

    // Act & Assert
    assertThat(registry.contains(String.class, null)).isTrue();
  }

  @Test
  void contains_missingType_returnsFalse() {
    // Arrange
    ResourceRegistry registry = ResourceRegistry.newBuilder().build();

    // Act & Assert
    assertThat(registry.contains(String.class, null)).isFalse();
  }

  @Test
  void contains_unnamedLookupWithOnlyNamedResources_returnsFalse() {
    // Arrange — only named resource of this type, no unnamed registration
    ResourceRegistry registry =
        ResourceRegistry.newBuilder().add(String.class, "value", "named").build();

    // Act & Assert — unnamed contains does not match named resources
    assertThat(registry.contains(String.class, null)).isFalse();
  }
}
