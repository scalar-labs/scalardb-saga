package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import org.junit.jupiter.api.Test;

class NamedTest {

  @Test
  void retention_always_isRuntime() {
    // Act
    RetentionPolicy retention =
        Named.class.getAnnotation(java.lang.annotation.Retention.class).value();

    // Assert
    assertThat(retention).isEqualTo(RetentionPolicy.RUNTIME);
  }

  @Test
  void target_always_isParameter() {
    // Act
    ElementType[] targets = Named.class.getAnnotation(java.lang.annotation.Target.class).value();

    // Assert
    assertThat(targets).containsExactly(ElementType.PARAMETER);
  }

  @Test
  void value_qualifierGiven_returnsQualifier() throws NoSuchMethodException {
    // Act
    Named named =
        SampleStep.class.getConstructor(String.class).getParameters()[0].getAnnotation(Named.class);

    // Assert
    assertThat(named).isNotNull();
    assertThat(named.value()).isEqualTo("account");
  }

  /** Test fixture. */
  @SuppressWarnings("unused")
  static class SampleStep {
    public SampleStep(@Named("account") String channel) {}
  }
}
