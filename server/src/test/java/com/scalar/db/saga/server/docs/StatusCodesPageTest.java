package com.scalar.db.saga.server.docs;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.exception.SagaErrorCode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StatusCodesPageTest {

  // Gradle runs a module's tests from the module directory.
  private static final Path PAGE = Path.of("..").resolve(StatusCodesPage.PATH);

  @Test
  void entries_everyUnreservedCode_hasExactlyOneEntry() {
    // Arrange
    Set<SagaErrorCode> expected =
        EnumSet.complementOf(EnumSet.copyOf(StatusCodeReference.RESERVED));

    // Act
    Set<SagaErrorCode> documented = StatusCodeReference.ENTRIES.keySet();

    // Assert
    assertThat(documented)
        .as("every SagaErrorCode needs a StatusCodeReference entry or a RESERVED place")
        .isEqualTo(expected);
  }

  @Test
  void entries_noWireStatus_carriesANote() {
    for (var e : StatusCodeReference.ENTRIES.entrySet()) {
      // Arrange
      StatusCodeReference.Entry entry = e.getValue();

      // Act
      boolean onTheWire = !entry.http().isEmpty() || !entry.grpc().isEmpty();

      // Assert
      assertThat(onTheWire || entry.note() != null)
          .as("%s is on neither transport, so its note has to say where it appears", e.getKey())
          .isTrue();
    }
  }

  @Test
  void render_committedPage_isCurrent() throws IOException {
    // Arrange
    String committed = Files.readString(PAGE, StandardCharsets.UTF_8);

    // Act
    String rendered = StatusCodesPage.render();

    // Assert
    assertThat(committed)
        .as(
            "%s is stale; run ./gradlew :server:generateStatusCodes and commit the result",
            StatusCodesPage.PATH)
        .isEqualTo(rendered);
  }
}
