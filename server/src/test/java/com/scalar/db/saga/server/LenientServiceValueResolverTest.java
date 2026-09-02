package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.server.ServiceValueResolver.Resolution;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code --validate-config} resolver. What matters here is the line it draws: a secret that is
 * merely absent from this machine is tolerated, because the tool is meant to run where the secrets
 * are not mounted, while a reference reaching outside the secrets root stays fatal — that is a
 * mistake in the file, not a property of where the check happens to run.
 */
class LenientServiceValueResolverTest {

  @TempDir Path secretsDir;
  @TempDir Path elsewhere;

  private LenientServiceValueResolver resolver() {
    return new LenientServiceValueResolver(secretsDir);
  }

  @Test
  void resolve_plainValueGiven_returnsItUnchanged() {
    // Act
    Resolution resolution = resolver().resolve("http://account:8080");

    // Assert
    assertThat(resolution.value()).isEqualTo("http://account:8080");
    assertThat(resolution.unresolvedReason()).isNull();
  }

  @Test
  void resolve_readableSecretGiven_resolvesLikeTheStrictResolver() throws IOException {
    // Arrange
    Path token = Files.writeString(secretsDir.resolve("token"), "s3cret");
    String reference = "${file:UTF-8:" + token + "}";

    // Act
    Resolution resolution = resolver().resolve(reference);

    // Assert — leniency is invisible when the secret is there.
    assertThat(resolution.value()).isEqualTo("s3cret");
    assertThat(resolution.unresolvedReason()).isNull();
    assertThat(new ServiceSecretResolver(secretsDir).resolve(reference).value())
        .isEqualTo(resolution.value());
  }

  @Test
  void resolve_secretFileNotOnThisMachine_returnsUnresolvedMarkerNamingTheReference() {
    // Arrange
    String reference = "${file:UTF-8:" + secretsDir.resolve("absent") + "}";

    // Act
    Resolution resolution = resolver().resolve(reference);

    // Assert — the reference text stands in, and the reason says why nothing better could.
    assertThat(resolution.value()).isEqualTo(reference);
    assertThat(resolution.unresolvedReason()).isNotNull().contains("absent");
  }

  @Test
  void resolve_referenceEscapingTheSecretsRoot_throws() throws IOException {
    // Arrange
    Path outside = Files.writeString(elsewhere.resolve("token"), "s3cret");

    // Act & Assert — not softened: this file would be read on a real daemon too.
    assertThatThrownBy(() -> resolver().resolve("${file:UTF-8:" + outside + "}"))
        .isInstanceOf(PermanentReferenceException.class);
  }

  @Test
  void resolve_secretsRootMissingAltogether_returnsUnresolvedMarker() {
    // Arrange — the ordinary laptop case: the mount point does not exist, and the reference points
    // inside it, which is what makes this a fact about the machine rather than about the file.
    Path root = secretsDir.resolve("not-mounted");
    LenientServiceValueResolver missingRoot = new LenientServiceValueResolver(root);

    // Act
    Resolution resolution = missingRoot.resolve("${file:UTF-8:" + root.resolve("token") + "}");

    // Assert
    assertThat(resolution.unresolvedReason()).isNotNull();
  }

  @Test
  void resolve_referenceMissingItsCharset_throws() {
    // ${file:<path>} without a charset is malformed wherever it runs: the daemon rejects it before
    // touching the filesystem. Softening it would let --validate-config pass a file that can never
    // start a server, which is the one thing it exists to prevent.
    assertThatThrownBy(() -> resolver().resolve("${file:/run/secrets/token}"))
        .isInstanceOf(PermanentReferenceException.class);
  }

  @Test
  void resolve_referenceWithAnUnknownCharset_throws() throws IOException {
    // Arrange — the file is present, so nothing here is environment-dependent.
    Path token = Files.writeString(secretsDir.resolve("token"), "s3cret");

    // Act & Assert
    assertThatThrownBy(() -> resolver().resolve("${file:NOSUCH-CHARSET:" + token + "}"))
        .isInstanceOf(PermanentReferenceException.class);
  }

  @Test
  void resolve_escapingReferenceWithTheSecretsRootUnmounted_throws() throws IOException {
    // Arrange — the laptop shape: the root is not mounted, so it cannot be resolved to compare
    // against. The escape is still an escape, and is still what this check exists to catch.
    Files.writeString(elsewhere.resolve("token"), "s3cret");
    LenientServiceValueResolver missingRoot =
        new LenientServiceValueResolver(secretsDir.resolve("not-mounted"));

    // Act & Assert
    assertThatThrownBy(
            () -> missingRoot.resolve("${file:UTF-8:" + elsewhere.resolve("token") + "}"))
        .isInstanceOf(PermanentReferenceException.class);
  }
}
