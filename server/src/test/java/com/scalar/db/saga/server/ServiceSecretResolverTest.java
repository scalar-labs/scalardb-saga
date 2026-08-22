package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ServiceSecretResolverTest {

  @TempDir Path secretsDir;
  @TempDir Path outsideDir;

  private ServiceSecretResolver resolver() {
    return new ServiceSecretResolver(secretsDir);
  }

  @Test
  void resolve_noReference_returnsValueUnchanged() {
    assertThat(resolver().resolve("plain-value")).isEqualTo("plain-value");
  }

  @Test
  void resolve_fileInsideSecretsRoot_returnsContents() throws IOException {
    Files.writeString(secretsDir.resolve("token"), "s3cr3t");

    String resolved = resolver().resolve("${file:UTF-8:" + secretsDir.resolve("token") + "}");

    assertThat(resolved).isEqualTo("s3cr3t");
  }

  @Test
  void resolve_fileOutsideSecretsRoot_throwsNamingThePathNotTheContents() throws IOException {
    // The confinement this class exists for: a service file cannot read arbitrary
    // process-readable files by pointing ${file:...} at them.
    Files.writeString(outsideDir.resolve("stolen"), "outside-content");

    assertThatThrownBy(
            () -> resolver().resolve("${file:UTF-8:" + outsideDir.resolve("stolen") + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outside secrets_root")
        .hasMessageNotContaining("outside-content");
  }

  @Test
  void resolve_symlinkEscapingSecretsRoot_throws() throws IOException {
    // A symlink INSIDE the root pointing outside it: the check runs after symlink resolution, so
    // the link's real target decides, not its location.
    Files.writeString(outsideDir.resolve("stolen"), "outside-content");
    Files.createSymbolicLink(secretsDir.resolve("innocent"), outsideDir.resolve("stolen"));

    assertThatThrownBy(
            () -> resolver().resolve("${file:UTF-8:" + secretsDir.resolve("innocent") + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outside secrets_root")
        .hasMessageNotContaining("outside-content");
  }

  @Test
  void resolve_missingFile_throwsWithoutContents() {
    assertThatThrownBy(() -> resolver().resolve("${file:UTF-8:" + secretsDir.resolve("nope") + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be read");
  }

  @Test
  void resolve_directoryTarget_throws() throws IOException {
    Path dir = Files.createDirectory(secretsDir.resolve("subdir"));

    assertThatThrownBy(() -> resolver().resolve("${file:UTF-8:" + dir + "}"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void resolve_oversizedFile_throws() throws IOException {
    Path big = secretsDir.resolve("big");
    Files.writeString(big, "x".repeat((int) ServiceSecretResolver.MAX_SECRET_FILE_BYTES + 1));

    assertThatThrownBy(() -> resolver().resolve("${file:UTF-8:" + big + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cap");
  }

  @Test
  void resolve_fileReferenceWithoutCharset_throwsNamingTheExpectedForm() {
    assertThatThrownBy(() -> resolver().resolve("${file:/run/secrets/token}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("<charset>:<path>");
  }

  @Test
  void resolve_envReference_resolvesFromEnvironment() {
    // PATH exists in any test environment; the parser (not this class) warns about env use.
    assertThat(resolver().resolve("${env:PATH}")).isEqualTo(System.getenv("PATH"));
  }

  @Test
  void resolve_unknownPrefix_leftVerbatim() {
    // Matching SecretResolver: the dangerous prefixes (script/url/dns) are not registered, so the
    // reference stays literal instead of executing.
    assertThat(resolver().resolve("${script:javascript:1+1}"))
        .isEqualTo("${script:javascript:1+1}");
  }

  @Test
  void resolve_secretContentsWithReferenceSyntax_notReinterpreted() throws IOException {
    // A password containing ${...} must arrive verbatim, and a secret that reads like a lookup
    // must not trigger one.
    Files.writeString(secretsDir.resolve("tricky"), "pa$$${env:HOME}word");

    String resolved = resolver().resolve("${file:UTF-8:" + secretsDir.resolve("tricky") + "}");

    assertThat(resolved).isEqualTo("pa$$${env:HOME}word");
  }
}
