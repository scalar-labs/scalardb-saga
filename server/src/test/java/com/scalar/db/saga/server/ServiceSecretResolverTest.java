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
  void resolve_fileOutsideSecretsRoot_doesNotEchoTheConfiguredRoot() throws IOException {
    // secrets_root is a scalar.db.saga.* value like any other, so it may itself have been written
    // as a ${file:...} reference and arrive here as the secret's plaintext. These messages reach
    // the reload WARN on every pass that rejects.
    // Arrange
    Path rootNamedLikeASecret = Files.createDirectory(secretsDir.resolve("SUPER-SECRET-VALUE"));
    Files.writeString(outsideDir.resolve("stolen"), "outside-content");
    ServiceSecretResolver resolver = new ServiceSecretResolver(rootNamedLikeASecret);

    // Act & Assert
    assertThatThrownBy(() -> resolver.resolve("${file:UTF-8:" + outsideDir.resolve("stolen") + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.SECRETS_ROOT_KEY)
        .hasMessageNotContaining("SUPER-SECRET-VALUE");
  }

  @Test
  void resolve_unresolvableSecretsRoot_doesNotEchoTheConfiguredRoot() {
    // The root itself missing is the case whose own cause carries it: a filesystem exception's
    // message is the path it failed on.
    // Arrange
    Path missing = secretsDir.resolve("SUPER-SECRET-VALUE");
    ServiceSecretResolver resolver = new ServiceSecretResolver(missing);

    // Act & Assert
    assertThatThrownBy(() -> resolver.resolve("${file:UTF-8:" + missing.resolve("token") + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("SUPER-SECRET-VALUE");
  }

  @Test
  void resolve_noReference_returnsValueUnchanged() {
    assertThat(resolver().resolve("plain-value").value()).isEqualTo("plain-value");
  }

  @Test
  void resolve_fileInsideSecretsRoot_returnsContents() throws IOException {
    Files.writeString(secretsDir.resolve("token"), "s3cr3t");

    String resolved =
        resolver().resolve("${file:UTF-8:" + secretsDir.resolve("token") + "}").value();

    assertThat(resolved).isEqualTo("s3cr3t");
  }

  @Test
  void resolve_fileOutsideSecretsRoot_throwsNamingThePathNotTheContents() throws IOException {
    // The confinement this class exists for: a service file cannot read arbitrary
    // process-readable files by pointing ${file:...} at them.
    Files.writeString(outsideDir.resolve("stolen"), "outside-content");

    assertThatThrownBy(
            () -> resolver().resolve("${file:UTF-8:" + outsideDir.resolve("stolen") + "}"))
        .isInstanceOf(PermanentReferenceException.class)
        .hasMessageContaining("resolves outside")
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
        .hasMessageContaining("resolves outside")
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
    assertThat(resolver().resolve("${env:PATH}").value()).isEqualTo(System.getenv("PATH"));
  }

  @Test
  void resolve_unknownPrefix_leftVerbatim() {
    // Matching SecretResolver: the dangerous prefixes (script/url/dns) are not registered, so the
    // reference stays literal instead of executing.
    assertThat(resolver().resolve("${script:javascript:1+1}").value())
        .isEqualTo("${script:javascript:1+1}");
  }

  @Test
  void resolve_secretContentsWithReferenceSyntax_notReinterpreted() throws IOException {
    // A password containing ${...} must arrive verbatim, and a secret that reads like a lookup
    // must not trigger one.
    Files.writeString(secretsDir.resolve("tricky"), "pa$$${env:HOME}word");

    String resolved =
        resolver().resolve("${file:UTF-8:" + secretsDir.resolve("tricky") + "}").value();

    assertThat(resolved).isEqualTo("pa$$${env:HOME}word");
  }

  @Test
  void resolve_pathReachingTheRootThroughASymlinkedAncestor_returnsContents() throws IOException {
    // The shape a container has: /var/run is a symlink to /run, so /var/run/secrets/token and
    // /run/secrets/token are one file. Comparing the paths as written cannot see that; only
    // resolving them can. A check that got this wrong would refuse a secret the daemon reads
    // today, failing boot and freezing every later reload on the last configuration it applied.
    // Arrange
    Path root = Files.createDirectories(secretsDir.resolve("run/secrets"));
    Files.createDirectory(secretsDir.resolve("var"));
    Files.createSymbolicLink(secretsDir.resolve("var/run"), secretsDir.resolve("run"));
    Files.writeString(root.resolve("token"), "s3cr3t");
    Path throughTheLink = secretsDir.resolve("var/run/secrets/token");

    // Act
    String resolved =
        new ServiceSecretResolver(root).resolve("${file:UTF-8:" + throughTheLink + "}").value();

    // Assert
    assertThat(resolved).isEqualTo("s3cr3t");
  }

  @Test
  void resolve_unknownCharsetGiven_throwsPermanentReferenceException() throws IOException {
    // Wrong wherever it runs, so it must not be softened by a caller that tolerates a secret this
    // machine happens not to have.
    // Arrange
    Path token = Files.writeString(secretsDir.resolve("token"), "s3cr3t");

    // Act & Assert
    assertThatThrownBy(() -> resolver().resolve("${file:NOSUCH-CHARSET:" + token + "}"))
        .isInstanceOf(PermanentReferenceException.class);
  }
}
