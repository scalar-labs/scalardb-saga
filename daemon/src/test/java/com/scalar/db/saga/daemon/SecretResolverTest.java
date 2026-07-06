package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SecretResolverTest {

  private final SecretResolver resolver = new SecretResolver();

  @Test
  void resolve_plainValueGiven_returnsUnchanged() {
    // Act
    String resolved = resolver.resolve("plain-value");

    // Assert
    assertThat(resolved).isEqualTo("plain-value");
  }

  @Test
  void resolve_fileReferenceGiven_returnsFileContents(@TempDir Path dir) throws IOException {
    // Arrange
    Path secret = dir.resolve("secret");
    Files.writeString(secret, "s3cr3t-value", StandardCharsets.UTF_8);

    // Act
    String resolved = resolver.resolve("${file:UTF-8:" + secret + "}");

    // Assert
    assertThat(resolved).isEqualTo("s3cr3t-value");
  }

  @Test
  void resolve_referenceEmbeddedInText_substitutesInPlace(@TempDir Path dir) throws IOException {
    // Arrange
    Path secret = dir.resolve("pw");
    Files.writeString(secret, "hunter2", StandardCharsets.UTF_8);

    // Act
    String resolved =
        resolver.resolve("jdbc:postgresql://db/app?password=${file:UTF-8:" + secret + "}");

    // Assert
    assertThat(resolved).isEqualTo("jdbc:postgresql://db/app?password=hunter2");
  }

  @Test
  void resolve_secretContainingReferenceSyntax_isNotRecursivelyReExpanded(@TempDir Path dir)
      throws IOException {
    // Arrange — a resolved secret must be treated literally. The outer file's contents are
    // themselves a ${file:...} reference; if resolution recursed into the value, it would follow
    // that reference and leak the inner secret instead of returning the outer file verbatim.
    Path inner = dir.resolve("inner");
    Files.writeString(inner, "inner-secret", StandardCharsets.UTF_8);
    Path outer = dir.resolve("outer");
    String nestedReference = "${file:UTF-8:" + inner + "}";
    Files.writeString(outer, nestedReference, StandardCharsets.UTF_8);

    // Act
    String resolved = resolver.resolve("${file:UTF-8:" + outer + "}");

    // Assert — the outer reference resolves exactly once, to the raw file contents.
    assertThat(resolved).isEqualTo(nestedReference);
  }

  @Test
  void resolve_envReferenceGiven_returnsEnvValue() {
    // Arrange — use whatever environment variable the JVM actually has, for determinism.
    Map<String, String> env = System.getenv();
    assumeTrue(!env.isEmpty(), "no environment variables available to exercise ${env:...}");
    Map.Entry<String, String> any = env.entrySet().iterator().next();

    // Act
    String resolved = resolver.resolve("${env:" + any.getKey() + "}");

    // Assert
    assertThat(resolved).isEqualTo(any.getValue());
  }

  @Test
  void resolve_undefinedEnvVarGiven_leftVerbatim() {
    // Act
    String resolved = resolver.resolve("${env:SAGA_SECRET_RESOLVER_SURELY_MISSING_VAR}");

    // Assert — an undefined variable is not disruptive; the reference is left as-is.
    assertThat(resolved).isEqualTo("${env:SAGA_SECRET_RESOLVER_SURELY_MISSING_VAR}");
  }

  @Test
  void resolve_scriptReferenceGiven_leftVerbatim() {
    // Arrange — the ${script:...} RCE vector of CVE-2022-42889 must not be reachable.
    String scriptRef = "${script:javascript:java.lang.Runtime.getRuntime()}";

    // Act
    String resolved = resolver.resolve(scriptRef);

    // Assert — the prefix is not in the allowlist, so it is left verbatim (never evaluated).
    assertThat(resolved).isEqualTo(scriptRef);
  }

  @Test
  void resolve_urlReferenceGiven_leftVerbatim() {
    // Arrange — the ${url:...} SSRF vector must not be reachable (no fetch is attempted).
    String urlRef = "${url:UTF-8:http://169.254.169.254/latest/meta-data/}";

    // Act
    String resolved = resolver.resolve(urlRef);

    // Assert
    assertThat(resolved).isEqualTo(urlRef);
  }

  @Test
  void resolve_dnsReferenceGiven_leftVerbatim() {
    // Arrange — the ${dns:...} exfiltration vector must not be reachable.
    String dnsRef = "${dns:address|example.com}";

    // Act
    String resolved = resolver.resolve(dnsRef);

    // Assert
    assertThat(resolved).isEqualTo(dnsRef);
  }
}
