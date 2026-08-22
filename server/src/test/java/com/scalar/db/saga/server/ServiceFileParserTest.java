package com.scalar.db.saga.server;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.scalar.db.saga.server.SagaServerConfig.ServiceConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ServiceFileParserTest {

  @TempDir Path servicesDir;
  @TempDir Path secretsDir;

  private ServiceSecretResolver secrets() {
    return new ServiceSecretResolver(secretsDir);
  }

  private Map<String, ServiceConfig> parse() {
    return ServiceFileParser.parseDirectory(servicesDir, secrets(), List.of());
  }

  private void writeService(String fileName, String content) throws IOException {
    Files.writeString(servicesDir.resolve(fileName), content);
  }

  // =========================================================================
  // Per-file parsing
  // =========================================================================

  @Nested
  class FileParsing {

    @Test
    void parseDirectory_baseUrlOnlyGiven_parsesService() throws IOException {
      writeService("account.properties", "base_url=http://account-svc:8080\n");

      assertThat(parse())
          .containsExactly(
              entry(
                  "account",
                  new ServiceConfig("http://account-svc:8080", List.of(), 0L, Map.of())));
    }

    @Test
    void parseDirectory_multipleFilesGiven_parsesAll() throws IOException {
      writeService("account.properties", "base_url=http://account-svc:8080\n");
      writeService("ledger.properties", "base_url=http://ledger-svc:9000\n");

      Map<String, ServiceConfig> services = parse();

      assertThat(services).containsOnlyKeys("account", "ledger");
      assertThat(requireNonNull(services.get("ledger")).baseUrl())
          .isEqualTo("http://ledger-svc:9000");
    }

    @Test
    void parseFile_everyAttributeGiven_parsesEach() throws IOException {
      writeService(
          "account.properties",
          """
          base_url=http://account-svc:8080
          allowed_hosts=account-svc, account-svc.internal
          max_body_bytes=2000000
          header.Authorization=Bearer token
          header.X-Tenant=acme
          """);

      ServiceConfig service = requireNonNull(parse().get("account"));

      assertThat(service.baseUrl()).isEqualTo("http://account-svc:8080");
      assertThat(service.allowedHosts()).containsExactly("account-svc", "account-svc.internal");
      assertThat(service.maxBodyBytes()).isEqualTo(2_000_000L);
      assertThat(service.headers())
          .containsOnly(entry("Authorization", "Bearer token"), entry("X-Tenant", "acme"));
    }

    @Test
    void parseFile_headerSecretReference_isResolvedWithinSecretsRoot() throws IOException {
      // The header value is how a daemon authenticates to a downstream service, so it must accept
      // a secret reference rather than force the credential inline in the service file.
      Files.writeString(secretsDir.resolve("downstream.token"), "Bearer resolved-token");
      writeService(
          "account.properties",
          "base_url=http://account-svc:8080\n"
              + "header.Authorization=${file:UTF-8:"
              + secretsDir.resolve("downstream.token")
              + "}\n");

      ServiceConfig service = requireNonNull(parse().get("account"));

      assertThat(service.headers())
          .containsExactly(entry("Authorization", "Bearer resolved-token"));
    }

    @Test
    void parseFile_withoutBaseUrl_throwsIllegalArgumentException() throws IOException {
      // A file without a base URL configures a service no declarative step can call.
      writeService("account.properties", "max_body_bytes=1000\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("account.properties");
    }

    @Test
    void parseFile_baseUrlWithUserInfo_throwsWithoutEchoingTheValue() throws IOException {
      // The user-info SSRF shape; the message must not echo the value, which may have resolved
      // from a secret reference.
      writeService("account.properties", "base_url=http://payment@evil.example\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("user-info")
          .hasMessageNotContaining("evil.example");
    }

    @Test
    void parseFile_blankBaseUrl_throwsIllegalArgumentException() throws IOException {
      writeService("account.properties", "base_url=   \n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseFile_unknownSetting_throwsIllegalArgumentException() throws IOException {
      writeService("account.properties", "base_url=http://account-svc:8080\ntimeout_millis=1000\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("timeout_millis");
    }

    @Test
    void parseFile_allowedHostsWithEmptyElement_throwsIllegalArgumentException()
        throws IOException {
      writeService(
          "account.properties",
          "base_url=http://account-svc:8080\nallowed_hosts=account-svc,,other\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseFile_maxBodyBytesZero_throwsIllegalArgumentException() throws IOException {
      writeService("account.properties", "base_url=http://account-svc:8080\nmax_body_bytes=0\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  // =========================================================================
  // Header guards (moved with the parser from the prefixed-key format)
  // =========================================================================

  @Nested
  class HeaderGuards {

    @Test
    void parseFile_headerWithoutName_throwsIllegalArgumentException() throws IOException {
      writeService("account.properties", "base_url=http://a:1\nheader.=x\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"X-Saga-Id", "X-Saga-Step", "X-Saga-Callback-Url", "x-saga-id"})
    void parseFile_engineReservedHeaderGiven_throwsIllegalArgumentException(String header)
        throws IOException {
      // The engine stamps these itself and its value always wins; the lower-cased spelling is in
      // the list because HTTP header names are case-insensitive.
      writeService("account.properties", "base_url=http://a:1\nheader." + header + "=x\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Connection", "Content-Length", "Expect", "Host", "Upgrade", "host"})
    void parseFile_jdkRestrictedHeaderGiven_throwsIllegalArgumentException(String header)
        throws IOException {
      // HttpRequest.Builder.header() throws on these, so the engine cannot build the request at
      // all and every call to the service fails permanently. Accepting the key would defer that to
      // the first outbound call, long after startup reported healthy.
      writeService("account.properties", "base_url=http://a:1\nheader." + header + "=x\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseFile_headersDifferingOnlyInCase_throwsIllegalArgumentException() throws IOException {
      writeService(
          "account.properties", "base_url=http://a:1\nheader.X-Tenant=a\nheader.x-tenant=b\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseDirectory_sameHeaderNameOnDifferentServices_isAccepted() throws IOException {
      writeService("a.properties", "base_url=http://a:1\nheader.Authorization=Bearer a\n");
      writeService("b.properties", "base_url=http://b:1\nheader.Authorization=Bearer b\n");

      assertThat(parse()).containsOnlyKeys("a", "b");
    }
  }

  // =========================================================================
  // jdkRestrictedHeaders (moved with the parser)
  // =========================================================================

  @Nested
  class JdkRestrictedHeaders {

    @Test
    void jdkRestrictedHeaders_nullGiven_returnsAllFiveRestrictedNames() {
      Set<String> restricted = ServiceFileParser.jdkRestrictedHeaders(null);

      assertThat(restricted)
          .containsExactlyInAnyOrder("Connection", "Content-Length", "Expect", "Host", "Upgrade");
    }

    @Test
    void jdkRestrictedHeaders_nameGiven_omitsThatNameCaseInsensitively() {
      Set<String> restricted = ServiceFileParser.jdkRestrictedHeaders("HOST");

      // The JDK removes from a case-insensitively ordered set, so the spelling in the property
      // does not have to match the canonical one.
      assertThat(restricted).doesNotContain("Host", "host");
      assertThat(restricted).contains("Connection");
    }

    @Test
    void jdkRestrictedHeaders_commaSeparatedNamesGiven_omitsAllOfThem() {
      Set<String> restricted = ServiceFileParser.jdkRestrictedHeaders("host,connection");

      assertThat(restricted).containsExactlyInAnyOrder("Content-Length", "Expect", "Upgrade");
    }

    @Test
    void jdkRestrictedHeaders_spaceAfterCommaGiven_keepsTheNameFollowingTheSpace() {
      Set<String> restricted = ServiceFileParser.jdkRestrictedHeaders("host, connection");

      // Mirrors the JDK, which trims the whole value once and then splits on commas without
      // trimming the tokens; " connection" therefore matches nothing. Trimming here instead would
      // accept a config key that the JDK still rejects at send time, which is the failure this
      // check prevents.
      assertThat(restricted).doesNotContain("Host");
      assertThat(restricted).contains("Connection");
    }

    @Test
    void jdkRestrictedHeaders_unrelatedNameGiven_omitsNothing() {
      Set<String> restricted = ServiceFileParser.jdkRestrictedHeaders("X-Nonsense");

      assertThat(restricted)
          .containsExactlyInAnyOrder("Connection", "Content-Length", "Expect", "Host", "Upgrade");
    }
  }

  // =========================================================================
  // Directory hygiene
  // =========================================================================

  @Nested
  class DirectoryHygiene {

    @Test
    void parseDirectory_missingDirectory_throwsIllegalArgumentException() {
      assertThatThrownBy(
              () ->
                  ServiceFileParser.parseDirectory(
                      servicesDir.resolve("nope"), secrets(), List.of()))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseDirectory_dotEntries_areIgnored() throws IOException {
      // kubelet's ..data symlink, its timestamped directories, and ordinary dotfiles.
      writeService("account.properties", "base_url=http://a:1\n");
      Files.createDirectory(servicesDir.resolve("..2026_08_21"));
      Files.writeString(servicesDir.resolve(".hidden"), "junk");
      Files.createSymbolicLink(servicesDir.resolve("..data"), servicesDir.resolve("..2026_08_21"));

      assertThat(parse()).containsOnlyKeys("account");
    }

    @Test
    void parseDirectory_nonPropertiesFile_throwsIllegalArgumentException() throws IOException {
      writeService("account.properties", "base_url=http://a:1\n");
      Files.writeString(servicesDir.resolve("stray.txt"), "junk");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("stray.txt");
    }

    @Test
    void parseDirectory_symlinkedServiceFile_throwsIllegalArgumentException() throws IOException {
      // A symlink under a service's name is a second route to reading an arbitrary file.
      Files.writeString(secretsDir.resolve("outside.properties"), "base_url=http://a:1\n");
      Files.createSymbolicLink(
          servicesDir.resolve("account.properties"), secretsDir.resolve("outside.properties"));

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("symlink");
    }

    @Test
    void parseDirectory_oversizedServiceFile_throwsIllegalArgumentException() throws IOException {
      writeService(
          "account.properties",
          "base_url=http://a:1\n# " + "x".repeat((int) ServiceFileParser.MAX_FILE_BYTES) + "\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cap");
    }

    @Test
    void parseDirectory_invalidServiceName_throwsIllegalArgumentException() throws IOException {
      writeService("bad name!.properties", "base_url=http://a:1\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  // =========================================================================
  // Egress ceiling
  // =========================================================================

  @Nested
  class EgressCeiling {

    private Map<String, ServiceConfig> parseWithCeiling(String... ceiling) {
      return ServiceFileParser.parseDirectory(servicesDir, secrets(), List.of(ceiling));
    }

    @Test
    void parseDirectory_allowedHostsWithinCeiling_isAccepted() throws IOException {
      writeService("a.properties", "base_url=http://a:1\nallowed_hosts=a-svc\n");

      assertThat(parseWithCeiling("a-svc", "b-svc")).containsOnlyKeys("a");
    }

    @Test
    void parseDirectory_allowedHostOutsideCeiling_throwsIllegalArgumentException()
        throws IOException {
      writeService("a.properties", "base_url=http://a:1\nallowed_hosts=evil-svc\n");

      assertThatThrownBy(() -> parseWithCeiling("a-svc"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ceiling");
    }

    @Test
    void parseDirectory_emptyAllowedHostsUnderCeiling_throwsIllegalArgumentException()
        throws IOException {
      // Empty means allow-all, which is precisely what a ceiling exists to forbid.
      writeService("a.properties", "base_url=http://a:1\n");

      assertThatThrownBy(() -> parseWithCeiling("a-svc"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("allow-all");
    }
  }
}
