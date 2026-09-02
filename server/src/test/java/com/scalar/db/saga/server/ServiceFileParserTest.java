package com.scalar.db.saga.server;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

import com.scalar.db.saga.server.SagaServerConfig.ServiceConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

  /**
   * The live reading path, which is the reconciler's: walk the directory, read each file through
   * the target the walk validated, parse those bytes. Going through it here is what keeps these
   * hygiene tests covering the code that actually runs.
   */
  private Map<String, ServiceConfig> parse() {
    return parse(servicesDir);
  }

  /** Warnings the parser collected on the most recent {@link #parse}. */
  private final List<String> warnings = new ArrayList<>();

  private Map<String, ServiceConfig> parse(Path directory) {
    warnings.clear();
    Map<String, ServiceConfig> services = new LinkedHashMap<>();
    ServiceFileParser.listServiceFiles(directory)
        .forEach(
            (name, file) ->
                services.put(
                    name,
                    ServiceFileParser.parseFile(
                            name,
                            file.fileName(),
                            WatchedFiles.read(
                                file.fileName(), file.target(), WatchedFiles.MAX_FILE_BYTES),
                            secrets(),
                            warnings)
                        .config()));
    return services;
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
    void parse_baseUrlOnlyGiven_parsesService() throws IOException {
      writeService("account.properties", "base_url=http://account-svc:8080\n");

      assertThat(parse())
          .containsExactly(
              entry(
                  "account",
                  new ServiceConfig("http://account-svc:8080", List.of(), 0L, Map.of())));
    }

    @Test
    void parse_multipleFilesGiven_parsesAll() throws IOException {
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
    void parseFile_allowedHostsSecretReference_isResolved() throws IOException {
      // The prefixed format resolved references in every daemon property before parsing, so the
      // file format keeps that for every setting, not just the credential-bearing ones.
      Files.writeString(secretsDir.resolve("hosts"), "account-svc,account-svc.internal");
      writeService(
          "account.properties",
          "base_url=http://account-svc:8080\n"
              + "allowed_hosts=${file:UTF-8:"
              + secretsDir.resolve("hosts")
              + "}\n");

      ServiceConfig service = requireNonNull(parse().get("account"));

      assertThat(service.allowedHosts()).containsExactly("account-svc", "account-svc.internal");
    }

    @Test
    void parseFile_maxBodyBytesSecretReference_isResolved() throws IOException {
      Files.writeString(secretsDir.resolve("cap"), "2000000");
      writeService(
          "account.properties",
          "base_url=http://account-svc:8080\n"
              + "max_body_bytes=${file:UTF-8:"
              + secretsDir.resolve("cap")
              + "}\n");

      ServiceConfig service = requireNonNull(parse().get("account"));

      assertThat(service.maxBodyBytes()).isEqualTo(2_000_000L);
    }

    @Test
    void parseFile_secretReferenceWithInvalidCharset_throwsNamingFileAndKey() throws IOException {
      // Charset.forName throws outside the resolver's own message wrapping, so the parser must add
      // the file-and-key attribution its contract promises. The message assertions are the
      // behavior under test.
      writeService(
          "account.properties",
          "base_url=http://a:1\nheader.Authorization=${file:UFT-8:/run/secrets/token}\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("account.properties")
          .hasMessageContaining("header.Authorization");
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
    void parseFile_allowedHostsEntryWithPort_throwsWithoutEchoingTheValue() throws IOException {
      // A port suffix would silently never match (the allowlist is compared against the request
      // URI's host), and the value is redacted because allowed_hosts is resolved before it is
      // checked — a secret pasted onto this key must not reach the message.
      writeService("account.properties", "base_url=http://a:1\nallowed_hosts=account-svc:8080\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("not a host name")
          .hasMessageNotContaining("account-svc:8080");
    }

    @Test
    void parseFile_allowedHostsIpv6Literal_isAccepted() throws IOException {
      // An IPv6 literal keeps its brackets, which is exactly what URI.getHost() returns.
      writeService("account.properties", "base_url=http://a:1\nallowed_hosts=[::1]\n");

      assertThat(requireNonNull(parse().get("account")).allowedHosts()).containsExactly("[::1]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo/path", "foo]", "payment_svc", "user@account-svc"})
    void parseFile_allowedHostsEntryThatIsNotAHost_throwsWithoutEchoingTheValue(String entry)
        throws IOException {
      // None of these is something URI.getHost() ever returns, so the entry could never match a
      // request: a pasted path, an unbalanced bracket, an underscored name the JDK's client cannot
      // send to at all, and a user-info prefix. The value is redacted for the same reason the port
      // case is; allowed_hosts is resolved before it is checked.
      // Arrange
      writeService("account.properties", "base_url=http://a:1\nallowed_hosts=" + entry + "\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("not a host name")
          .hasMessageNotContaining(entry);
    }

    @Test
    void parseFile_allowedHostsEntryWithAMissedComma_throwsWithoutEchoingTheValue()
        throws IOException {
      // The accident the shape check is really for: a comma dropped between two hosts leaves one
      // entry with a space in it. The escape is not what carries the space here, since this one
      // sits in the value, where Properties does not split; it is written that way to read as the
      // typo it stands for.
      // Arrange
      writeService("account.properties", "base_url=http://a:1\nallowed_hosts=payment\\ svc\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("not a host name")
          .hasMessageNotContaining("payment svc");
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
    void parseFile_headerValueWithANewline_throwsWithoutEchoingTheValue() throws IOException {
      // The realistic accident: a token pasted into a secret file with a line break in it. Only
      // the ends of a header value are trimmed, so the break survives, the JDK's client then
      // refuses every request, and each step calling this service fails permanently and
      // compensates — after a reload that reported success.
      // Arrange
      Files.writeString(secretsDir.resolve("token"), "line-one\nSECRET-line-two");
      writeService(
          "account.properties",
          "base_url=http://a:1\nheader.Authorization=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Authorization")
          .hasMessageContaining("control character")
          .hasMessageNotContaining("SECRET-line-two");
    }

    @Test
    void parseFile_headerValueAboveLatin1_throwsWithoutEchoingTheValue() throws IOException {
      // A value is not a token, so the name rule above does not reach it — but the JDK's client
      // refuses any character above U+00FF in a value just as firmly as a control character, and
      // an accepted one fails every call permanently after a pass that reported success.
      // Arrange
      Files.writeString(secretsDir.resolve("token"), "Bearer SECRET-中文");
      writeService(
          "account.properties",
          "base_url=http://a:1\nheader.Authorization=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("above U+00FF")
          .hasMessageNotContaining("SECRET");
    }

    @Test
    void parseFile_secretFileWithAByteOrderMark_throwsWithoutEchoingTheValue() throws IOException {
      // The likeliest arrival of one: an editor saves the secret with a BOM. It is not whitespace,
      // so the trim keeps it, and it is not a control character, so only the U+00FF rule catches
      // it.
      // Arrange
      Files.writeString(secretsDir.resolve("token"), "﻿Bearer SECRET-abc");
      writeService(
          "account.properties",
          "base_url=http://a:1\nheader.Authorization=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("above U+00FF")
          .hasMessageNotContaining("SECRET");
    }

    @Test
    void parseFile_headerValueAtTheLatin1Boundary_isAccepted() throws IOException {
      // The other side of the same rule: U+00FF is the last character the client will send, so the
      // check must not reach below it and reject a value that works.
      // Arrange
      writeService("account.properties", "base_url=http://a:1\nheader.X-Api-Key=abcÿ\n");

      // Act & Assert
      assertThat(requireNonNull(parse().get("account")).headers())
          .containsEntry("X-Api-Key", "abcÿ");
    }

    @Test
    void parseFile_headerNameWithAControlCharacter_throwsIllegalArgumentException()
        throws IOException {
      // Properties.load performs escape processing on keys, so the name half of the key can carry
      // one too, and an unsendable name fails every call exactly as an unsendable value does. A
      // control character is not an HTTP token character, so the token rule is what rejects it
      // here; a value is not a token, so it carries its own rule.
      // Arrange
      writeService("account.properties", "base_url=http://a:1\nheader.X-A\\u0000B=v\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseFile_secretEndingInANewline_isStillAccepted() throws IOException {
      // The trim stays: a secret file ending in a newline is the case it exists for, and the new
      // check must not take it away.
      // Arrange
      Files.writeString(secretsDir.resolve("token"), "Bearer abc\n");
      writeService(
          "account.properties",
          "base_url=http://a:1\nheader.Authorization=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");

      // Act & Assert
      assertThat(requireNonNull(parse().get("account")).headers())
          .containsEntry("Authorization", "Bearer abc");
    }

    @Test
    void parseFile_maxBodyBytesAboveTheCeiling_throwsIllegalArgumentException() throws IOException {
      // Without a ceiling a service file sets this to Long.MAX_VALUE and the coordinator's heap
      // becomes whatever a participant returns — one service file taking the daemon down for
      // every saga, not only for its own.
      // Arrange
      writeService(
          "account.properties",
          "base_url=http://a:1\nmax_body_bytes="
              + (ServiceFileParser.MAX_BODY_BYTES_CEILING + 1)
              + "\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("max_body_bytes");
    }

    @Test
    void parseFile_maxBodyBytesAtTheCeiling_isAccepted() throws IOException {
      // Arrange
      writeService(
          "account.properties",
          "base_url=http://a:1\nmax_body_bytes=" + ServiceFileParser.MAX_BODY_BYTES_CEILING + "\n");

      // Act & Assert
      assertThat(requireNonNull(parse().get("account")).maxBodyBytes())
          .isEqualTo(ServiceFileParser.MAX_BODY_BYTES_CEILING);
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
    @ValueSource(strings = {"X-Api(Key)", "X-Api,Key", "X/Key", "X@Key", "X{Key}"})
    void parseFile_headerNameThatIsNotAToken_throwsIllegalArgumentException(String header)
        throws IOException {
      // HttpRequest.Builder.header() enforces RFC 7230's token rule, so a name it refuses fails
      // every call to the service permanently and compensates. It is the same end state the
      // restricted name list exists to prevent, reached by a name nobody thought to list.
      // Arrange
      writeService("account.properties", "base_url=http://a:1\nheader." + header + "=v\n");

      // Act & Assert
      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseFile_headerNameCarryingAnEscapedSpace_throwsIllegalArgumentException()
        throws IOException {
      // An unescaped space or colon ends the key, so a name carrying one reaches this check only
      // when it is escaped. A pasted header line is not that shape: it parses as a name with the
      // rest of the line as its value, which is a separate gap this check does not close.
      // Arrange
      writeService("account.properties", "base_url=http://a:1\nheader.X\\ Api\\ Key=v\n");

      // Act & Assert
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
    void parse_sameHeaderNameOnDifferentServices_isAccepted() throws IOException {
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
    void parse_missingDirectory_throwsIllegalArgumentException() {
      assertThatThrownBy(() -> parse(servicesDir.resolve("nope")))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parse_dotEntries_areIgnored() throws IOException {
      // kubelet's ..data symlink, its timestamped directories, and ordinary dotfiles.
      writeService("account.properties", "base_url=http://a:1\n");
      Files.createDirectory(servicesDir.resolve("..2026_08_21"));
      Files.writeString(servicesDir.resolve(".hidden"), "junk");
      Files.createSymbolicLink(servicesDir.resolve("..data"), servicesDir.resolve("..2026_08_21"));

      assertThat(parse()).containsOnlyKeys("account");
    }

    @Test
    void parse_nonPropertiesFile_throwsIllegalArgumentException() throws IOException {
      writeService("account.properties", "base_url=http://a:1\n");
      Files.writeString(servicesDir.resolve("stray.txt"), "junk");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("stray.txt");
    }

    @Test
    void parse_kubeletProjectedVolumeLayout_parsesServices() throws IOException {
      // The layout kubelet mounts a ConfigMap as: the real files live in a ..<timestamp>
      // directory, ..data symlinks to it, and every visible file is a symlink through ..data.
      Path timestamped = Files.createDirectory(servicesDir.resolve("..2026_08_23_10_00_00"));
      Files.writeString(timestamped.resolve("account.properties"), "base_url=http://a:1\n");
      Files.createSymbolicLink(servicesDir.resolve("..data"), Path.of("..2026_08_23_10_00_00"));
      Files.createSymbolicLink(
          servicesDir.resolve("account.properties"), Path.of("..data", "account.properties"));

      Map<String, ServiceConfig> services = parse();

      assertThat(services).containsOnlyKeys("account");
      assertThat(requireNonNull(services.get("account")).baseUrl()).isEqualTo("http://a:1");
    }

    @Test
    void parse_symlinkEscapingServicesPath_throwsIllegalArgumentException() throws IOException {
      // A symlink out of the directory is a second route to reading an arbitrary file.
      Files.writeString(secretsDir.resolve("outside.properties"), "base_url=http://a:1\n");
      Files.createSymbolicLink(
          servicesDir.resolve("account.properties"), secretsDir.resolve("outside.properties"));

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("symlink");
    }

    @Test
    void parse_danglingSymlink_throwsIllegalArgumentException() throws IOException {
      Files.createSymbolicLink(
          servicesDir.resolve("account.properties"), servicesDir.resolve("gone.properties"));

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cannot be resolved");
    }

    @Test
    void parse_symlinkToDirectoryInsideServicesPath_throwsIllegalArgumentException()
        throws IOException {
      // Contained but not a regular file: resolving inside the directory is necessary, not
      // sufficient.
      Files.createDirectory(servicesDir.resolve("..subdir"));
      Files.createSymbolicLink(
          servicesDir.resolve("account.properties"), servicesDir.resolve("..subdir"));

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("regular file");
    }

    @Test
    void parse_symlinkWithNonPropertiesName_throwsIllegalArgumentException() throws IOException {
      // The visible name carries the service name, so the extension rule applies to it even when
      // the link target is a contained regular file.
      Files.writeString(servicesDir.resolve("account.properties"), "base_url=http://a:1\n");
      Files.createSymbolicLink(
          servicesDir.resolve("stray.txt"), servicesDir.resolve("account.properties"));

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("stray.txt");
    }

    @Test
    void parse_oversizedServiceFile_throwsIllegalArgumentException() throws IOException {
      writeService(
          "account.properties",
          "base_url=http://a:1\n# " + "x".repeat((int) WatchedFiles.MAX_FILE_BYTES) + "\n");

      assertThatThrownBy(ServiceFileParserTest.this::parse)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cap");
    }

    @Test
    void parse_invalidServiceName_throwsIllegalArgumentException() throws IOException {
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
      Map<String, ServiceConfig> services = parse();
      services.forEach(
          (name, service) ->
              ServiceFileParser.requireWithinCeiling(name, service, List.of(ceiling)));
      return services;
    }

    @Test
    void parse_allowedHostsWithinCeiling_isAccepted() throws IOException {
      writeService("a.properties", "base_url=http://a:1\nallowed_hosts=a-svc\n");

      assertThat(parseWithCeiling("a-svc", "b-svc")).containsOnlyKeys("a");
    }

    @Test
    void requireWithinCeiling_hostDifferingOnlyInCase_isAccepted() throws IOException {
      // The runtime matches hosts case-insensitively: OutboundHttpPolicy lowercases its allowlist
      // and the request URI's host. A ceiling that compared raw would reject a service over a
      // difference that never reaches the wire.
      // Arrange
      writeService("a.properties", "base_url=http://a:1\nallowed_hosts=Account-Svc\n");

      // Act & Assert
      assertThat(parseWithCeiling("account-svc")).containsOnlyKeys("a");
    }

    @Test
    void parse_allowedHostOutsideCeiling_throwsIllegalArgumentException() throws IOException {
      writeService("a.properties", "base_url=http://a:1\nallowed_hosts=evil-svc\n");

      assertThatThrownBy(() -> parseWithCeiling("a-svc"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ceiling");
    }

    @Test
    void parse_secretResolvedAllowedHostOutsideCeiling_throwsIllegalArgumentException()
        throws IOException {
      // Pins the ordering that makes the ceiling meaningful: allowed_hosts is resolved before the
      // ceiling check, so a secret-sourced host cannot smuggle egress past the operator's ceiling.
      // The message assertion distinguishes the ceiling firing from a resolution failure, which
      // would throw the same type.
      Files.writeString(secretsDir.resolve("hosts"), "evil-svc");
      writeService(
          "a.properties",
          "base_url=http://a:1\nallowed_hosts=${file:UTF-8:" + secretsDir.resolve("hosts") + "}\n");

      assertThatThrownBy(() -> parseWithCeiling("a-svc"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ceiling");
    }

    @Test
    void parse_emptyAllowedHostsUnderCeiling_throwsIllegalArgumentException() throws IOException {
      // Empty means allow-all, which is precisely what a ceiling exists to forbid.
      writeService("a.properties", "base_url=http://a:1\n");

      assertThatThrownBy(() -> parseWithCeiling("a-svc"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("allow-all");
    }
  }

  /**
   * The offline half: with a lenient resolver, a value that cannot be read stops being an error and
   * becomes a skipped check plus a warning. Every test here asserts both halves — that the parse
   * survives, and that it said what it did not check — because a skip nobody is told about is
   * exactly the failure this design was chosen to avoid.
   */
  @Nested
  class LenientResolution {

    private final List<String> lenientWarnings = new ArrayList<>();

    /** Parses one file through the resolver {@code --validate-config} uses. */
    private ServiceFileParser.ParsedService parseLeniently(String name, String content)
        throws IOException {
      String fileName = name + ServiceFileParser.PROPERTIES_EXTENSION;
      writeService(fileName, content);
      return ServiceFileParser.parseFile(
          name,
          fileName,
          Files.readAllBytes(servicesDir.resolve(fileName)),
          new LenientServiceValueResolver(secretsDir),
          lenientWarnings);
    }

    /**
     * A reference to a secret that is not on this machine, which is the whole point of leniency.
     */
    private String absentSecret() {
      return "${file:UTF-8:" + secretsDir.resolve("absent") + "}";
    }

    @Test
    void parseFile_unresolvableBaseUrlGiven_skipsTheUrlChecksAndWarns() throws IOException {
      // Act
      ServiceFileParser.ParsedService parsed = parseLeniently("a", "base_url=" + absentSecret());

      // Assert
      assertThat(parsed.unresolvedKeys()).containsExactly("base_url");
      assertThat(lenientWarnings)
          .singleElement(as(STRING))
          .contains("base_url")
          .contains("URL and host checks");
    }

    @Test
    void parseFile_unresolvableBaseUrlGiven_isAnErrorForTheDaemon() throws IOException {
      // The same file the lenient resolver tolerates must still stop a daemon that cannot read the
      // secret: it would otherwise serve a service whose address it never resolved.
      String fileName = "a" + ServiceFileParser.PROPERTIES_EXTENSION;
      writeService(fileName, "base_url=" + absentSecret() + "\n");
      byte[] content = Files.readAllBytes(servicesDir.resolve(fileName));

      assertThatThrownBy(
              () -> ServiceFileParser.parseFile("a", fileName, content, secrets(), lenientWarnings))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseFile_unresolvableHeaderValueGiven_skipsTheValueChecksAndWarns() throws IOException {
      // Act
      ServiceFileParser.ParsedService parsed =
          parseLeniently("a", "base_url=http://a:1\nheader.X-Api-Key=" + absentSecret() + "\n");

      // Assert — the header name was still checked; only its value was not.
      assertThat(parsed.unresolvedKeys()).containsExactly("header.X-Api-Key");
      assertThat(parsed.config().headers()).containsOnlyKeys("X-Api-Key");
      assertThat(lenientWarnings)
          .singleElement(as(STRING))
          .contains("header.X-Api-Key")
          .contains("header-value checks");
    }

    @Test
    void parseFile_unresolvableHeaderNameStillChecked_throws() throws IOException {
      // Leniency is about values. A header name the JDK refuses is wrong whatever its value
      // resolves to, so it must still fail here.
      assertThatThrownBy(
              () ->
                  parseLeniently(
                      "a", "base_url=http://a:1\nheader.Content-Length=" + absentSecret() + "\n"))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseFile_unresolvableAllowedHostsGiven_skipsTheHostChecksAndWarns() throws IOException {
      // Act
      ServiceFileParser.ParsedService parsed =
          parseLeniently("a", "base_url=http://a:1\nallowed_hosts=" + absentSecret() + "\n");

      // Assert — recorded by key, because the ceiling check outside this class must skip too.
      assertThat(parsed.unresolvedKeys()).containsExactly("allowed_hosts");
      assertThat(parsed.config().allowedHosts()).isEmpty();
      assertThat(lenientWarnings).singleElement(as(STRING)).contains("egress-ceiling");
    }

    @Test
    void parseFile_unresolvableMaxBodyBytesGiven_skipsTheRangeCheckAndWarns() throws IOException {
      // Act — the reference text is not a number, so an unskipped check would fail here.
      ServiceFileParser.ParsedService parsed =
          parseLeniently("a", "base_url=http://a:1\nmax_body_bytes=" + absentSecret() + "\n");

      // Assert
      assertThat(parsed.unresolvedKeys()).containsExactly("max_body_bytes");
      assertThat(lenientWarnings).singleElement(as(STRING)).contains("numeric range check");
    }

    @Test
    void parseFile_resolvableSecretsGiven_reportsNothingUnresolved() throws IOException {
      // Arrange — with the secrets present, a lenient run must be indistinguishable from a strict
      // one; leniency softens a failure, it does not skip the attempt.
      Path token = Files.writeString(secretsDir.resolve("token"), "s3cret");

      // Act
      ServiceFileParser.ParsedService parsed =
          parseLeniently(
              "a", "base_url=http://a:1\nheader.X-Api-Key=${file:UTF-8:" + token + "}\n");

      // Assert
      assertThat(parsed.unresolvedKeys()).isEmpty();
      assertThat(lenientWarnings).isEmpty();
      assertThat(parsed.config().headers()).containsExactly(entry("X-Api-Key", "s3cret"));
    }

    @Test
    void parseFile_envReferenceGiven_warnsThatItWillNotRotate() throws IOException {
      // Not an error in either mode, and not about resolution: an env-sourced value cannot change
      // in a running pod, which defeats the directory it is written in.
      String fileName = "a" + ServiceFileParser.PROPERTIES_EXTENSION;
      writeService(fileName, "base_url=http://a:1\nheader.X-Api-Key=${env:SAGA_TEST_TOKEN}\n");

      ServiceFileParser.parseFile(
          "a",
          fileName,
          Files.readAllBytes(servicesDir.resolve(fileName)),
          secrets(),
          lenientWarnings);

      assertThat(lenientWarnings)
          .singleElement(as(STRING))
          .contains("header.X-Api-Key")
          .contains("will not pick up rotation");
    }
  }
}
