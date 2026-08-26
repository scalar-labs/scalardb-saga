package com.scalar.db.saga.server;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.transport.HttpEndpointRegistrar;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigReconcilerTest {

  private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

  @TempDir Path servicesDir;
  @TempDir Path definitionsDir;
  @TempDir Path secretsDir;

  private final List<Map<String, HttpServiceConfig>> swaps = new ArrayList<>();
  private final HttpEndpointRegistrar registrar = services -> swaps.add(services);
  private final List<SagaDefinition> registered = new ArrayList<>();
  private Consumer<SagaDefinition> definitionRegistrar = definition -> registered.add(definition);

  /**
   * A store with the real one's two load-bearing properties: append-only, and latest means most
   * recently FIRST registered. Re-registering a version that already exists is a no-op that does
   * not move it to the front, which is what makes a rolled-back definition file leave the newer
   * version serving. A LinkedHashSet models exactly that — adding an existing element does not
   * change iteration order.
   */
  private final Map<String, LinkedHashSet<String>> stored = new LinkedHashMap<>();

  private final DefinitionStore definitionStore =
      new DefinitionStore() {
        @Override
        public void register(SagaDefinition definition) {
          definitionRegistrar.accept(definition);
          stored
              .computeIfAbsent(definition.getName(), name -> new LinkedHashSet<>())
              .add(definition.getVersion());
        }

        @Override
        public boolean isRegistered(String sagaName, String version) {
          LinkedHashSet<String> versions = stored.get(sagaName);
          return versions != null && versions.contains(version);
        }

        @Override
        public @Nullable String latestVersion(String sagaName) {
          LinkedHashSet<String> versions = stored.get(sagaName);
          if (versions == null || versions.isEmpty()) {
            return null;
          }
          String latest = null;
          for (String version : versions) {
            latest = version;
          }
          return latest;
        }
      };

  private ConfigReconciler reconciler() {
    return reconciler(false);
  }

  private ConfigReconciler reconciler(boolean asyncCallbacksConfigured) {
    ReloadConfig reloadConfig =
        new ReloadConfig(servicesDir, 10, secretsDir, List.of(), Clock.fixed(NOW, ZoneOffset.UTC));
    return new ConfigReconciler(
        reloadConfig, definitionsDir, asyncCallbacksConfigured, registrar, definitionStore);
  }

  /** A clock a test can step, for the timestamps a fixed clock cannot tell apart. */
  private static final class SteppingClock extends Clock {
    private Instant instant = NOW;

    void advance(Duration amount) {
      instant = instant.plus(amount);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }

  private ConfigReconciler reconciler(Clock clock) {
    return new ConfigReconciler(
        new ReloadConfig(servicesDir, 10, secretsDir, List.of(), clock),
        definitionsDir,
        false,
        registrar,
        definitionStore);
  }

  private void writeService(String name, String content) throws IOException {
    Files.writeString(servicesDir.resolve(name + ".properties"), content);
  }

  private void writeDefinition(String fileName, String sagaName, String version, String service)
      throws IOException {
    writeDefinition(fileName, sagaName, version, service, "/x");
  }

  private void writeDefinition(
      String fileName, String sagaName, String version, String service, String path)
      throws IOException {
    Files.writeString(
        definitionsDir.resolve(fileName),
        "{\"name\":\""
            + sagaName
            + "\",\"version\":\""
            + version
            + "\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\",\"service\":\""
            + service
            + "\",\"execution\":{\"method\":\"POST\",\"path\":\""
            + path
            + "\"},\"compensation\":{\"method\":\"POST\",\"path\":\"/undo\"}}]}");
  }

  // =========================================================================
  // Apply
  // =========================================================================

  @Nested
  class Apply {

    @Test
    void run_firstPass_swapsServicesAndRegistersDefinitions() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");

      // Act
      boolean applied = reconciler().run();

      // Assert
      assertThat(applied).isTrue();
      assertThat(swaps).hasSize(1);
      assertThat(swaps.get(0)).containsOnlyKeys("account");
      assertThat(registered).hasSize(1);
      assertThat(registered.get(0).getName()).isEqualTo("order-saga");
    }

    @Test
    void run_secondPassUnchanged_doesNothing() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      swaps.clear();
      registered.clear();

      // Act
      boolean applied = reconciler.run();

      // Assert — no swap, no registration: unchanged files cost no store round-trip
      assertThat(applied).isTrue();
      assertThat(swaps).isEmpty();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_headerRotation_swapsWithNewHeaders() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\nheader.Authorization=Bearer old\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      writeService("account", "base_url=http://account:8080\nheader.Authorization=Bearer new\n");

      // Act
      reconciler.run();

      // Assert — the second swap carries the rotated header (the endpoint manager decides that
      // this is an in-place value swap; the pass just hands it the full candidate set)
      assertThat(swaps).hasSize(2);
      assertThat(requireNonNull(swaps.get(1).get("account")).defaultHeaders())
          .containsEntry("Authorization", "Bearer new");
    }

    @Test
    void run_rotatedSecretFile_reachesTheSwapWithoutAnyWatchedFileChange() throws IOException {
      // Arrange — the header references a mounted secret; rotating the SECRET (not the service
      // file) must still propagate, because references resolve once per pass.
      Files.writeString(secretsDir.resolve("token"), "Bearer old");
      writeService(
          "account",
          "base_url=http://account:8080\nheader.Authorization=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.writeString(secretsDir.resolve("token"), "Bearer new");

      // Act
      reconciler.run();

      // Assert
      assertThat(swaps).hasSize(2);
      assertThat(requireNonNull(swaps.get(1).get("account")).defaultHeaders())
          .containsEntry("Authorization", "Bearer new");
    }

    @Test
    void run_yamlDefinitionFile_parsesAndApplies() throws IOException {
      // The reconciler dispatches json/yaml itself now (it parses the bytes it hashed rather than
      // re-opening the file), so both formats need pinning here.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      Files.writeString(
          definitionsDir.resolve("saga.yaml"),
          """
          name: yaml-saga
          version: "1.0"
          mode: SAGA
          steps:
            - name: s
              service: account
              execution:
                method: POST
                path: /x
              compensation:
                method: POST
                path: /undo
          """);

      // Act & Assert
      assertThat(reconciler().run()).isTrue();
      assertThat(registered).hasSize(1);
      assertThat(registered.get(0).getName()).isEqualTo("yaml-saga");
    }

    @Test
    void run_changedDefinitionWithBumpedVersion_registersIt() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      writeDefinition("saga.json", "order-saga", "2.0", "account", "/x2");

      // Act
      boolean applied = reconciler.run();

      // Assert
      assertThat(applied).isTrue();
      assertThat(registered).hasSize(2);
      assertThat(registered.get(1).getVersion()).isEqualTo("2.0");
    }
  }

  // =========================================================================
  // Validation rejections
  // =========================================================================

  @Nested
  class Rejections {

    @Test
    void run_invalidServiceFile_rejectsWholePassIncludingValidDefinitions() throws IOException {
      // Arrange — all-or-nothing: a bad service file must also hold back the (valid) definitions
      writeService("account", "base_url=   \n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");

      // Act
      boolean applied = reconciler().run();

      // Assert
      assertThat(applied).isFalse();
      assertThat(swaps).isEmpty();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_errorsInTwoFiles_aggregatesBothInTheRejection() throws IOException {
      // Arrange — team A's mistake must not hide team B's
      writeService("account", "base_url=   \n");
      writeService("ledger", "base_url=http://ledger:9000\nbogus_key=x\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();

      // Act & Assert — one WARN naming both files
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        reconciler.run();

        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.WARN);
                  assertThat(event.getFormattedMessage()).contains("account.properties");
                  assertThat(event.getFormattedMessage()).contains("ledger");
                });
      }
    }

    @Test
    void run_danglingServiceReference_rejectsNamingDefinitionAndService() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "payment");
      ConfigReconciler reconciler = reconciler();

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = reconciler.run();

        assertThat(applied).isFalse();
        assertThat(registered).isEmpty();
        assertThat(logs.events())
            .anySatisfy(
                event ->
                    assertThat(event.getFormattedMessage())
                        .contains("saga.json")
                        .contains("payment"));
      }
    }

    @Test
    void run_unBumpedDefinitionChange_rejectsNamingTheFile() throws IOException {
      // Arrange — same version, different content
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      writeDefinition("saga.json", "order-saga", "1.0", "account", "/changed");

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = reconciler.run();

        assertThat(applied).isFalse();
        assertThat(registered).hasSize(1); // only the original registration
        assertThat(logs.events())
            .anySatisfy(
                event ->
                    assertThat(event.getFormattedMessage()).contains("saga.json").contains("bump"));
      }
    }

    @Test
    void run_rejectionThenFix_appliesOnTheNextPass() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "payment"); // dangling
      ConfigReconciler reconciler = reconciler();
      assertThat(reconciler.run()).isFalse();
      writeDefinition("saga.json", "order-saga", "1.0", "account"); // fixed

      // Act & Assert — self-heals without any state reset
      assertThat(reconciler.run()).isTrue();
      assertThat(registered).hasSize(1);
    }

    @Test
    void run_emptyServicesAfterNonEmpty_rejectsAsSuspectedMountFailure() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.delete(servicesDir.resolve("account.properties"));

      // Act & Assert — the definition still references the service, but even without that, the
      // empty transition alone must reject
      assertThat(reconciler.run()).isFalse();
    }

    @Test
    void run_emptyDefinitionsAfterNonEmpty_rejectsAsSuspectedMountFailure() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.delete(definitionsDir.resolve("saga.json"));

      // Act & Assert
      assertThat(reconciler.run()).isFalse();
    }

    @Test
    void run_allowedHostsLoosenedToEmpty_rejects() throws IOException {
      // Arrange — a service that restricted egress must not silently loosen to allow-all
      writeService("account", "base_url=http://account:8080\nallowed_hosts=account\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      writeService("account", "base_url=http://account:8080\n");

      // Act & Assert
      assertThat(reconciler.run()).isFalse();
    }

    @Test
    void run_duplicateSagaNameAcrossFiles_rejects() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "order-saga", "1.0", "account");
      writeDefinition("b.json", "order-saga", "2.0", "account");

      // Act & Assert
      assertThat(reconciler().run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_definitionSymlinkEscapingTheDirectory_rejects() throws IOException {
      // Arrange — a symlink escaping definitions_path is a second route to arbitrary files
      writeService("account", "base_url=http://account:8080\n");
      Files.writeString(secretsDir.resolve("outside.json"), "{}");
      Files.createSymbolicLink(
          definitionsDir.resolve("saga.json"), secretsDir.resolve("outside.json"));

      // Act & Assert
      assertThat(reconciler().run()).isFalse();
    }

    @Test
    void run_singleFileDefinitionsPathWithUnknownExtension_rejects() throws IOException {
      // The directory walk filters by extension, so a single configured file is the only way an
      // unknown extension reaches the parser. It must be refused, not guessed at as YAML.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      Path file = definitionsDir.resolve("defs.txt");
      Files.writeString(file, "{\"name\":\"x\",\"mode\":\"SAGA\",\"steps\":[]}");
      ReloadConfig reloadConfig =
          new ReloadConfig(
              servicesDir, 10, secretsDir, List.of(), Clock.fixed(NOW, ZoneOffset.UTC));
      ConfigReconciler reconciler =
          new ConfigReconciler(reloadConfig, file, false, registrar, definitionStore);

      // Act & Assert
      assertThat(reconciler.run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_singleFileDefinitionsPathThatIsASymlink_isRead() throws IOException {
      // definitions_path may name a single file, and a ConfigMap that mounts one key publishes it
      // as a symlink (kubelet's ..data indirection). Reading it must follow that link.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      Path data = Files.createDirectory(definitionsDir.resolve("data"));
      Files.writeString(
          data.resolve("saga.json"),
          "{\"name\":\"linked-saga\",\"version\":\"1.0\",\"mode\":\"SAGA\",\"steps\":"
              + "[{\"name\":\"s\",\"service\":\"account\","
              + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
              + "\"compensation\":{\"method\":\"POST\",\"path\":\"/undo\"}}]}");
      Path link = definitionsDir.resolve("mounted.json");
      Files.createSymbolicLink(link, data.resolve("saga.json"));
      ReloadConfig reloadConfig =
          new ReloadConfig(
              servicesDir, 10, secretsDir, List.of(), Clock.fixed(NOW, ZoneOffset.UTC));
      ConfigReconciler reconciler =
          new ConfigReconciler(reloadConfig, link, false, registrar, definitionStore);

      // Act & Assert
      assertThat(reconciler.run()).isTrue();
      assertThat(registered).extracting(SagaDefinition::getName).containsExactly("linked-saga");
    }

    @Test
    void run_definitionSymlinkResolvingInsideTheDirectory_isAccepted() throws IOException {
      // Arrange — kubelet publishes every visible file of a mounted volume as a symlink through
      // its ..data indirection, so a contained symlink is the expected shape, not an anomaly
      writeService("account", "base_url=http://account:8080\n");
      Path data = Files.createDirectory(definitionsDir.resolve("..data"));
      Files.writeString(
          data.resolve("saga.json"),
          "{\"name\":\"order-saga\",\"version\":\"1.0\",\"mode\":\"SAGA\",\"steps\":"
              + "[{\"name\":\"s\",\"service\":\"account\","
              + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
              + "\"compensation\":{\"method\":\"POST\",\"path\":\"/undo\"}}]}");
      Files.createSymbolicLink(definitionsDir.resolve("saga.json"), data.resolve("saga.json"));

      // Act & Assert
      assertThat(reconciler().run()).isTrue();
      assertThat(registered).hasSize(1);
    }

    @Test
    void run_baseUrlWithUserInfo_rejectedAtValidationWithoutEchoingTheUrl() throws IOException {
      // Arrange — the user-info SSRF shape (http://svc@evil resolves to evil). Boot rejects this
      // through the orchestrator builder; the reload path must reject it at validation too, and
      // the message must not echo the URL (a base_url may resolve from a secret reference).
      writeService("account", "base_url=http://payment@evil.example\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = reconciler.run();

        assertThat(applied).isFalse();
        assertThat(swaps).isEmpty();
        assertThat(registered).isEmpty();
        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getFormattedMessage()).contains("user-info");
                  assertThat(event.getFormattedMessage()).doesNotContain("evil.example");
                });
      }
    }

    @Test
    void run_secretValuedAllowedHostsUnderCeiling_rejectedWithoutEchoingTheValue()
        throws IOException {
      // allowed_hosts is resolved before it is checked, so a secret reference pasted onto it
      // arrives as plaintext. The rejection is logged every pass, and logs are readable far more
      // widely than the secret — so the value must never appear in one.
      // Arrange
      Files.writeString(secretsDir.resolve("token"), "SUPER-SECRET-VALUE");
      writeService(
          "account",
          "base_url=http://account:8080\nallowed_hosts=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ReloadConfig withCeiling =
          new ReloadConfig(
              servicesDir, 10, secretsDir, List.of("account"), Clock.fixed(NOW, ZoneOffset.UTC));
      ConfigReconciler reconciler =
          new ConfigReconciler(withCeiling, definitionsDir, false, registrar, definitionStore);

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(reconciler.run()).isFalse();

        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("SUPER-SECRET-VALUE"));
      }
      assertThat(requireNonNull(reconciler.status().rejection()).reason())
          .doesNotContain("SUPER-SECRET-VALUE");
    }

    @Test
    void run_servicesPathNotADirectory_rejectedWithoutEchoingThePath() throws IOException {
      // services_path is a resolved value like any other, so a secret reference pasted onto that
      // key arrives as plaintext — and the directory walk's own failures are the messages most
      // likely to quote it back.
      // Arrange
      Path notADirectory = secretsDir.resolve("SUPER-SECRET-VALUE");
      Files.writeString(notADirectory, "");
      ReloadConfig misconfigured =
          new ReloadConfig(
              notADirectory, 10, secretsDir, List.of(), Clock.fixed(NOW, ZoneOffset.UTC));
      ConfigReconciler reconciler =
          new ConfigReconciler(misconfigured, definitionsDir, false, registrar, definitionStore);

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(reconciler.run()).isFalse();

        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("SUPER-SECRET-VALUE"));
      }
      assertThat(requireNonNull(reconciler.status().rejection()).reason())
          .contains(SagaServerConfig.SERVICES_PATH_KEY)
          .doesNotContain("SUPER-SECRET-VALUE");
    }

    @Test
    void run_propertyKeyContainingANewline_cannotForgeASeparateLogRecord() throws IOException {
      // Properties.load performs escape processing on keys, so a key written as a\nb arrives
      // carrying a real newline. The rejection quotes the key back, and an unflattened newline
      // would render the rest of it as what reads as its own log record.
      // Arrange — one bad key, so the rejection carries exactly one problem
      writeService("account", "base_url=http://account:8080\nWARN\\nforged-record=x\n");

      // Act & Assert — the header line plus the one problem, and nothing the key could add
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(reconciler().run()).isFalse();

        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.WARN);
                  assertThat(event.getFormattedMessage()).contains("forged-record");
                  assertThat(event.getFormattedMessage().lines()).hasSize(2);
                });
      }
    }

    @Test
    void run_secretValuedAllowedHostsWithNoCeiling_rejectedAtValidationWithoutEchoingTheValue()
        throws IOException {
      // The path that needs no ceiling at all: a secret containing a colon reads as a port
      // suffix. Validating the shape here is what stops it reaching the engine, whose own
      // rejection names the host and which this module cannot redact.
      // Arrange
      Files.writeString(secretsDir.resolve("token"), "user:PASSWORD-hunter2");
      writeService(
          "account",
          "base_url=http://account:8080\nallowed_hosts=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");

      // Act & Assert — no ceiling configured
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(reconciler().run()).isFalse();

        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("PASSWORD-hunter2"));
      }
      assertThat(swaps).isEmpty();
    }

    @Test
    void run_asyncStepWithoutCallbackConfig_rejectedAtValidation() throws IOException {
      // Arrange — an async phase on a daemon without callback configuration could never be
      // provisioned: registration would fail on every pass, so validation rejects it up front.
      writeService("account", "base_url=http://account:8080\n");
      writeAsyncDefinition();

      // Act & Assert
      assertThat(reconciler().run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_asyncStepWithCallbackConfig_applies() throws IOException {
      // Arrange — the same definition on a daemon WITH async completion configured
      writeService("account", "base_url=http://account:8080\n");
      writeAsyncDefinition();

      // Act & Assert
      assertThat(reconciler(true).run()).isTrue();
      assertThat(registered).hasSize(1);
    }

    private void writeAsyncDefinition() throws IOException {
      Files.writeString(
          definitionsDir.resolve("async.json"),
          "{\"name\":\"async-saga\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\","
              + "\"service\":\"account\","
              + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\",\"async\":true},"
              + "\"compensation\":{\"method\":\"POST\",\"path\":\"/undo\"}}]}");
    }

    @Test
    void run_oversizedDefinitionFile_rejects() throws IOException {
      // The cap is enforced on the bytes actually read. Before the single-read change the parse
      // re-opened the file and read it unbounded, so only a racy size check stood in the way.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      Files.writeString(
          definitionsDir.resolve("big.json"),
          "{\"_pad\":\"" + "x".repeat((int) WatchedFiles.MAX_FILE_BYTES) + "\"}");

      // Act & Assert
      assertThat(reconciler().run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_definitionFileWithInvalidUtf8_rejects() throws IOException {
      // The bytes are decoded strictly once; an undecodable file is a rejection, not a mangled
      // definition.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      Files.write(definitionsDir.resolve("bad.json"), new byte[] {(byte) 0xC3, (byte) 0x28});

      // Act & Assert
      assertThat(reconciler().run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void runOrThrow_validationError_throwsWithAggregatedMessage() throws IOException {
      // Arrange
      writeService("account", "base_url=   \n");

      // Act & Assert — the boot entry rethrows instead of returning false
      assertThatThrownBy(() -> reconciler().runOrThrow())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("account.properties");
    }
  }

  // =========================================================================
  // Apply-phase bookkeeping
  // =========================================================================

  @Nested
  class ApplyBookkeeping {

    @Test
    void run_registrationFailsTransiently_retryRegistersOnlyTheRemainder() throws IOException {
      // Arrange — two definitions; the store rejects the second one once (transient)
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "account");
      List<String> failOnce = new ArrayList<>(List.of("saga-b"));
      definitionRegistrar =
          definition -> {
            if (failOnce.remove(definition.getName())) {
              throw new IllegalStateException("store hiccup");
            }
            registered.add(definition);
          };
      ConfigReconciler reconciler = reconciler();

      // Act — first pass fails mid-apply, second retries
      boolean first = reconciler.run();
      swaps.clear();
      boolean second = reconciler.run();

      // Assert — per-artifact bookkeeping: the already-registered definition is not re-registered,
      // the failed one is retried, and the service swap is not repeated
      assertThat(first).isFalse();
      assertThat(second).isTrue();
      assertThat(registered)
          .extracting(SagaDefinition::getName)
          .containsExactly("saga-a", "saga-b");
      assertThat(swaps).isEmpty();
    }

    @Test
    void run_serviceSwapCommitsThenRegistrationFails_statusAndAuditRecordTheSwap()
        throws IOException {
      // A rejected pass is not always a no-op: the endpoints are already live when a registration
      // fails, so the status must name the applied service set and the audit line must record it.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      definitionRegistrar =
          definition -> {
            throw new IllegalStateException("store hiccup");
          };
      ConfigReconciler reconciler = reconciler();
      String beforeServicesSha = reconciler.status().appliedServicesSha256();

      // Act
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(reconciler.run()).isFalse();

        // Assert — an audit line for the committed swap, alongside the rejection
        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.INFO);
                  assertThat(event.getFormattedMessage())
                      .contains("Config applied")
                      .contains("+account");
                });
      }
      // Assert — the status names the service set that is actually serving, plus the rejection
      ReloadStatus status = reconciler.status();
      assertThat(status.appliedServicesSha256()).isNotEqualTo(beforeServicesSha);
      assertThat(requireNonNull(status.rejection()).reason()).contains("saga-a");
    }

    @Test
    void run_partialDefinitionApply_auditsTheRegistrationsThatCommitted() throws IOException {
      // The first definition registers, the second fails: the committed one is live fleet-wide,
      // so it belongs in the audit trail even though the pass is rejected.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "account");
      definitionRegistrar =
          definition -> {
            if (definition.getName().equals("saga-b")) {
              throw new IllegalStateException("store hiccup");
            }
            registered.add(definition);
          };
      ConfigReconciler reconciler = reconciler();

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(reconciler.run()).isFalse();

        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.INFO);
                  assertThat(event.getFormattedMessage())
                      .contains("Config applied")
                      .contains("saga-a:1.0");
                });
      }
      // The definitions hash stays behind: the candidate set is not what is applied.
      assertThat(reconciler.status().appliedDefinitionsSha256()).isNull();
    }

    @Test
    void run_oneDefinitionFailsPermanently_othersStillRegister() throws IOException {
      // A permanent conflict on one definition must not hold every other definition hostage. The
      // file names decide iteration order, so "a-stuck" is attempted before "z-new": if the loop
      // abandons the pass on the first failure, the unrelated new saga never registers — not
      // "retried next pass" but skipped on every pass, for as long as the conflict persists.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a-stuck.json", "saga-stuck", "1.0", "account");
      writeDefinition("z-new.json", "saga-new", "1.0", "account");
      definitionRegistrar =
          definition -> {
            if (definition.getName().equals("saga-stuck")) {
              throw SagaDefinitionException.versionContentConflict("saga-stuck", "1.0");
            }
            registered.add(definition);
          };
      ConfigReconciler reconciler = reconciler();

      // Act — several passes, as the scheduler would run
      reconciler.run();
      reconciler.run();

      // Assert — the unrelated saga is live despite the stuck one
      assertThat(registered).extracting(SagaDefinition::getName).containsExactly("saga-new");
    }

    @Test
    void run_registrationFailsWhileAnotherDefinitionVanished_warnsOnlyOnce() throws IOException {
      // The vanished-name cleanup used to sit after the registration loop, so an unrelated
      // failure skipped it and the "deleting retires nothing" warning re-fired every pass.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.delete(definitionsDir.resolve("a.json"));
      writeDefinition("c.json", "saga-c", "1.0", "account");
      definitionRegistrar =
          definition -> {
            if (definition.getName().equals("saga-c")) {
              throw new IllegalStateException("store hiccup");
            }
            registered.add(definition);
          };

      // Act — the pass that sees the vanish also fails an unrelated registration, then another
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        reconciler.run();
        long afterFirst =
            logs.events().stream()
                .filter(e -> e.getFormattedMessage().contains("saga-a"))
                .filter(e -> e.getFormattedMessage().contains("retires nothing"))
                .count();
        reconciler.run();
        long afterSecond =
            logs.events().stream()
                .filter(e -> e.getFormattedMessage().contains("saga-a"))
                .filter(e -> e.getFormattedMessage().contains("retires nothing"))
                .count();

        // Assert — warned once, not once per pass, despite the ongoing unrelated failure
        assertThat(afterFirst).isEqualTo(1);
        assertThat(afterSecond).isEqualTo(1);
      }
    }

    @Test
    void run_permanentVersionConflict_namesItAsUnretryable() throws IOException {
      // Arrange — the store reports same-version-different-content: retrying cannot fix it
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      definitionRegistrar =
          definition -> {
            throw SagaDefinitionException.versionContentConflict("saga-a", "1.0");
          };
      ConfigReconciler reconciler = reconciler();

      // Act & Assert — the status and the WARN distinguish the permanent case from a generic
      // rejection
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        reconciler.run();

        assertThat(logs.events())
            .anySatisfy(
                event ->
                    assertThat(event.getFormattedMessage())
                        .contains("permanent")
                        .contains("bump the version"));
      }
      ReloadStatus.Rejection rejection = requireNonNull(reconciler.status().rejection());
      assertThat(rejection.reason()).contains("permanent");
      // The same distinction as a field, so an endpoint never has to match on the prose
      assertThat(rejection.operatorActionRequired()).isTrue();
    }
  }

  // =========================================================================
  // Logging and status
  // =========================================================================

  @Nested
  class LoggingAndStatus {

    @Test
    void run_repeatedIdenticalFailure_warnsOnceThenDebug() throws IOException {
      // Arrange
      writeService("account", "base_url=   \n");
      ConfigReconciler reconciler = reconciler();

      // Act & Assert — one WARN for the state change, DEBUG for the repeats
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        reconciler.run();
        reconciler.run();
        reconciler.run();

        assertThat(logs.events().stream().filter(e -> e.getLevel() == Level.WARN)).hasSize(1);
      }
    }

    @Test
    void run_recoveryAfterFailure_logsInfoAndClearsRejection() throws IOException {
      // Arrange
      writeService("account", "base_url=   \n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      writeService("account", "base_url=http://account:8080\n");

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        reconciler.run();

        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.INFO);
                  assertThat(event.getFormattedMessage()).contains("recovered");
                });
      }
      assertThat(reconciler.status().rejection()).isNull();
    }

    @Test
    void run_contentEmbeddingAnotherFilesFraming_hashesDifferently() throws IOException {
      // The digest is what an operator greps across replicas, so two different mounted sets must
      // not report the same one. Without a length in the framing these two do: the stream carries
      // no boundary between files, and set B's single file spells out set A's second file exactly
      // — name, separator, content. Both sets parse, because the embedded text sits in a comment,
      // and the walk sorts account before payment, so the two byte streams line up.
      // Arrange — set A: two service files, the first ending mid-comment
      String nul = String.valueOf((char) 0);
      writeService("account", "base_url=http://a:1\n#");
      writeService("payment", "base_url=http://p:1\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler first = reconciler();
      assertThat(first.run()).isTrue();
      String setA = requireNonNull(first.status().appliedServicesSha256());

      // Arrange — set B: one file whose comment carries the other's framing
      Files.delete(servicesDir.resolve("payment.properties"));
      writeService(
          "account", "base_url=http://a:1\n#payment.properties" + nul + "base_url=http://p:1\n");

      // Act
      ConfigReconciler second = reconciler();
      assertThat(second.run()).isTrue();

      // Assert
      assertThat(second.status().appliedServicesSha256()).isNotEqualTo(setA);
    }

    @Test
    void run_kubeletGenerationFlipBetweenPasses_isNotMistakenForATornSnapshot() throws IOException {
      // The torn-snapshot check compares where each file resolved, and under kubelet's layout
      // every file resolves through ..data — so an ordinary ConfigMap update moves all of them.
      // A check that could not tell that from a tear would reject every legitimate update.
      // Arrange — the layout kubelet mounts a ConfigMap as, on its first generation
      Path first = Files.createDirectory(servicesDir.resolve("..2026_08_26_10_00"));
      Files.writeString(first.resolve("account.properties"), "base_url=http://account-v1:8080\n");
      Files.createSymbolicLink(servicesDir.resolve("..data"), Path.of("..2026_08_26_10_00"));
      Files.createSymbolicLink(
          servicesDir.resolve("account.properties"), Path.of("..data", "account.properties"));
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      assertThat(reconciler.run()).isTrue();
      assertThat(requireNonNull(swaps.get(0).get("account")).baseUrl())
          .isEqualTo("http://account-v1:8080");

      // Act — a second generation, published the way kubelet does: write it, then flip ..data
      Path second = Files.createDirectory(servicesDir.resolve("..2026_08_26_10_05"));
      Files.writeString(second.resolve("account.properties"), "base_url=http://account-v2:8080\n");
      Path staged = servicesDir.resolve("..data_tmp");
      Files.createSymbolicLink(staged, Path.of("..2026_08_26_10_05"));
      Files.move(staged, servicesDir.resolve("..data"), StandardCopyOption.REPLACE_EXISTING);

      // Assert — the update applies; the new generation is not read as a torn read of the old
      assertThat(reconciler.run()).isTrue();
      assertThat(requireNonNull(swaps.get(swaps.size() - 1).get("account")).baseUrl())
          .isEqualTo("http://account-v2:8080");
      assertThat(reconciler.status().rejection()).isNull();
    }

    @Test
    void run_appliedStatus_hashesStableAcrossIdenticalPasses() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();

      // Act
      reconciler.run();
      ReloadStatus first = reconciler.status();
      reconciler.run();
      ReloadStatus second = reconciler.status();

      // Assert — the greppable fleet-comparison property: same files, same hashes
      assertThat(first.appliedServicesSha256()).isEqualTo(second.appliedServicesSha256());
      assertThat(first.appliedDefinitionsSha256()).isEqualTo(second.appliedDefinitionsSha256());
      assertThat(first.rejection()).isNull();
    }

    @Test
    void run_serviceRemovedWhileAVanishedDefinitionStillNeedsIt_warnsNamingBoth()
        throws IOException {
      // Deleting a definition file retires nothing: the registered version stays startable. So a
      // later pass removing one of its services strands it, and the candidate cross-check cannot
      // see that — the definition has no file left to be a candidate.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeService("ledger", "base_url=http://ledger:9000\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "ledger");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.delete(definitionsDir.resolve("a.json"));
      reconciler.run(); // the vanish warning fires here
      Files.delete(servicesDir.resolve("account.properties"));

      // Act — removing the service the vanished saga still needs
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = reconciler.run();

        // Assert — allowed (refusing would leave no way to retire a service), but named
        assertThat(applied).isTrue();
        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.WARN);
                  assertThat(event.getFormattedMessage()).contains("account").contains("saga-a");
                });
      }
    }

    @Test
    void run_afterTheRemovalPass_theStrandedWarningDoesNotRepeat() throws IOException {
      // The warning is armed by the applied set still holding the service. Once the swap commits,
      // the condition can no longer hold, so the warning must not become a per-pass drumbeat.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeService("ledger", "base_url=http://ledger:9000\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "ledger");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.delete(definitionsDir.resolve("a.json"));
      reconciler.run();
      Files.delete(servicesDir.resolve("account.properties"));
      reconciler.run(); // the removal pass — warns

      // Act — every later pass over the same, now-settled state
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        reconciler.run();
        reconciler.run();

        // Assert
        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("still references it"));
      }
    }

    @Test
    void run_serviceRemovedWithNoVanishedReference_doesNotWarn() throws IOException {
      // A service removed while nothing registered needs it is an ordinary change.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeService("ledger", "base_url=http://ledger:9000\n");
      writeDefinition("b.json", "saga-b", "1.0", "ledger");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.delete(servicesDir.resolve("account.properties"));

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(reconciler.run()).isTrue();

        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("still references it"));
      }
    }

    @Test
    void run_noOpPass_keepsAppliedAtButAdvancesLastPassAt() throws IOException {
      // appliedAt answers "when did this replica last apply a change", so routine verification
      // must not masquerade as an apply. lastPassAt answers the other question — is anything still
      // verifying? — and a wedged pass thread is exactly a replica where it stops advancing while
      // everything else looks current.
      // Arrange
      SteppingClock clock = new SteppingClock();
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler(clock);
      reconciler.run();
      Instant appliedAt = reconciler.status().appliedAt();
      clock.advance(Duration.ofMinutes(5));

      // Act — a pass over unchanged files
      reconciler.run();

      // Assert
      assertThat(reconciler.status().appliedAt()).isEqualTo(appliedAt);
      assertThat(reconciler.status().lastPassAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    @Test
    void status_beforeTheFirstPass_isEmptyRatherThanSentinelled() {
      // Act
      ReloadStatus status = reconciler().status();

      // Assert — an endpoint serializing this must not publish a hash or a timestamp that was
      // never real
      assertThat(status.appliedServicesSha256()).isNull();
      assertThat(status.appliedDefinitionsSha256()).isNull();
      assertThat(status.appliedAt()).isNull();
      assertThat(status.lastPassAt()).isNull();
      assertThat(status.rejection()).isNull();
    }

    @Test
    void run_rejectedPass_advancesLastPassAtAndRecordsWhoMustAct() throws IOException {
      // A rejection is a concluded pass: the thread came back. Nothing else on the status moves,
      // so without lastPassAt a rejecting replica and a wedged one look alike.
      // Arrange
      SteppingClock clock = new SteppingClock();
      writeService("account", "base_url=   \n");
      ConfigReconciler reconciler = reconciler(clock);
      clock.advance(Duration.ofMinutes(2));

      // Act
      assertThat(reconciler.run()).isFalse();

      // Assert — a malformed file waits for an operator; retrying it forever changes nothing
      ReloadStatus status = reconciler.status();
      assertThat(status.lastPassAt()).isEqualTo(NOW.plus(Duration.ofMinutes(2)));
      assertThat(requireNonNull(status.rejection()).operatorActionRequired()).isTrue();
    }

    @Test
    void run_transientRegistrationFailure_marksTheRejectionAsSelfHealing() throws IOException {
      // A store outage clears on its own, and the next pass retries the same candidate set. An
      // alert that woke someone for this would be waking them for nothing.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      definitionRegistrar =
          definition -> {
            throw SagaPersistenceException.storeUnavailable(new IOException("connection reset"));
          };
      ConfigReconciler reconciler = reconciler();

      // Act
      assertThat(reconciler.run()).isFalse();

      // Assert
      assertThat(requireNonNull(reconciler.status().rejection()).operatorActionRequired())
          .isFalse();
      assertThat(requireNonNull(reconciler.status().rejection()).reason())
          .contains("will retry next pass");
    }

    @Test
    void run_permanentStoreFailure_marksTheRejectionAsNeedingAnOperator() throws IOException {
      // Same exception type as the outage above, opposite verdict: the code says whether it is
      // retryable, so a serialization failure is not reported as a blip that will pass.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      definitionRegistrar =
          definition -> {
            throw SagaPersistenceException.serializationFailed(new IOException("bad json"));
          };
      ConfigReconciler reconciler = reconciler();

      // Act
      assertThat(reconciler.run()).isFalse();

      // Assert
      assertThat(requireNonNull(reconciler.status().rejection()).operatorActionRequired()).isTrue();
    }

    @Test
    void run_definitionTheEngineRefusesToBuild_marksTheRejectionAsNeedingAnOperator()
        throws IOException {
      // register() resolves the plan before it persists, so a definition the engine will not build
      // fails here rather than at validation — and fails identically on every pass. Reporting it
      // as "will retry" leaves an operator waiting for something that cannot happen.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      definitionRegistrar =
          definition -> {
            throw SagaDefinitionException.definitionInvalid(definition.getName(), "unbuildable");
          };
      ConfigReconciler reconciler = reconciler();

      // Act
      assertThat(reconciler.run()).isFalse();

      // Assert
      ReloadStatus.Rejection rejection = requireNonNull(reconciler.status().rejection());
      assertThat(rejection.operatorActionRequired()).isTrue();
      assertThat(rejection.reason()).contains("permanent").doesNotContain("will retry");
    }

    @Test
    void run_registrationFailsWithAnUnexpectedException_marksTheRejectionAsNeedingAnOperator()
        throws IOException {
      // A bug in this process is the case the old default got most wrong: it never clears, and
      // "will retry next pass" told an operator to wait for it anyway.
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      definitionRegistrar =
          definition -> {
            throw new NullPointerException("bug");
          };
      ConfigReconciler reconciler = reconciler();

      // Act
      assertThat(reconciler.run()).isFalse();

      // Assert — and the pass still collects rather than aborting, so other definitions are not
      // held hostage by it
      assertThat(requireNonNull(reconciler.status().rejection()).operatorActionRequired()).isTrue();
    }

    @Test
    void run_definitionFileRolledBack_rejectedRatherThanTrackedAsApplied() throws IOException {
      // A rollback of the definitions directory reverts the file and not the store: registering
      // older content is a no-op and the newer version keeps serving. Recording the file's version
      // as applied would leave every check reasoning about a version nobody runs.
      // Arrange — the real history: v1 shipped, then v2 replaced it. Re-registering v1 later is
      // then a no-op, which is the whole reason a rollback does not take effect.
      writeService("account", "base_url=http://account:8080\n");
      writeService("legacy", "base_url=http://legacy:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      assertThat(reconciler.run()).isTrue();
      writeDefinition("saga.json", "order-saga", "2.0", "legacy");
      assertThat(reconciler.run()).isTrue();

      // Act — the file reverts to v1; the store still serves v2
      writeDefinition("saga.json", "order-saga", "1.0", "account");

      // Assert
      assertThat(reconciler.run()).isFalse();
      ReloadStatus.Rejection rejection = requireNonNull(reconciler.status().rejection());
      assertThat(rejection.reason())
          .contains("2.0 is what the store serves")
          .contains("register it as a NEW, higher version");
      assertThat(rejection.operatorActionRequired()).isTrue();
    }

    @Test
    void run_rolledBackDefinitionThenServiceRemoval_removalIsNotApplied() throws IOException {
      // The hazard the check exists for. With the file rolled back to v1, a pass that also drops
      // the service only the SERVING v2 needs would sail through the cross-check — v1 does not
      // reference it — and starts of v2 would then fail to resolve an endpoint on every replica.
      // Arrange — v1 shipped, then v2 replaced it and moved the saga onto the legacy service
      writeService("account", "base_url=http://account:8080\n");
      writeService("legacy", "base_url=http://legacy:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      assertThat(reconciler.run()).isTrue();
      writeDefinition("saga.json", "order-saga", "2.0", "legacy");
      assertThat(reconciler.run()).isTrue();
      int swapsBefore = swaps.size();

      // Act — roll the definition back to a version that does not name legacy, and drop legacy
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      Files.delete(servicesDir.resolve("legacy.properties"));

      // Assert — the whole pass is rejected, so the service the serving version needs stays live
      assertThat(reconciler.run()).isFalse();
      assertThat(swaps).hasSize(swapsBefore);
    }

    @Test
    void run_vanishedDefinitionFile_warnsThatDeletionRetiresNothing() throws IOException {
      // Arrange — two definitions so the empty-transition guard does not fire; one file vanishes
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "account");
      ConfigReconciler reconciler = reconciler();
      reconciler.run();
      Files.delete(definitionsDir.resolve("b.json"));

      // Act & Assert — a warning, not a rejection: the saga remains registered and startable
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = reconciler.run();

        assertThat(applied).isTrue();
        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.WARN);
                  assertThat(event.getFormattedMessage()).contains("saga-b");
                });
      }
    }
  }
}
