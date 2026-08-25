package com.scalar.db.saga.server;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.transport.HttpEndpointRegistrar;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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

  private ConfigReconciler pass() {
    return pass(Map.of());
  }

  private ConfigReconciler pass(Map<String, SagaServerConfig.ServiceConfig> seed) {
    return pass(seed, false);
  }

  private ConfigReconciler pass(
      Map<String, SagaServerConfig.ServiceConfig> seed, boolean asyncCallbacksConfigured) {
    ReloadConfig reloadConfig =
        new ReloadConfig(servicesDir, 10, secretsDir, List.of(), Clock.fixed(NOW, ZoneOffset.UTC));
    return new ConfigReconciler(
        reloadConfig,
        definitionsDir,
        asyncCallbacksConfigured,
        seed,
        () -> registrar,
        d -> definitionRegistrar.accept(d));
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
      boolean applied = pass().run();

      // Assert
      assertThat(applied).isTrue();
      assertThat(swaps).hasSize(1);
      assertThat(swaps.get(0)).containsOnlyKeys("account");
      assertThat(registered).hasSize(1);
      assertThat(registered.get(0).getName()).isEqualTo("order-saga");
    }

    @Test
    void run_seededServicesUnchanged_swapsNothing() throws IOException {
      // Arrange — the boot caller seeds the applied services with what the orchestrator was built
      // from; a pass over the same files must verify, not re-apply.
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      Map<String, SagaServerConfig.ServiceConfig> seed =
          Map.of(
              "account",
              new SagaServerConfig.ServiceConfig("http://account:8080", List.of(), 0L, Map.of()));

      // Act
      boolean applied = pass(seed).run();

      // Assert — definitions registered, but no endpoint swap
      assertThat(applied).isTrue();
      assertThat(swaps).isEmpty();
      assertThat(registered).hasSize(1);
    }

    @Test
    void run_secondPassUnchanged_doesNothing() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      swaps.clear();
      registered.clear();

      // Act
      boolean applied = pass.run();

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
      ConfigReconciler pass = pass();
      pass.run();
      writeService("account", "base_url=http://account:8080\nheader.Authorization=Bearer new\n");

      // Act
      pass.run();

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
      ConfigReconciler pass = pass();
      pass.run();
      Files.writeString(secretsDir.resolve("token"), "Bearer new");

      // Act
      pass.run();

      // Assert
      assertThat(swaps).hasSize(2);
      assertThat(requireNonNull(swaps.get(1).get("account")).defaultHeaders())
          .containsEntry("Authorization", "Bearer new");
    }

    @Test
    void run_yamlDefinitionFile_parsesAndApplies() throws IOException {
      // The reconciler dispatches json/yaml itself now (it parses the bytes it hashed rather than
      // re-opening the file), so both formats need pinning here.
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

      assertThat(pass().run()).isTrue();
      assertThat(registered).hasSize(1);
      assertThat(registered.get(0).getName()).isEqualTo("yaml-saga");
    }

    @Test
    void run_changedDefinitionWithBumpedVersion_registersIt() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      writeDefinition("saga.json", "order-saga", "2.0", "account", "/x2");

      // Act
      boolean applied = pass.run();

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
      boolean applied = pass().run();

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
      ConfigReconciler pass = pass();

      // Act & Assert — one WARN naming both files
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        pass.run();

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
      ConfigReconciler pass = pass();

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = pass.run();

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
      ConfigReconciler pass = pass();
      pass.run();
      writeDefinition("saga.json", "order-saga", "1.0", "account", "/changed");

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = pass.run();

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
      ConfigReconciler pass = pass();
      assertThat(pass.run()).isFalse();
      writeDefinition("saga.json", "order-saga", "1.0", "account"); // fixed

      // Act & Assert — self-heals without any state reset
      assertThat(pass.run()).isTrue();
      assertThat(registered).hasSize(1);
    }

    @Test
    void run_emptyServicesAfterNonEmpty_rejectsAsSuspectedMountFailure() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      Files.delete(servicesDir.resolve("account.properties"));

      // Act & Assert — the definition still references the service, but even without that, the
      // empty transition alone must reject
      assertThat(pass.run()).isFalse();
    }

    @Test
    void run_emptyDefinitionsAfterNonEmpty_rejectsAsSuspectedMountFailure() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      Files.delete(definitionsDir.resolve("saga.json"));

      // Act & Assert
      assertThat(pass.run()).isFalse();
    }

    @Test
    void run_allowedHostsLoosenedToEmpty_rejects() throws IOException {
      // Arrange — a service that restricted egress must not silently loosen to allow-all
      writeService("account", "base_url=http://account:8080\nallowed_hosts=account\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      writeService("account", "base_url=http://account:8080\n");

      // Act & Assert
      assertThat(pass.run()).isFalse();
    }

    @Test
    void run_duplicateSagaNameAcrossFiles_rejects() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "order-saga", "1.0", "account");
      writeDefinition("b.json", "order-saga", "2.0", "account");

      // Act & Assert
      assertThat(pass().run()).isFalse();
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
      assertThat(pass().run()).isFalse();
    }

    @Test
    void run_singleFileDefinitionsPathWithUnknownExtension_rejects() throws IOException {
      // The directory walk filters by extension, so a single configured file is the only way an
      // unknown extension reaches the parser. It must be refused, not guessed at as YAML.
      writeService("account", "base_url=http://account:8080\n");
      Path file = definitionsDir.resolve("defs.txt");
      Files.writeString(file, "{\"name\":\"x\",\"mode\":\"SAGA\",\"steps\":[]}");
      ReloadConfig reloadConfig =
          new ReloadConfig(
              servicesDir, 10, secretsDir, List.of(), Clock.fixed(NOW, ZoneOffset.UTC));
      ConfigReconciler pass =
          new ConfigReconciler(
              reloadConfig,
              file,
              false,
              Map.of(),
              () -> registrar,
              d -> definitionRegistrar.accept(d));

      // Act & Assert
      assertThat(pass.run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_singleFileDefinitionsPathThatIsASymlink_isRead() throws IOException {
      // definitions_path may name a single file, and a ConfigMap that mounts one key publishes it
      // as a symlink (kubelet's ..data indirection). Reading it must follow that link.
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
      ConfigReconciler pass =
          new ConfigReconciler(
              reloadConfig,
              link,
              false,
              Map.of(),
              () -> registrar,
              d -> definitionRegistrar.accept(d));

      // Act & Assert
      assertThat(pass.run()).isTrue();
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
      assertThat(pass().run()).isTrue();
      assertThat(registered).hasSize(1);
    }

    @Test
    void run_baseUrlWithUserInfo_rejectedAtValidationWithoutEchoingTheUrl() throws IOException {
      // Arrange — the user-info SSRF shape (http://svc@evil resolves to evil). Boot rejects this
      // through the orchestrator builder; the reload path must reject it at validation too, and
      // the message must not echo the URL (a base_url may resolve from a secret reference).
      writeService("account", "base_url=http://payment@evil.example\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = pass.run();

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
      ConfigReconciler pass =
          new ConfigReconciler(
              withCeiling,
              definitionsDir,
              false,
              Map.of(),
              () -> registrar,
              d -> definitionRegistrar.accept(d));

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(pass.run()).isFalse();

        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("SUPER-SECRET-VALUE"));
      }
      assertThat(requireNonNull(pass.status().rejection()).reason())
          .doesNotContain("SUPER-SECRET-VALUE");
    }

    @Test
    void run_secretValuedAllowedHostsWithNoCeiling_rejectedAtValidationWithoutEchoingTheValue()
        throws IOException {
      // The path that needs no ceiling at all: a secret containing a colon reads as a port
      // suffix. Validating the shape here is what stops it reaching the engine, whose own
      // rejection names the host and which this module cannot redact.
      Files.writeString(secretsDir.resolve("token"), "user:PASSWORD-hunter2");
      writeService(
          "account",
          "base_url=http://account:8080\nallowed_hosts=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");

      // Act & Assert — no ceiling configured
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(pass().run()).isFalse();

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
      assertThat(pass().run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_asyncStepWithCallbackConfig_applies() throws IOException {
      // Arrange — the same definition on a daemon WITH async completion configured
      writeService("account", "base_url=http://account:8080\n");
      writeAsyncDefinition();

      // Act & Assert
      assertThat(pass(Map.of(), true).run()).isTrue();
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
      writeService("account", "base_url=http://account:8080\n");
      Files.writeString(
          definitionsDir.resolve("big.json"),
          "{\"_pad\":\"" + "x".repeat((int) ConfigReconciler.MAX_DEFINITION_FILE_BYTES) + "\"}");

      assertThat(pass().run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void run_definitionFileWithInvalidUtf8_rejects() throws IOException {
      // The bytes are decoded strictly once; an undecodable file is a rejection, not a mangled
      // definition.
      writeService("account", "base_url=http://account:8080\n");
      Files.write(definitionsDir.resolve("bad.json"), new byte[] {(byte) 0xC3, (byte) 0x28});

      assertThat(pass().run()).isFalse();
      assertThat(registered).isEmpty();
    }

    @Test
    void runOrThrow_validationError_throwsWithAggregatedMessage() throws IOException {
      // Arrange
      writeService("account", "base_url=   \n");

      // Act & Assert — the boot entry rethrows instead of returning false
      assertThatThrownBy(() -> pass().runOrThrow())
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
      ConfigReconciler pass = pass();

      // Act — first pass fails mid-apply, second retries
      boolean first = pass.run();
      swaps.clear();
      boolean second = pass.run();

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
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      definitionRegistrar =
          definition -> {
            throw new IllegalStateException("store hiccup");
          };
      ConfigReconciler pass = pass();
      String beforeServicesSha = pass.status().appliedServicesSha256();

      // Act
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(pass.run()).isFalse();

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
      ReloadStatus status = pass.status();
      assertThat(status.appliedServicesSha256()).isNotEqualTo(beforeServicesSha);
      assertThat(requireNonNull(status.rejection()).reason()).contains("saga-a");
    }

    @Test
    void run_partialDefinitionApply_auditsTheRegistrationsThatCommitted() throws IOException {
      // The first definition registers, the second fails: the committed one is live fleet-wide,
      // so it belongs in the audit trail even though the pass is rejected.
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
      ConfigReconciler pass = pass();

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(pass.run()).isFalse();

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
      assertThat(pass.status().appliedDefinitionsSha256()).isEqualTo("(not yet applied)");
    }

    @Test
    void run_oneDefinitionFailsPermanently_othersStillRegister() throws IOException {
      // A permanent conflict on one definition must not hold every other definition hostage. The
      // file names decide iteration order, so "a-stuck" is attempted before "z-new": if the loop
      // abandons the pass on the first failure, the unrelated new saga never registers — not
      // "retried next pass" but skipped on every pass, for as long as the conflict persists.
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
      ConfigReconciler pass = pass();

      // Act — several passes, as the scheduler would run
      pass.run();
      pass.run();

      // Assert — the unrelated saga is live despite the stuck one
      assertThat(registered).extracting(SagaDefinition::getName).containsExactly("saga-new");
    }

    @Test
    void run_registrationFailsWhileAnotherDefinitionVanished_warnsOnlyOnce() throws IOException {
      // The vanished-name cleanup used to sit after the registration loop, so an unrelated
      // failure skipped it and the "deleting retires nothing" warning re-fired every pass.
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
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
        pass.run();
        long afterFirst =
            logs.events().stream()
                .filter(e -> e.getFormattedMessage().contains("saga-a"))
                .filter(e -> e.getFormattedMessage().contains("retires nothing"))
                .count();
        pass.run();
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
      ConfigReconciler pass = pass();

      // Act & Assert — the status and the WARN distinguish the permanent case from a generic
      // rejection
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        pass.run();

        assertThat(logs.events())
            .anySatisfy(
                event ->
                    assertThat(event.getFormattedMessage())
                        .contains("permanent")
                        .contains("bump the version"));
      }
      ReloadStatus.Rejection rejection = requireNonNull(pass.status().rejection());
      assertThat(rejection.reason()).contains("permanent");
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
      ConfigReconciler pass = pass();

      // Act & Assert — one WARN for the state change, DEBUG for the repeats
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        pass.run();
        pass.run();
        pass.run();

        assertThat(logs.events().stream().filter(e -> e.getLevel() == Level.WARN)).hasSize(1);
      }
    }

    @Test
    void run_recoveryAfterFailure_logsInfoAndClearsRejection() throws IOException {
      // Arrange
      writeService("account", "base_url=   \n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      writeService("account", "base_url=http://account:8080\n");

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        pass.run();

        assertThat(logs.events())
            .anySatisfy(
                event -> {
                  assertThat(event.getLevel()).isEqualTo(Level.INFO);
                  assertThat(event.getFormattedMessage()).contains("recovered");
                });
      }
      assertThat(pass.status().rejection()).isNull();
    }

    @Test
    void run_appliedStatus_hashesStableAcrossIdenticalPasses() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();

      // Act
      pass.run();
      ReloadStatus first = pass.status();
      pass.run();
      ReloadStatus second = pass.status();

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
      writeService("account", "base_url=http://account:8080\n");
      writeService("ledger", "base_url=http://ledger:9000\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "ledger");
      ConfigReconciler pass = pass();
      pass.run();
      Files.delete(definitionsDir.resolve("a.json"));
      pass.run(); // the vanish warning fires here
      Files.delete(servicesDir.resolve("account.properties"));

      // Act — removing the service the vanished saga still needs
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = pass.run();

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
      writeService("account", "base_url=http://account:8080\n");
      writeService("ledger", "base_url=http://ledger:9000\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "ledger");
      ConfigReconciler pass = pass();
      pass.run();
      Files.delete(definitionsDir.resolve("a.json"));
      pass.run();
      Files.delete(servicesDir.resolve("account.properties"));
      pass.run(); // the removal pass — warns

      // Act — every later pass over the same, now-settled state
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        pass.run();
        pass.run();

        // Assert
        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("still references it"));
      }
    }

    @Test
    void run_serviceRemovedWithNoVanishedReference_doesNotWarn() throws IOException {
      // A service removed while nothing registered needs it is an ordinary change.
      writeService("account", "base_url=http://account:8080\n");
      writeService("ledger", "base_url=http://ledger:9000\n");
      writeDefinition("b.json", "saga-b", "1.0", "ledger");
      ConfigReconciler pass = pass();
      pass.run();
      Files.delete(servicesDir.resolve("account.properties"));

      // Act & Assert
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        assertThat(pass.run()).isTrue();

        assertThat(logs.events())
            .noneSatisfy(
                event -> assertThat(event.getFormattedMessage()).contains("still references it"));
      }
    }

    @Test
    void run_noOpPass_keepsThePreviousAppliedTimestamp() throws IOException {
      // appliedAt answers "when did this replica last apply a change", so routine verification
      // must not masquerade as an apply.
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("saga.json", "order-saga", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      Instant appliedAt = pass.status().appliedAt();

      // Act — a pass over unchanged files
      pass.run();

      // Assert
      assertThat(pass.status().appliedAt()).isEqualTo(appliedAt);
    }

    @Test
    void run_vanishedDefinitionFile_warnsThatDeletionRetiresNothing() throws IOException {
      // Arrange — two definitions so the empty-transition guard does not fire; one file vanishes
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("a.json", "saga-a", "1.0", "account");
      writeDefinition("b.json", "saga-b", "1.0", "account");
      ConfigReconciler pass = pass();
      pass.run();
      Files.delete(definitionsDir.resolve("b.json"));

      // Act & Assert — a warning, not a rejection: the saga remains registered and startable
      try (LogCapture logs = LogCapture.of(ConfigReconciler.class)) {
        boolean applied = pass.run();

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
