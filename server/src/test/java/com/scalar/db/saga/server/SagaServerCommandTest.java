package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Argument parsing for {@link SagaServerCommand}. The parse is what stands between an operator's
 * command line and a process that either serves or exits, and it is the half of the command that
 * can be exercised without binding ports — {@link SagaServerCommand#call()} blocks until shutdown
 * by design, so the server-starting half is exercised only by the {@code image-smoke-test} CI job,
 * which boots the image with {@code --config}. The integration tests do not reach it: they
 * construct {@link SagaServer} directly, so a change to {@code call()} that they all survive can
 * still break the entry point the container runs.
 */
class SagaServerCommandTest {

  private ch.qos.logback.classic.Logger commandLogger;
  private ListAppender<ILoggingEvent> appender;

  @BeforeEach
  void attachAppender() {
    commandLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SagaServerCommand.class);
    appender = new ListAppender<>();
    appender.start();
    commandLogger.addAppender(appender);
  }

  @AfterEach
  void detachAppender() {
    commandLogger.detachAppender(appender);
    appender.stop();
  }

  /** The ERROR records the command emitted, which is where a startup failure is reported. */
  private List<ILoggingEvent> errorEvents() {
    return appender.list.stream().filter(event -> event.getLevel() == Level.ERROR).toList();
  }

  /** Parses {@code args} without running the command, returning the parse result. */
  private static CommandLine.ParseResult parse(String... args) {
    return SagaServerCommand.newCommandLine().parseArgs(args);
  }

  /**
   * Runs the command far enough to produce an exit code, capturing what it writes. Goes through
   * {@link SagaServerCommand#newCommandLine()} so these assertions cover the parser the process
   * actually runs, including the at-file and exception-handler configuration.
   */
  private static int executeCapturingErr(StringWriter err, String... args) {
    CommandLine commandLine = SagaServerCommand.newCommandLine();
    commandLine.setErr(new PrintWriter(err, true));
    return commandLine.execute(args);
  }

  @Test
  void parseArgs_configGiven_bindsThePath() {
    CommandLine.ParseResult result = parse("--config", "/etc/saga/server.properties");

    Path value = result.matchedOptionValue("--config", (Path) null);
    assertThat(value).isEqualTo(Path.of("/etc/saga/server.properties"));
  }

  @Test
  void parseArgs_propertiesAliasGiven_bindsTheSamePath() {
    // The alias must reach the same option, not a second one that call() would then ignore.
    CommandLine.ParseResult result = parse("--properties", "/etc/saga/server.properties");

    Path value = result.matchedOptionValue("--config", (Path) null);
    assertThat(value).isEqualTo(Path.of("/etc/saga/server.properties"));
  }

  @Test
  void execute_noConfigGiven_failsWithUsageExitCode() {
    StringWriter err = new StringWriter();

    // A missing --config must be a usage error an init system can act on, not a stack trace.
    int exitCode = executeCapturingErr(err);

    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
    // The first line must be the diagnosis, with the usage block after it. Asserting only that
    // "--config" appears somewhere would pass off the usage block alone and pin nothing at all.
    assertThat(err.toString().lines().findFirst().orElse(""))
        .startsWith("Missing required option:");
  }

  @Test
  void execute_unreadableConfigGiven_reportsWithoutAStackTrace() {
    // Arrange — the diagnostic goes through SLF4J, so watching picocli's error writer alone would
    // observe an empty string and pass no matter what the handler did.
    StringWriter err = new StringWriter();

    // Act — the likeliest operator error.
    int exitCode = executeCapturingErr(err, "--config", "/nonexistent/server.properties");

    // Assert — one ERROR naming the file, carrying no throwable. Logback prints frames only for an
    // event that carries one, so a null throwable is precisely "no stack trace". Asserting the
    // formatted text instead would pass on a pattern that happens not to include %ex.
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.SOFTWARE);
    assertThat(errorEvents())
        .singleElement()
        .satisfies(
            event -> {
              assertThat(event.getFormattedMessage())
                  .contains("Cannot read the configuration file")
                  .contains("/nonexistent/server.properties");
              assertThat(event.getThrowableProxy()).isNull();
              // The cause here is a NoSuchFileException whose message is the path the line already
              // names, so walking the chain must add nothing — the path appears once.
              assertThat(event.getFormattedMessage().split("/nonexistent/server.properties", -1))
                  .hasSize(2);
            });
    // And picocli's default handler, which is what would have printed the trace, never ran.
    assertThat(err.toString()).isEmpty();
  }

  @Test
  void execute_configWithUnparsablePortGiven_reportsRedactedReasonWithoutTheValue(@TempDir Path dir)
      throws Exception {
    // Arrange — a value that fails in SagaServerConfig.load, before anything touches a database.
    Path config = dir.resolve("server.properties");
    Files.writeString(config, "scalar.db.saga.server.http.port=notanumber\n");
    StringWriter err = new StringWriter();

    // Act
    int exitCode = executeCapturingErr(err, "--config", config.toString());

    // Assert — the line names the key and what was wrong with the value at the default level, but
    // never the value itself: it may be a resolved secret reference, so the parse error carries
    // neither the value nor a NumberFormatException cause, whose message would embed it via
    // describeChain.
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.SOFTWARE);
    assertThat(errorEvents())
        .singleElement()
        .satisfies(
            event -> {
              assertThat(event.getFormattedMessage())
                  .contains("scalar.db.saga.server.http.port")
                  .contains("not a number")
                  .doesNotContain("notanumber");
              assertThat(event.getThrowableProxy()).isNull();
            });
  }

  @Test
  void describeChain_wrappedCauseGiven_appendsTheReason() {
    // The shape that costs the diagnosis: the wrapper names the operation, the cause holds the
    // reason, and the operation succeeded as far as the wrapper's own wording goes.
    Exception failure =
        new IllegalStateException(
            "Failed to read definition file: /conf/definitions/order.json",
            new IllegalArgumentException("Unexpected character ('t' (code 116))"));

    assertThat(SagaServerCommand.describeChain(failure))
        .isEqualTo(
            "Failed to read definition file: /conf/definitions/order.json"
                + " (IllegalArgumentException: Unexpected character ('t' (code 116)))");
  }

  @Test
  void describeChain_causeAlreadyQuotedGiven_doesNotRepeatIt() {
    // The common wrapper appends its cause's message verbatim; re-appending would print it twice.
    Exception cause = new IllegalArgumentException("Permission denied");
    Exception failure = new IllegalStateException("Cannot read '/etc/x': Permission denied", cause);

    assertThat(SagaServerCommand.describeChain(failure))
        .isEqualTo("Cannot read '/etc/x': Permission denied");
  }

  @Test
  void describeChain_multiLineCauseGiven_collapsesItToOneLine() {
    // Jackson puts the source location on a second line. A report split across two lines loses its
    // second half to any pipeline that keys off the first.
    Exception failure =
        new IllegalStateException(
            "Failed to parse", new IllegalArgumentException("Unexpected character\n at [line: 3]"));

    assertThat(SagaServerCommand.describeChain(failure))
        .isEqualTo("Failed to parse (IllegalArgumentException: Unexpected character at [line: 3])")
        .doesNotContain("\n");
  }

  @Test
  void describeChain_messagelessFailureGiven_fallsBackToTheType() {
    // An NPE carries no message; without the fallback the line would read "Failed to start the
    // server: " and name nothing at all.
    assertThat(SagaServerCommand.describeChain(new NullPointerException()))
        .contains("NullPointerException");
  }

  @Test
  void describeChain_messagelessCauseGiven_skipsIt() {
    Exception failure = new IllegalStateException("Startup failed", new NullPointerException());

    // An empty parenthesised clause would be noise, not a diagnosis.
    assertThat(SagaServerCommand.describeChain(failure)).isEqualTo("Startup failed");
  }

  @Test
  void describeChain_deeplyNestedCauseGiven_reachesTheInnermostReason() {
    Exception failure =
        new IllegalStateException(
            "Failed to start",
            new IllegalStateException(
                "Failed to open the store", new IllegalStateException("EOF")));

    assertThat(SagaServerCommand.describeChain(failure))
        .isEqualTo(
            "Failed to start (IllegalStateException: Failed to open the store)"
                + " (IllegalStateException: EOF)");
  }

  @Test
  void describeChain_cyclicChainGiven_terminates() {
    // Nothing in the JDK forbids a cause chain that loops. Hanging here would leave the process
    // alive with no diagnosis, which is worse than the failure being reported.
    Exception first = new IllegalStateException("first");
    Exception second = new IllegalStateException("second", first);
    first.initCause(second);

    assertThat(SagaServerCommand.describeChain(first))
        .isEqualTo("first (IllegalStateException: second)");
  }

  @Test
  void execute_atFileArgumentGiven_isNotExpanded(@TempDir Path dir) throws Exception {
    Path secret = dir.resolve("secret");
    Files.writeString(secret, "super-secret-token\n");
    StringWriter err = new StringWriter();

    // picocli expands @file by default and echoes lines it cannot match, which would print the
    // file's contents. The argument must be rejected as-is instead.
    int exitCode = executeCapturingErr(err, "--config", "/nonexistent/x.properties", "@" + secret);

    assertThat(exitCode).isNotZero();
    assertThat(err.toString()).doesNotContain("super-secret-token");
  }

  @Test
  void execute_versionRequested_printsAVersionRatherThanNothing() {
    CommandLine commandLine = SagaServerCommand.newCommandLine();
    StringWriter out = new StringWriter();
    commandLine.setOut(new PrintWriter(out, true));

    int exitCode = commandLine.execute("--version");

    // Without a version provider picocli prints an empty line and exits 0, which looks like a flag
    // that silently does nothing. Off the packaged jar there is no manifest version, so the honest
    // answer here is "unknown"; what this pins is that something identifying is always printed.
    assertThat(exitCode).isZero();
    assertThat(out.toString()).contains("scalardb-saga-server");
  }

  @Test
  void execute_helpRequested_succeedsAndNamesTheCommand() {
    CommandLine commandLine = SagaServerCommand.newCommandLine();
    StringWriter out = new StringWriter();
    commandLine.setOut(new PrintWriter(out, true));

    int exitCode = commandLine.execute("--help");

    assertThat(exitCode).isZero();
    // The start script the image invokes carries this name, so usage output has to match it.
    assertThat(out.toString()).contains("scalardb-saga-server").contains("--config");
  }

  /**
   * {@code --validate-config}: the half of the command that runs to completion without binding a
   * port or opening a store, so unlike {@link SagaServerCommand#call()}'s serving path it can be
   * exercised end to end here — through the same parser and the same exception handler the process
   * uses.
   */
  @Nested
  class ValidateConfig {

    @TempDir Path configDir;
    @TempDir Path servicesDir;
    @TempDir Path definitionsDir;
    @TempDir Path secretsDir;

    /** Runs the command capturing what it prints, the way the operator would see it. */
    private int validate(StringWriter out, Path configFile) {
      CommandLine commandLine = SagaServerCommand.newCommandLine();
      commandLine.setOut(new PrintWriter(out, true));
      return commandLine.execute("--validate-config", "--config", configFile.toString());
    }

    /** Writes a server.properties pointing at this test's directories. */
    private Path writeConfig(String... extraLines) throws IOException {
      StringBuilder text = new StringBuilder();
      text.append("scalar.db.saga.server.services_path=").append(servicesDir).append('\n');
      text.append("scalar.db.saga.server.definitions_path=").append(definitionsDir).append('\n');
      text.append("scalar.db.saga.server.secrets_root=").append(secretsDir).append('\n');
      for (String line : extraLines) {
        text.append(line).append('\n');
      }
      return Files.writeString(configDir.resolve("server.properties"), text);
    }

    private void writeService(String name, String content) throws IOException {
      Files.writeString(servicesDir.resolve(name + ".properties"), content);
    }

    private void writeDefinition(String sagaName, String service) throws IOException {
      Files.writeString(
          definitionsDir.resolve(sagaName + ".json"),
          "{\"name\":\""
              + sagaName
              + "\",\"version\":\"1\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\",\"service\":\""
              + service
              + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
              + "\"compensation\":{\"method\":\"POST\",\"path\":\"/undo\"}}]}");
    }

    @Test
    void parseArgs_validateConfigGiven_bindsTheFlag() throws IOException {
      CommandLine.ParseResult result =
          parse("--validate-config", "--config", writeConfig().toString());

      assertThat(result.hasMatchedOption("--validate-config")).isTrue();
    }

    @Test
    void execute_validConfigurationGiven_exitsZero() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("order-saga", "account");
      StringWriter out = new StringWriter();

      // Act
      int exitCode = validate(out, writeConfig());

      // Assert
      assertThat(exitCode).isZero();
      assertThat(out.toString())
          .contains("1 service file(s) and 1 saga definition(s)")
          .contains("Configuration is acceptable.");
    }

    @Test
    void execute_definitionNamingAnAbsentService_exitsOneAndNamesIt() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("order-saga", "billing");
      StringWriter out = new StringWriter();

      // Act
      int exitCode = validate(out, writeConfig());

      // Assert — 1, not picocli's SOFTWARE: a rejected configuration is a finding, not a crash.
      assertThat(exitCode).isEqualTo(1);
      assertThat(out.toString()).contains("billing").contains("Configuration is rejected.");
    }

    @Test
    void execute_noDefinitionsGiven_exitsOneMirroringTheBootGuard() throws IOException {
      // Arrange — a daemon refuses to start with nothing registered, so a validator that passed
      // this would bless a configuration that cannot boot.
      writeService("account", "base_url=http://account:8080\n");
      StringWriter out = new StringWriter();

      // Act
      int exitCode = validate(out, writeConfig());

      // Assert
      assertThat(exitCode).isEqualTo(1);
      assertThat(out.toString()).contains(SagaServer.noDefinitionsMessage());
    }

    @Test
    void execute_anyConfigurationGiven_enumeratesWhatItCouldNotCheck() throws IOException {
      // Arrange
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("order-saga", "account");
      StringWriter out = new StringWriter();

      // Act
      validate(out, writeConfig());

      // Assert — a clean result must not read as a promise that the daemon will start.
      assertThat(out.toString())
          .contains("Not checked without a running daemon:")
          .contains("bumping its version")
          .contains("store is reachable");
    }

    @Test
    void execute_secretsPresent_neverPrintsAResolvedValue() throws IOException {
      // Arrange
      Files.writeString(secretsDir.resolve("token"), "SUPER-SECRET-VALUE");
      writeService(
          "account",
          "base_url=http://account:8080\nheader.X-Api-Key=${file:UTF-8:"
              + secretsDir.resolve("token")
              + "}\n");
      writeDefinition("order-saga", "account");
      StringWriter out = new StringWriter();

      // Act
      int exitCode = validate(out, writeConfig());

      // Assert
      assertThat(exitCode).isZero();
      assertThat(out.toString()).doesNotContain("SUPER-SECRET-VALUE");
    }

    @Test
    void execute_secretNotOnThisMachine_passesAndNamesTheSkippedCheck() throws IOException {
      // Arrange — the laptop and CI case the lenient mode exists for.
      writeService(
          "account",
          "base_url=http://account:8080\nheader.X-Api-Key=${file:UTF-8:"
              + secretsDir.resolve("absent")
              + "}\n");
      writeDefinition("order-saga", "account");
      StringWriter out = new StringWriter();

      // Act
      int exitCode = validate(out, writeConfig());

      // Assert — acceptable, and explicit about the one thing it did not check.
      assertThat(exitCode).isZero();
      assertThat(out.toString())
          .contains("warning(s):")
          .contains("header.X-Api-Key")
          .contains("header-value checks");
    }

    @Test
    void execute_unknownServerSettingGiven_exitsOneAndSaysReadingStopped() throws IOException {
      // Arrange — server settings are read in order and stop at the first refusal, unlike the
      // service and definition files, which aggregate. The report must not imply otherwise.
      writeService("account", "base_url=http://account:8080\n");
      writeDefinition("order-saga", "account");
      StringWriter out = new StringWriter();

      // Act
      int exitCode = validate(out, writeConfig("scalar.db.saga.server.no_such_key=1"));

      // Assert
      assertThat(exitCode).isEqualTo(1);
      assertThat(out.toString())
          .contains("no_such_key")
          .contains("there may be more problems after this one");
    }
  }
}
