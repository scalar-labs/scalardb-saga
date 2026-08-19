package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
}
