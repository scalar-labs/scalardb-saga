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
 * by design, so the server-starting half is covered by the integration tests instead.
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
            });
    // And picocli's default handler, which is what would have printed the trace, never ran.
    assertThat(err.toString()).isEmpty();
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
