package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Argument parsing for {@link SagaServerCommand}. The parse is what stands between an operator's
 * command line and a process that either serves or exits, and it is the half of the command that
 * can be exercised without binding ports — {@link SagaServerCommand#call()} blocks until shutdown
 * by design, so the server-starting half is covered by the integration tests instead.
 */
class SagaServerCommandTest {

  /** Parses {@code args} without running the command, returning the parse result. */
  private static CommandLine.ParseResult parse(String... args) {
    return new CommandLine(new SagaServerCommand()).parseArgs(args);
  }

  /** Runs the command far enough to produce an exit code, capturing what it writes. */
  private static int executeCapturingErr(StringWriter err, String... args) {
    CommandLine commandLine = new CommandLine(new SagaServerCommand());
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
    assertThat(err.toString()).contains("--config");
  }

  @Test
  void execute_unreadableConfigGiven_failsWithNonZeroExitCode() {
    StringWriter err = new StringWriter();

    int exitCode = executeCapturingErr(err, "--config", "/nonexistent/server.properties");

    assertThat(exitCode).isNotZero();
  }

  @Test
  void execute_versionRequested_printsAVersionRatherThanNothing() {
    CommandLine commandLine = new CommandLine(new SagaServerCommand());
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
    CommandLine commandLine = new CommandLine(new SagaServerCommand());
    StringWriter out = new StringWriter();
    commandLine.setOut(new PrintWriter(out, true));

    int exitCode = commandLine.execute("--help");

    assertThat(exitCode).isZero();
    // The start script the image invokes carries this name, so usage output has to match it.
    assertThat(out.toString()).contains("scalardb-saga-server").contains("--config");
  }
}
