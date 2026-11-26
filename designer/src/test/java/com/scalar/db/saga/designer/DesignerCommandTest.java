package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class DesignerCommandTest {

  @TempDir Path dir;
  private final StringWriter out = new StringWriter();
  private final StringWriter err = new StringWriter();

  private int run(String... args) {
    CommandLine command = new CommandLine(new DesignerCommand());
    command.setOut(new PrintWriter(out, true));
    command.setErr(new PrintWriter(err, true));
    return command.execute(args);
  }

  @Test
  void execute_help_describesTheOptionsAndSucceeds() {
    // Act
    int exitCode = run("--help");

    // Assert
    assertThat(exitCode).isZero();
    assertThat(out.toString())
        .contains("--workspace", "--services", "--host", "--allowed-host", "--port", "--open");
  }

  @Test
  void execute_version_namesTheTool() {
    // Act
    int exitCode = run("--version");

    // Assert
    assertThat(exitCode).isZero();
    assertThat(out.toString()).startsWith("scalardb-saga-designer ");
  }

  @Test
  void execute_missingWorkspace_fails() {
    // Act
    int exitCode = run("--workspace", dir.resolve("missing").toString(), "--port", "0");

    // Assert
    assertThat(exitCode).isNotZero();
  }

  @Test
  void execute_servicesThatIsNotAFolder_fails() throws Exception {
    // Arrange
    Path file = Files.writeString(dir.resolve("payment.properties"), "");

    // Act
    int exitCode = run("--workspace", dir.toString(), "--services", file.toString(), "--port", "0");

    // Assert
    assertThat(exitCode).isNotZero();
    assertThat(err.toString()).contains("--services");
  }

  @Test
  void execute_portOutOfRange_failsAsAUsageError() {
    // Act
    int exitCode = run("--workspace", dir.toString(), "--port", "70000");

    // Assert
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
  }

  @Test
  void execute_unknownOption_failsAsAUsageError() {
    // Act
    int exitCode = run("--colour", "blue");

    // Assert
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
  }
}
