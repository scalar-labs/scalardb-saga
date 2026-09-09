package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class BenchmarkCliTest {

  @Test
  public void convert_lowercaseGiven_returnsStartMode() {
    // Arrange
    BenchmarkCli.StartModeConverter converter = new BenchmarkCli.StartModeConverter();

    // Act & Assert
    assertThat(converter.convert("sync")).isEqualTo(StartMode.SYNC);
  }

  @Test
  public void convert_hyphenatedGiven_returnsStartMode() {
    // Arrange
    BenchmarkCli.StartModeConverter converter = new BenchmarkCli.StartModeConverter();

    // Act & Assert
    assertThat(converter.convert("async-poll")).isEqualTo(StartMode.ASYNC_POLL);
    assertThat(converter.convert("ASYNC_FIRE")).isEqualTo(StartMode.ASYNC_FIRE);
  }

  @Test
  public void convert_unknownValueGiven_throwsException() {
    // Arrange
    BenchmarkCli.StartModeConverter converter = new BenchmarkCli.StartModeConverter();

    // Act & Assert
    assertThatThrownBy(() -> converter.convert("bogus"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void call_noModeGiven_returnsUsageExitCode() {
    // Arrange
    CommandLine commandLine = new CommandLine(new BenchmarkCli());

    // Act
    int exitCode = commandLine.execute();

    // Assert
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
  }

  @Test
  public void call_grpcModeWithoutTarget_returnsUsageExitCode() {
    // Arrange
    CommandLine commandLine =
        new CommandLine(new BenchmarkCli()).setCaseInsensitiveEnumValuesAllowed(true);

    // Act
    int exitCode = commandLine.execute("--mode", "grpc");

    // Assert
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
  }

  @Test
  public void call_printDefinition_returnsOkWithoutMode() {
    // Arrange
    CommandLine commandLine = new CommandLine(new BenchmarkCli());

    // Act
    int exitCode = commandLine.execute("--print-definition", "--steps", "2");

    // Assert
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.OK);
  }

  @Test
  public void call_unknownOptionGiven_returnsUsageExitCode() {
    // Arrange
    CommandLine commandLine = new CommandLine(new BenchmarkCli());

    // Act
    int exitCode = commandLine.execute("--no-such-option");

    // Assert
    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
  }
}
