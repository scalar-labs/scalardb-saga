package com.scalar.db.saga.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * The command-line entry point that starts a {@link SagaServer} from a properties file.
 *
 * <p>Separate from {@link SagaServer} rather than annotated onto it, unlike ScalarDB Cluster's
 * {@code ClusterNodeServer}, which is both. {@link SagaServer} is constructed with a {@link
 * SagaServerConfig} and closed by its owner, which is what lets a test start one on ephemeral
 * ports; making it the command as well would mean a no-argument constructor and mutable option
 * fields that mean nothing on that path. The command owns argument parsing and the process
 * lifecycle; the server owns serving.
 *
 * <p>The properties file holds ScalarDB connection settings plus the {@code
 * scalar.db.saga.server.*} keys documented on {@link SagaServerConfig}.
 */
@CommandLine.Command(
    name = "scalardb-saga-server",
    description = "Starts ScalarDB Saga Server.",
    mixinStandardHelpOptions = true,
    versionProvider = SagaServerCommand.ManifestVersionProvider.class)
public class SagaServerCommand implements Callable<Integer> {

  private static final Logger logger = LoggerFactory.getLogger(SagaServerCommand.class);

  /**
   * Reports the version {@code --version} prints, read from the jar manifest rather than a constant
   * in the annotation so it cannot drift from the build that produced the jar. Reports {@code
   * unknown} when the class was not loaded from the packaged jar, which is the case in a test and
   * when running from a classes directory.
   */
  static class ManifestVersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
      String version = SagaServerCommand.class.getPackage().getImplementationVersion();
      return new String[] {"scalardb-saga-server " + (version == null ? "unknown" : version)};
    }
  }

  /**
   * {@code --config} is the name ScalarDB Cluster, ScalarDB Server, and Schema Loader all use, so
   * it is the one an operator will reach for first. {@code --properties} is accepted as an alias
   * because the file this points at is a properties file everywhere else in the documentation, and
   * failing on the obvious synonym would be a needless papercut.
   */
  @CommandLine.Option(
      names = {"--config", "--properties"},
      required = true,
      paramLabel = "PROPERTIES_FILE",
      description = "A configuration file in properties format.")
  private @Nullable Path configFile;

  @Override
  public Integer call() throws Exception {
    // Unreachable in practice: picocli rejects a missing required option before it calls this, so a
    // null here would be a parser contract violation, not an operator error. The message names both
    // spellings rather than the parser, since either one satisfies it.
    Path path = Objects.requireNonNull(configFile, "--config or --properties must be set");

    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(path)) {
      properties.load(in);
    } catch (IOException e) {
      // A mistyped path is the likeliest operator error, so this line has to stand on its own.
      // FileSystemException carries the path as its whole message, which the prefix already names —
      // there the exception type is what says what went wrong; elsewhere the message does.
      String reason =
          e.getMessage() == null || e.getMessage().equals(path.toString())
              ? e.getClass().getSimpleName()
              : e.getMessage();
      throw new IllegalArgumentException(
          "Cannot read the configuration file '" + path + "': " + reason, e);
    }

    SagaServer server = new SagaServer(SagaServerConfig.load(properties)).start();
    CountDownLatch shutdown = new CountDownLatch(1);
    // close() drains in-flight work and logs the stopped line before the latch opens, so the main
    // thread cannot reach System.exit and halt the JVM mid-drain.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.close();
                  shutdown.countDown();
                },
                "saga-server-shutdown"));
    shutdown.await();
    return 0;
  }

  /**
   * Starts the server.
   *
   * <p>The JUL-to-SLF4J bridge is installed here rather than inside {@link SagaServer} because
   * installing a handler on the JVM-wide JUL root logger is an application's decision, not a
   * library's; see {@link SagaServer#installJulToSlf4jBridge()}. It runs before parsing so the
   * process cannot fail and exit before its logging is configured.
   *
   * @param args the command-line arguments
   */
  public static void main(String[] args) {
    // Exit with the parsed exit code so a startup failure becomes a non-zero exit an init system or
    // container runtime can act on.
    System.exit(run(args));
  }

  /**
   * Reports a startup failure as one log line instead of a stack trace, and returns the exit code
   * for it.
   *
   * <p>picocli formats a <i>parse</i> error itself, but its default handler rethrows an exception
   * from {@link #call()} and prints the whole trace. Every message this server produces on a bad
   * configuration is written for an operator, and burying those under JDK and picocli frames is
   * what this avoids. The trace is still available at {@code DEBUG}, which the image exposes
   * through {@code SCALAR_DB_SAGA_LOG_LEVEL}.
   */
  private static int reportStartupFailure(
      Exception e, CommandLine commandLine, CommandLine.ParseResult parseResult) {
    // getMessage() is the operator-facing text everywhere it is set; toString() keeps an exception
    // that carries none (an NPE, say) from logging an empty line.
    String detail =
        e.getMessage() == null || e.getMessage().isBlank() ? e.toString() : e.getMessage();
    logger.error("Failed to start the server: {}", detail);
    logger.debug("Startup failure detail", e);
    return CommandLine.ExitCode.SOFTWARE;
  }

  /**
   * Installs logging and then parses and runs, returning the exit code {@link #main} exits with.
   *
   * <p>Split out of {@link #main} so a test can assert the ordering — the bridge is installed
   * before anything can fail, including argument validation — without the {@code System.exit} that
   * makes {@code main} itself uncallable from a test JVM.
   *
   * @param args the command-line arguments
   * @return the exit code to pass to {@link System#exit}
   */
  static int run(String[] args) {
    SagaServer.installJulToSlf4jBridge();
    return newCommandLine().execute(args);
  }

  /**
   * The configured parser, package-private so a test exercises the same configuration the process
   * runs rather than a bare {@code CommandLine} that would silently lack it.
   */
  static CommandLine newCommandLine() {
    return new CommandLine(new SagaServerCommand())
        // picocli expands an @-prefixed argument by reading that file and splicing its lines into
        // the argument list, echoing any it cannot match back in the error message. This command
        // takes one option and has no use for that, while the process it runs can read every
        // mounted secret — so anyone able to append an argument could print one to the log.
        .setExpandAtFiles(false)
        .setExecutionExceptionHandler(SagaServerCommand::reportStartupFailure);
  }
}
