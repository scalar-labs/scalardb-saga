package com.scalar.db.saga.server;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.jspecify.annotations.Nullable;
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
    // picocli enforces required = true before it ever calls this, so a null here would be a picocli
    // contract violation rather than an operator error.
    Path path = Objects.requireNonNull(configFile, "configFile must have been set by picocli");

    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(path)) {
      properties.load(in);
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
   * library's; see {@link SagaServer#installJulToSlf4jBridge()}. It runs before parsing so that a
   * usage error is reported through the same logging pipeline as everything else.
   *
   * @param args the command-line arguments
   */
  public static void main(String[] args) {
    // Exit with picocli's code so a usage error (a missing or unreadable --config) is a non-zero
    // exit an init system or container runtime can act on, rather than a stack trace and a 1.
    System.exit(run(args));
  }

  /**
   * Installs logging and then parses and runs, returning the exit code {@link #main} exits with.
   *
   * <p>Split out of {@link #main} so a test can assert the ordering — the bridge is installed
   * before anything can fail, including argument validation — without the {@code System.exit} that
   * makes {@code main} itself uncallable from a test JVM.
   *
   * @param args the command-line arguments
   * @return the picocli exit code
   */
  static int run(String[] args) {
    SagaServer.installJulToSlf4jBridge();
    return new CommandLine(new SagaServerCommand()).execute(args);
  }
}
