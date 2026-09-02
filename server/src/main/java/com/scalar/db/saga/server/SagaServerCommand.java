package com.scalar.db.saga.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
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

  /**
   * The exit code for "I checked it and it is not acceptable", kept distinct from picocli's {@code
   * SOFTWARE} (70), which this command already returns when it fails unexpectedly. A validator that
   * reported a rejected configuration the same way it reports its own crash would be unusable in a
   * CI gate, where those two need different handling. 1 is what a linter returns for findings.
   */
  private static final int INVALID_CONFIGURATION = 1;

  @CommandLine.Option(
      names = "--validate-config",
      description =
          "Check the configuration and exit without starting the server. Reads no store, opens no"
              + " connection, and calls no service. Exit 0 when the configuration is acceptable, 1"
              + " when it is not.")
  private boolean validateConfig;

  /**
   * picocli's own output stream, so the validation report goes where the command's other output
   * goes and a test can capture it without reassigning the JVM's {@code System.out}.
   */
  @CommandLine.Spec private CommandLine.Model.@Nullable CommandSpec spec;

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

    if (validateConfig) {
      // Unreachable null, like configFile above: picocli injects the spec before it calls this.
      return validateConfiguration(
          path, properties, Objects.requireNonNull(spec).commandLine().getOut());
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
   * The checks an offline run cannot perform, printed with every report so a clean result is not
   * read as a promise that the daemon will start.
   *
   * <p>The first three are all the same absence — there is no store here, so nothing can be
   * compared against what is already registered — but they are listed separately because they are
   * three different mistakes an operator makes, and a reader looking for their own case should find
   * it worded the way they would word it.
   */
  private static final List<String> UNCHECKABLE_OFFLINE =
      List.of(
          "whether a definition changed without bumping its version (compared against what a"
              + " running replica has applied)",
          "whether a version is already registered, and which version is actually serving",
          "whether a version's stored content conflicts with the file",
          "whether a service's base_url is reachable, and whether a callback URL and secret are"
              + " the ones the participant expects (no request is made)",
          "whether the store is reachable and its schema is present",
          "the TLS certificate and key pair, which is validated at startup");

  /**
   * Validates the configuration without starting anything, and prints a report.
   *
   * <p>Secret resolution is lenient here, and only here. The tool is for the moment before a
   * rollout — a laptop, a CI job — where the mounted secrets are usually absent, and a run that
   * failed on the first unreadable {@code ${file:...}} would be useless in exactly the places it is
   * for. Leniency is invisible when the secrets are present: it softens a failure, it does not skip
   * the attempt. Every check skipped because a value could not be read is named in the report, so a
   * clean run never means less than it says.
   *
   * @return 0 when the configuration is acceptable, 1 when it is not
   */
  private static int validateConfiguration(Path path, Properties properties, PrintWriter out) {
    List<String> warnings = new ArrayList<>();
    SagaServerConfig config;
    try {
      config = SagaServerConfig.load(properties, SagaServerConfig.SecretMode.LENIENT, warnings);
    } catch (RuntimeException e) {
      // load() stops at the first key it cannot accept, so this is one problem rather than the
      // list; the report says as much instead of implying the file has exactly one.
      return report(
          out,
          path,
          List.of(describeChain(e)),
          warnings,
          0,
          0,
          "The server settings are read in order and reading stopped here, so there may be more"
              + " problems after this one.");
    }
    ConfigReconciler reconciler =
        new ConfigReconciler(
            config.reloadConfig(),
            config.definitionsPath().orElse(null),
            config.callbackBaseUrl().isPresent() && config.callbackSecret().isPresent(),
            new LenientServiceValueResolver(config.reloadConfig().secretsRoot()),
            // Validation stops before a pass applies anything, so neither of these is reached.
            // They throw rather than doing nothing so that a change which applies during
            // validation fails loudly here instead of acting on a daemon that is not running.
            services -> {
              throw new AssertionError("A configuration validation must not swap endpoints");
            },
            new NoDefinitionStore(),
            served -> {
              throw new AssertionError("A configuration validation must not publish served sagas");
            });
    ConfigReconciler.ValidationReport result = reconciler.validate();
    List<String> problems = new ArrayList<>(result.problems());
    // The daemon refuses to start with nothing registered, and that guard lives in SagaServer
    // rather than in the pass — so a validator that did not mirror it would pass a configuration
    // that cannot boot, which is the one thing this command exists to prevent.
    if (problems.isEmpty() && result.definitionCount() == 0) {
      problems.add(SagaServer.noDefinitionsMessage());
    }
    warnings.addAll(result.warnings());
    return report(
        out, path, problems, warnings, result.serviceCount(), result.definitionCount(), null);
  }

  /**
   * Prints the report and returns the exit code. Problems first, because that is what the reader
   * came for; the enumeration of what could not be checked comes last, where it qualifies the
   * verdict above it.
   */
  private static int report(
      PrintWriter out,
      Path path,
      List<String> problems,
      List<String> warnings,
      int serviceCount,
      int definitionCount,
      @Nullable String truncationNote) {
    out.println("Validating " + path);
    out.println();
    if (problems.isEmpty()) {
      out.println(
          "Checked "
              + serviceCount
              + " service file(s) and "
              + definitionCount
              + " saga definition(s). No problems found.");
    } else {
      out.println(problems.size() + " problem(s) found:");
      // One line per problem, whatever the message carries. These are composed from mounted files:
      // a parse error puts its source location on a second line, and a ${file:...} reference may
      // itself contain a newline, either of which would break the list or forge output lines.
      problems.forEach(problem -> out.println("  - " + Redaction.oneLine(problem)));
      if (truncationNote != null) {
        out.println();
        out.println(truncationNote);
      }
    }
    if (!warnings.isEmpty()) {
      out.println();
      out.println(warnings.size() + " warning(s):");
      warnings.forEach(warning -> out.println("  - " + Redaction.oneLine(warning)));
    }
    out.println();
    out.println("Not checked without a running daemon:");
    UNCHECKABLE_OFFLINE.forEach(check -> out.println("  - " + check));
    out.println();
    out.println(problems.isEmpty() ? "Configuration is acceptable." : "Configuration is rejected.");
    out.flush();
    return problems.isEmpty() ? CommandLine.ExitCode.OK : INVALID_CONFIGURATION;
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
    logger.error("Failed to start the server: {}", describeChain(e));
    logger.debug("Startup failure detail", e);
    return CommandLine.ExitCode.SOFTWARE;
  }

  /**
   * Renders a failure and every cause that adds something the text does not already carry, as one
   * line. Package-private so the cases the command line cannot reach — a cause with no message, a
   * multi-line one, a chain that loops — are testable directly.
   *
   * <p>A wrapper names the operation it was attempting and leaves the reason to its cause, so the
   * outermost message alone can misdirect. A malformed definition file reports only {@code Failed
   * to read definition file: /conf/definitions/order.json}, sending an operator to check mounts and
   * permissions when the file was read without error and its JSON is bad. Walking the chain keeps
   * the reason at the level the operator sees, without the frames this handler exists to suppress.
   *
   * <p>A cause whose message the line already carries is skipped, so a wrapper that only prefixes
   * its cause does not print the reason twice. A cause that restates the outer message in other
   * words is still appended: a redundant clause costs the operator a few words, whereas dropping
   * the one clause carrying the diagnosis costs a restart at {@code DEBUG}.
   */
  static String describeChain(Throwable failure) {
    String message = failure.getMessage();
    // toString() keeps an exception that carries no message (an NPE, say) from logging an empty
    // line; getMessage() is the operator-facing text everywhere it is set.
    StringBuilder detail =
        new StringBuilder(
            message == null || message.isBlank() ? failure.toString() : Redaction.oneLine(message));
    // Compared by identity so a cause chain that loops back terminates rather than hanging the
    // handler that was supposed to end the process.
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    seen.add(failure);
    for (Throwable cause = failure.getCause();
        cause != null && seen.add(cause);
        cause = cause.getCause()) {
      String causeMessage = cause.getMessage();
      if (causeMessage == null || causeMessage.isBlank()) {
        continue;
      }
      String text = Redaction.oneLine(causeMessage);
      if (detail.indexOf(text) >= 0) {
        continue;
      }
      // The type is named because a cause often reads as a fragment on its own; "NumberFormat
      // Exception" or "JsonParseException" is what makes the appended clause self-explanatory.
      detail.append(" (").append(cause.getClass().getSimpleName()).append(": ").append(text);
      detail.append(')');
    }
    return detail.toString();
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
