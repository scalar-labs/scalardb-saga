package com.scalar.db.saga.designer;

import java.awt.Desktop;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * The command-line entry point of the saga definition designer: serves the editor for the
 * definition files in one folder until stopped.
 */
@CommandLine.Command(
    name = "scalardb-saga-designer",
    description = "Edits saga definitions in a browser.",
    mixinStandardHelpOptions = true,
    versionProvider = DesignerCommand.ManifestVersionProvider.class)
public final class DesignerCommand implements Callable<Integer> {

  private static final Logger logger = LoggerFactory.getLogger(DesignerCommand.class);

  @CommandLine.Option(
      names = "--workspace",
      paramLabel = "DIR",
      description = "The folder of definition files to edit. Default: the current folder.")
  private Path workspace = Path.of(".");

  @CommandLine.Option(
      names = "--services",
      paramLabel = "DIR",
      description =
          "The server's folder of service files, read only: offers existing service names and"
              + " skips templates for services that already have a file.")
  private @Nullable Path services;

  @CommandLine.Option(
      names = "--host",
      paramLabel = "ADDRESS",
      description =
          "The address to listen on. Default: 127.0.0.1. The designer has no authentication: on"
              + " any other address, anyone who can reach the port can change files in the"
              + " workspace.")
  private String host = "127.0.0.1";

  @CommandLine.Option(
      names = "--allowed-host",
      paramLabel = "NAME",
      description =
          "A further host name the browser may use to reach the designer, such as a VM's host"
              + " name; repeatable. Loopback names, the --host address and, on a wildcard --host,"
              + " this machine's addresses are always accepted; requests by any other name are"
              + " refused, so that a web page cannot reach the designer through DNS rebinding.")
  private List<String> allowedHosts = new ArrayList<>();

  @CommandLine.Option(
      names = "--port",
      paramLabel = "PORT",
      description = "The port to listen on; 0 picks a free one. Default: 7070.")
  private int port = 7070;

  @CommandLine.Option(names = "--open", description = "Open the editor in the default browser.")
  private boolean open;

  /** Creates the command; {@link #main} hands it to picocli, which binds the options. */
  public DesignerCommand() {}

  /**
   * Runs the designer until the process is stopped.
   *
   * @param args the command-line arguments
   */
  public static void main(String[] args) {
    int exitCode = new CommandLine(new DesignerCommand()).execute(args);
    // On success the server's threads keep the process alive; only a failure ends it here.
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  @Override
  public Integer call() throws IOException {
    if (port < 0 || port > 65535) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--port must be between 0 and 65535");
    }
    Workspace folder = new Workspace(workspace);
    if (services != null && !Files.isDirectory(services)) {
      throw new IOException("--services is not a folder: " + services);
    }
    folder.removeStaleTemporaryFiles();
    DesignerServer server =
        new DesignerServer(
            new DesignerApi(folder, services),
            new RequestGuard(
                host, allowedHosts, RequestGuard.isWildcard(host) ? localAddresses() : List.of()),
            host,
            port);
    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread(server::close, "designer-shutdown"));

    URI uri = URI.create("http://" + urlHost() + ":" + server.port() + "/");
    logger.info("Saga designer is running at {} (workspace: {})", uri, folder.root());
    if (!isLoopback(host)) {
      logger.warn(
          "Listening on {}: the designer has no authentication, so anyone who can reach this"
              + " port can change the files in {}",
          host,
          folder.root());
    }
    if (open) {
      openBrowser(uri);
    }
    return 0;
  }

  private String urlHost() {
    if (RequestGuard.isWildcard(host)) {
      return "127.0.0.1";
    }
    return host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host;
  }

  private static List<InetAddress> localAddresses() throws IOException {
    List<InetAddress> addresses = new ArrayList<>();
    for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      addresses.addAll(Collections.list(nic.getInetAddresses()));
    }
    return addresses;
  }

  private static boolean isLoopback(String host) {
    try {
      return InetAddress.getByName(host).isLoopbackAddress();
    } catch (UnknownHostException e) {
      return false;
    }
  }

  private static void openBrowser(URI uri) {
    try {
      if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        Desktop.getDesktop().browse(uri);
        return;
      }
    } catch (IOException | UnsupportedOperationException e) {
      logger.debug("Opening the browser failed", e);
    }
    logger.info("Could not open a browser here; open {} yourself", uri);
  }

  /** Reports the version from the jar manifest, or {@code unknown} outside the packaged jar. */
  static final class ManifestVersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
      String version = DesignerCommand.class.getPackage().getImplementationVersion();
      return new String[] {"scalardb-saga-designer " + (version == null ? "unknown" : version)};
    }
  }
}
