package com.scalar.db.saga.daemon;

import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinitionParser;
import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.daemon.api.ErrorMapper;
import com.scalar.db.saga.daemon.api.HealthResource;
import com.scalar.db.saga.daemon.api.SagaResource;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone HTTP server that hosts a saga engine and exposes it over a REST API (daemon mode).
 *
 * <p>Construction builds the embedded {@link SagaManager} — creating the saga schema if needed —
 * from the configured properties, then loads and registers any declarative saga definitions found
 * at the configured definitions path. {@link #start()} starts background recovery/retention tasks
 * and binds the HTTP port. {@link #close()} stops accepting requests and then drains in-flight
 * sagas via {@link SagaManager#close()}.
 *
 * <p>Daemon mode is <b>declarative-only</b>: the server ships as a container, so operators cannot
 * supply code-step classes. A definition that declares a code step ({@code stepClass}) is rejected
 * at startup — use a declarative service step, or run the engine in embedded mode for code steps.
 */
public final class SagaServer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SagaServer.class);

  private final SagaServerConfig config;
  private final SagaManager sagaManager;
  private final Javalin app;
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Builds the server, its underlying saga engine (connecting to ScalarDB), and registers
   * configured definitions and routes. Does not bind the HTTP port — call {@link #start()}.
   *
   * @param config the server configuration
   */
  public SagaServer(SagaServerConfig config) {
    this(config, buildSagaManager(config));
  }

  /**
   * Visible for testing: builds the server around an already-constructed {@link SagaManager}, so a
   * test can inject a mock to exercise definition loading and route wiring without a database.
   */
  SagaServer(SagaServerConfig config, SagaManager sagaManager) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.sagaManager = Objects.requireNonNull(sagaManager, "sagaManager must not be null");
    this.app = Javalin.create();
    try {
      loadDefinitions();
      registerRoutes();
    } catch (RuntimeException e) {
      // Release the store/DB connections held by the manager if startup wiring fails.
      sagaManager.close();
      throw e;
    }
  }

  private static SagaManager buildSagaManager(SagaServerConfig config) {
    Objects.requireNonNull(config, "config must not be null");
    SagaManager.Builder builder =
        SagaManager.newBuilder().storeFactory(ScalarDbSagaStoreFactory.create(config.properties()));
    config.serviceBaseUrls().forEach((name, baseUrl) -> builder.httpEndpoint(name, baseUrl).add());
    return builder.build();
  }

  private void loadDefinitions() {
    int count = config.definitionsPath().map(this::registerDefinitions).orElse(0);
    // A daemon with no registered definitions cannot run any saga, and definitions are currently
    // loaded only here at startup — so fail fast rather than serve a healthy but useless process.
    // If dynamic definition registration (e.g. an admin endpoint) is added later, relax this to
    // allow an empty startup when that mechanism is enabled.
    if (count == 0) {
      throw new IllegalStateException(
          "No saga definitions registered. Set '"
              + SagaServerConfig.DEFINITIONS_PATH_KEY
              + "' to a file or directory containing at least one saga definition.");
    }
    logger.info("Registered {} saga definition(s)", count);
  }

  private int registerDefinitions(Path path) {
    try {
      if (Files.isDirectory(path)) {
        try (Stream<Path> files = Files.list(path)) {
          List<Path> definitions =
              files
                  .filter(Files::isRegularFile)
                  .filter(SagaServer::isDefinitionFile)
                  .sorted()
                  .toList();
          definitions.forEach(this::registerDefinition);
          return definitions.size();
        }
      }
      registerDefinition(path);
      return 1;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load saga definitions from " + path, e);
    }
  }

  /**
   * Parses one definition file and registers it, rejecting code steps — daemon mode is
   * declarative-only (see the class comment).
   */
  private void registerDefinition(Path path) {
    SagaDefinition definition = SagaDefinitionParser.parseFile(path);
    for (SagaDefinition.StepDefinition step : definition.getSteps()) {
      if (step instanceof SagaDefinition.ClassStep) {
        throw new SagaDefinitionException(
            "Saga '"
                + definition.getName()
                + "' step '"
                + step.getName()
                + "' is a code step (stepClass), which daemon mode does not support. Use a"
                + " declarative service step, or run the engine in embedded mode for code steps.");
      }
    }
    sagaManager.register(definition);
  }

  private static boolean isDefinitionFile(Path path) {
    String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
    return name.endsWith(".json") || name.endsWith(".yaml") || name.endsWith(".yml");
  }

  private void registerRoutes() {
    HealthResource.register(app);
    ErrorMapper.register(app);
    SagaResource.register(app, sagaManager, config.syncTimeoutMillis());
  }

  /**
   * Starts background recovery/retention tasks, binds the HTTP port, and begins serving.
   *
   * @return this server
   */
  public SagaServer start() {
    try {
      sagaManager.startBackgroundTasks();
      app.start(config.port());
    } catch (RuntimeException e) {
      // Stop the (partially started) app and drain/close the manager so a failed start — e.g. a
      // port bind failure after background tasks are running — does not leak threads/connections.
      close();
      throw e;
    }
    logger.info("SagaServer started on port {}", port());
    return this;
  }

  /**
   * Returns the bound HTTP port (the actual ephemeral port when configured with {@code 0}). Only
   * meaningful after {@link #start()}.
   *
   * @return the bound port
   */
  public int port() {
    return app.port();
  }

  @Override
  public void close() {
    // Idempotent: start() calls close() on a bind failure, and try-with-resources will call it
    // again, so guard against draining the manager (and closing the store) twice.
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Stop accepting new requests first, then drain in-flight sagas.
    app.stop();
    sagaManager.close();
    logger.info("SagaServer stopped");
  }

  /**
   * Command-line entry point: {@code SagaServer <server.properties>}. The properties file holds
   * ScalarDB connection settings plus optional {@code scalar.db.saga.server.*} keys.
   *
   * @param args {@code args[0]} is the path to the server properties file
   * @throws IOException if the properties file cannot be read
   * @throws InterruptedException if interrupted while awaiting shutdown
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 1) {
      throw new IllegalArgumentException("usage: SagaServer <server.properties>");
    }
    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(Path.of(args[0]))) {
      properties.load(in);
    }
    SagaServer server = new SagaServer(SagaServerConfig.load(properties)).start();
    CountDownLatch shutdown = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.close();
                  shutdown.countDown();
                },
                "saga-server-shutdown"));
    shutdown.await();
  }
}
