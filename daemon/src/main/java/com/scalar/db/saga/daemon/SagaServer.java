package com.scalar.db.saga.daemon;

import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.daemon.api.ErrorMapper;
import com.scalar.db.saga.daemon.api.HealthResource;
import com.scalar.db.saga.daemon.api.SagaResource;
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
 */
public final class SagaServer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SagaServer.class);

  private final SagaServerConfig config;
  private final SagaManager sagaManager;
  private final Javalin app;

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
    config
        .definitionsPath()
        .ifPresentOrElse(
            this::registerDefinitions,
            () ->
                logger.warn("No saga definitions path configured; no sagas registered at startup"));
  }

  private void registerDefinitions(Path path) {
    try {
      int count;
      if (Files.isDirectory(path)) {
        try (Stream<Path> files = Files.list(path)) {
          List<Path> definitions = files.filter(SagaServer::isDefinitionFile).sorted().toList();
          definitions.forEach(sagaManager::register);
          count = definitions.size();
        }
      } else {
        sagaManager.register(path);
        count = 1;
      }
      if (count == 0) {
        logger.warn("No saga definition files (.json/.yaml/.yml) found in {}", path);
      } else {
        logger.info("Registered {} saga definition(s) from {}", count, path);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load saga definitions from " + path, e);
    }
  }

  private static boolean isDefinitionFile(Path path) {
    String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
    return name.endsWith(".json") || name.endsWith(".yaml") || name.endsWith(".yml");
  }

  private void registerRoutes() {
    HealthResource.register(app);
    ErrorMapper.register(app);
    SagaResource.register(app, sagaManager);
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
