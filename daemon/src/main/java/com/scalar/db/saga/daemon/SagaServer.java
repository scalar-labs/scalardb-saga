package com.scalar.db.saga.daemon;

import com.scalar.db.saga.daemon.api.ErrorMapper;
import com.scalar.db.saga.daemon.api.HealthResource;
import com.scalar.db.saga.daemon.api.SagaResource;
import com.scalar.db.saga.daemon.grpc.SagaServiceImpl;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone HTTP server that hosts a saga engine and exposes it over a REST API (daemon mode).
 *
 * <p>Construction builds the embedded {@link DefaultSagaOrchestrator} — creating the saga schema if
 * needed — from the configured properties, then loads and registers any declarative saga
 * definitions found at the configured definitions path. {@link #start()} starts background
 * recovery/retention tasks and binds the HTTP port. {@link #close()} stops accepting requests and
 * then drains in-flight sagas via {@link DefaultSagaOrchestrator#close()}.
 *
 * <p>Daemon mode is <b>declarative-only</b>: the server ships as a container, so operators cannot
 * supply code-step classes. A definition that declares a code step ({@code stepClass}) is rejected
 * at startup — use a declarative service step, or run the engine in embedded mode for code steps.
 */
public final class SagaServer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SagaServer.class);
  private static final long GRPC_SHUTDOWN_MIN_SECONDS = 30L;
  private static final long GRPC_SHUTDOWN_SLACK_MILLIS = 5_000L;

  private final SagaServerConfig config;
  private final DefaultSagaOrchestrator orchestrator;
  private final Javalin app;
  private final ExecutorService grpcExecutor;
  private final Server grpcServer;
  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile boolean grpcStarted;

  /**
   * Builds the server, its underlying saga engine (connecting to ScalarDB), and registers
   * configured definitions and routes. Does not bind the HTTP port — call {@link #start()}.
   *
   * @param config the server configuration
   */
  public SagaServer(SagaServerConfig config) {
    this(config, buildDefaultSagaOrchestrator(config));
  }

  /**
   * Visible for testing: builds the server around an already-constructed {@link
   * DefaultSagaOrchestrator}, so a test can inject a mock to exercise definition loading and route
   * wiring without a database.
   */
  SagaServer(SagaServerConfig config, DefaultSagaOrchestrator orchestrator) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator must not be null");
    this.app = Javalin.create();
    this.grpcExecutor = Executors.newVirtualThreadPerTaskExecutor();
    try {
      loadDefinitions();
      registerRoutes();
      this.grpcServer = buildGrpcServer();
    } catch (RuntimeException e) {
      // Release the executor and the store/DB connections held by the orchestrator if startup
      // wiring fails.
      grpcExecutor.shutdown();
      orchestrator.close();
      throw e;
    }
  }

  /**
   * Builds (does not bind) the gRPC server: it serves {@link SagaServiceImpl} over the same {@link
   * SagaServerConfig#host()} as HTTP on its own port, delegating to the same orchestrator the REST
   * routes use. The wait-heavy bounded-sync calls run on a virtual-thread executor (cheap
   * blocking); the inbound-size and metadata caps bound abuse on the unauthenticated port; server
   * reflection is deliberately not registered (it would expose the schema to any client).
   */
  private Server buildGrpcServer() {
    return NettyServerBuilder.forAddress(new InetSocketAddress(config.host(), config.grpcPort()))
        .addService(
            new SagaServiceImpl(
                orchestrator, config.syncTimeoutMillis(), config.syncMaxWaitMillis()))
        .maxInboundMessageSize(config.grpcMaxInboundMessageBytes())
        .maxInboundMetadataSize(8 * 1024)
        .executor(grpcExecutor)
        .permitKeepAliveTime(1, TimeUnit.MINUTES)
        .build();
  }

  private static DefaultSagaOrchestrator buildDefaultSagaOrchestrator(SagaServerConfig config) {
    Objects.requireNonNull(config, "config must not be null");
    DefaultSagaOrchestrator.Builder builder =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(ScalarDbSagaStoreFactory.create(config.properties()));
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
    orchestrator.register(definition);
  }

  private static boolean isDefinitionFile(Path path) {
    String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
    return name.endsWith(".json") || name.endsWith(".yaml") || name.endsWith(".yml");
  }

  private void registerRoutes() {
    HealthResource.register(app);
    ErrorMapper.register(app);
    SagaResource.register(app, orchestrator, config.syncTimeoutMillis());
  }

  /**
   * Starts background recovery/retention tasks, binds the HTTP port, and begins serving.
   *
   * @return this server
   */
  public SagaServer start() {
    try {
      orchestrator.startBackgroundTasks();
      app.start(config.host(), config.port());
      grpcServer.start();
      grpcStarted = true;
    } catch (RuntimeException e) {
      // Stop the (partially started) app/gRPC server and drain/close the orchestrator so a failed
      // start — e.g. a port bind failure after background tasks are running — does not leak
      // threads/connections. Two ports means two bind-failure windows; close() covers both.
      close();
      throw e;
    } catch (IOException e) {
      // io.grpc.Server.start() throws IOException on a bind failure (e.g. the gRPC port is in use).
      close();
      throw new UncheckedIOException("Failed to start gRPC server on port " + config.grpcPort(), e);
    }
    logger.info("SagaServer started on HTTP port {} and gRPC port {}", port(), grpcPort());
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

  /**
   * Returns the bound gRPC port (the actual ephemeral port when configured with {@code 0}). Only
   * meaningful after {@link #start()}.
   *
   * @return the bound gRPC port
   */
  public int grpcPort() {
    return grpcServer.getPort();
  }

  @Override
  public void close() {
    // Idempotent: start() calls close() on a bind failure, and try-with-resources will call it
    // again, so guard against draining the orchestrator (and closing the store) twice.
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Stop accepting new requests on both transports, drain in-flight gRPC calls, then drain sagas.
    app.stop();
    shutdownGrpc();
    // gRPC does not own the executor we supplied it, so shut it down ourselves. shutdownGrpc() has
    // already drained in-flight calls, so no tasks remain and a plain shutdown() suffices.
    grpcExecutor.shutdown();
    orchestrator.close();
    logger.info("SagaServer stopped");
  }

  /**
   * Gracefully shuts the gRPC server: stop accepting calls, drain in-flight ones up to {@link
   * #grpcDrainMillis()}, then force-cancel any stragglers. A no-op if the server never started (a
   * built-but-unbound server holds no resources).
   */
  private void shutdownGrpc() {
    if (!grpcStarted) {
      return;
    }
    grpcServer.shutdown();
    try {
      if (!grpcServer.awaitTermination(grpcDrainMillis(), TimeUnit.MILLISECONDS)) {
        grpcServer.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      grpcServer.shutdownNow();
    }
  }

  /**
   * The graceful gRPC drain window (ms). Derived from {@code sync_max_wait_millis} so an in-flight
   * bounded-sync {@code StartSaga}/{@code AwaitSaga} call can reach its own wait ceiling before we
   * force-cancel it: a fixed 30s drain would cut a legitimate 60s (default) wait in half, and the
   * gap would widen further whenever an operator raises {@code sync_max_wait_millis}. Kept at a
   * {@value #GRPC_SHUTDOWN_MIN_SECONDS}s floor for small ceilings, and padded with {@value
   * #GRPC_SHUTDOWN_SLACK_MILLIS}ms of slack so the call unwinds before the deadline rather than at
   * it.
   *
   * <p>Package-private for testing the derivation without binding a port or shutting down a server.
   */
  long grpcDrainMillis() {
    return Math.max(
        TimeUnit.SECONDS.toMillis(GRPC_SHUTDOWN_MIN_SECONDS),
        config.syncMaxWaitMillis() + GRPC_SHUTDOWN_SLACK_MILLIS);
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
