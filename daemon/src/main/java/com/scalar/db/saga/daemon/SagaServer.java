package com.scalar.db.saga.daemon;

import com.scalar.db.saga.daemon.api.ErrorMapper;
import com.scalar.db.saga.daemon.api.HealthResource;
import com.scalar.db.saga.daemon.api.RateLimitHandler;
import com.scalar.db.saga.daemon.api.SagaResource;
import com.scalar.db.saga.daemon.grpc.SagaSecurityInterceptor;
import com.scalar.db.saga.daemon.grpc.SagaServiceImpl;
import com.scalar.db.saga.daemon.security.AuthExemptions;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import com.scalar.db.saga.daemon.security.SecurityHandler;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
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
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone server that hosts a saga engine and exposes it over a REST and/or gRPC API (daemon
 * mode).
 *
 * <p>Construction builds the embedded {@link DefaultSagaOrchestrator} — creating the saga schema if
 * needed — from the configured properties, then loads and registers any declarative saga
 * definitions found at the configured definitions path. {@link #start()} starts background
 * recovery/retention tasks and binds the enabled transports. {@link #close()} stops accepting
 * requests and then drains in-flight sagas via {@link DefaultSagaOrchestrator#close()}.
 *
 * <p>Each transport is independently toggleable ({@link SagaServerConfig#httpEnabled()} / {@link
 * SagaServerConfig#grpcEnabled()}, both on by default); the config layer guarantees at least one is
 * enabled. Each enabled transport carries its own health check — HTTP serves {@code GET /health}
 * and gRPC registers the standard {@code grpc.health.v1.Health} service — so whichever transport(s)
 * an operator runs stays probeable.
 *
 * <p>Daemon mode is <b>declarative-only</b>: the server ships as a container, so operators cannot
 * supply code-step classes. A definition that declares a code step ({@code stepClass}) is rejected
 * at startup — use a declarative service step, or run the engine in embedded mode for code steps.
 */
public final class SagaServer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SagaServer.class);
  private static final long GRPC_SHUTDOWN_MIN_SECONDS = 30L;
  private static final long GRPC_SHUTDOWN_SLACK_MILLIS = 5_000L;
  private static final long THREAD_POOL_IDLE_TIMEOUT_MILLIS = 60_000L;

  private final SagaServerConfig config;
  private final DefaultSagaOrchestrator orchestrator;
  private final SagaSecurityProvider securityProvider;
  // Each transport is null when disabled; SagaServerConfig guarantees at least one is enabled.
  private final @Nullable Javalin httpServer;
  private final @Nullable ExecutorService grpcExecutor;
  private final @Nullable Server grpcServer;
  private final @Nullable HealthStatusManager grpcHealth;
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
    Javalin httpServer = config.httpEnabled() ? createHttpServer(config) : null;
    ExecutorService grpcExecutor =
        config.grpcEnabled() ? Executors.newVirtualThreadPerTaskExecutor() : null;
    this.httpServer = httpServer;
    this.grpcExecutor = grpcExecutor;
    // Built before wiring the transports so both can share one provider (the gRPC interceptor uses
    // it too). A null placeholder lets the catch below close it only if it was built.
    @Nullable SagaSecurityProvider provider = null;
    try {
      provider = SecurityProviderFactory.create(config);
      this.securityProvider = provider;
      loadDefinitions();
      if (httpServer != null) {
        registerRoutes(httpServer);
      }
      if (grpcExecutor != null) {
        HealthStatusManager health = new HealthStatusManager();
        this.grpcHealth = health;
        this.grpcServer = buildGrpcServer(grpcExecutor, health);
      } else {
        this.grpcHealth = null;
        this.grpcServer = null;
      }
    } catch (RuntimeException e) {
      // Release the executor, the security provider, and the store/DB connections held by the
      // orchestrator if startup wiring fails.
      if (grpcExecutor != null) {
        grpcExecutor.shutdown();
      }
      closeSecurityProvider(provider);
      orchestrator.close();
      throw e;
    }
  }

  /**
   * Builds (does not bind) the gRPC server: it serves {@link SagaServiceImpl} over the same {@link
   * SagaServerConfig#host()} as HTTP on its own port, delegating to the same orchestrator the REST
   * routes use. The saga service is wrapped with {@link SagaSecurityInterceptor} so gRPC calls are
   * authenticated/authorized by the same {@link SagaSecurityProvider} as REST. It also registers
   * the standard {@code grpc.health.v1.Health} service — deliberately <b>not</b> intercepted, so a
   * gRPC-only deployment stays probeable (e.g. by K8s-native gRPC probes) without a credential. The
   * wait-heavy bounded-sync calls run on a virtual-thread executor (cheap blocking); the
   * inbound-size and metadata caps bound abuse; server reflection is deliberately not registered
   * (it would expose the schema to any client).
   */
  private Server buildGrpcServer(ExecutorService executor, HealthStatusManager health) {
    return NettyServerBuilder.forAddress(new InetSocketAddress(config.host(), config.grpcPort()))
        .addService(
            ServerInterceptors.intercept(
                new SagaServiceImpl(
                    orchestrator, config.syncTimeoutMillis(), config.syncMaxWaitMillis()),
                new SagaSecurityInterceptor(securityProvider)))
        .addService(health.getHealthService())
        .maxInboundMessageSize(config.grpcMaxInboundMessageBytes())
        .maxInboundMetadataSize(8 * 1024)
        .executor(executor)
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
    orchestrator.register(applyDefaultTimeout(definition));
  }

  /**
   * Applies the server-wide default saga timeout to a definition that specified none ({@code
   * timeoutMillis == 0}), so a daemon-hosted saga cannot run without a deadline. A definition's own
   * timeout is left untouched, and when no default is configured this is a no-op.
   */
  private SagaDefinition applyDefaultTimeout(SagaDefinition definition) {
    long defaultTimeout = config.defaultSagaTimeoutMillis();
    if (defaultTimeout > 0 && definition.getTimeoutMillis() == 0) {
      logger.info(
          "Applying default timeout of {} ms to saga '{}' (no timeout set)",
          defaultTimeout,
          definition.getName());
      return definition.withTimeoutMillis(defaultTimeout);
    }
    return definition;
  }

  private static boolean isDefinitionFile(Path path) {
    String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
    return name.endsWith(".json") || name.endsWith(".yaml") || name.endsWith(".yml");
  }

  /**
   * Builds the Javalin app with a bounded Jetty thread pool, so a burst of slow requests cannot
   * exhaust request-handling threads. {@code maxThreads} caps concurrency; the idle timeout lets
   * the pool shrink back toward {@code minThreads} when quiet.
   */
  private static Javalin createHttpServer(SagaServerConfig config) {
    return Javalin.create(
        cfg ->
            cfg.jetty.threadPool =
                new QueuedThreadPool(
                    config.maxThreads(),
                    config.minThreads(),
                    (int) THREAD_POOL_IDLE_TIMEOUT_MILLIS));
  }

  private void registerRoutes(Javalin httpServer) {
    // The RBAC before-handler authenticates every request except the exempt paths. The liveness
    // probe carries no user credential; the async-callback route (PR B) authenticates with its own
    // per-step HMAC token and registers CallbackResource.PATH here when that work merges.
    SecurityHandler.register(httpServer, securityProvider, AuthExemptions.of(HealthResource.PATH));
    // Rate limiting runs after auth (it keys off the resolved principal) and only when enabled;
    // registered before the routes so it gates saga-start requests.
    if (config.maxStartRequestsPerMinute() > 0) {
      RateLimitHandler.register(httpServer, config.maxStartRequestsPerMinute());
    }
    HealthResource.register(httpServer);
    ErrorMapper.register(httpServer);
    SagaResource.register(httpServer, orchestrator, config.syncTimeoutMillis());
  }

  /**
   * Starts background recovery/retention tasks, binds the HTTP port, and begins serving.
   *
   * @return this server
   */
  public SagaServer start() {
    try {
      orchestrator.startBackgroundTasks();
      if (httpServer != null) {
        httpServer.start(config.host(), config.port());
      }
      if (grpcServer != null) {
        grpcServer.start();
        grpcStarted = true;
      }
    } catch (RuntimeException e) {
      // Stop the (partially started) HTTP/gRPC server and drain/close the orchestrator so a failed
      // start — e.g. a port bind failure after background tasks are running — does not leak
      // threads/connections. Two ports means two bind-failure windows; close() covers both.
      close();
      throw e;
    } catch (IOException e) {
      // io.grpc.Server.start() throws IOException on a bind failure (e.g. the gRPC port is in use).
      close();
      throw new UncheckedIOException("Failed to start gRPC server on port " + config.grpcPort(), e);
    }
    logger.info(
        "SagaServer started ({}, {})",
        httpServer == null ? "HTTP disabled" : "HTTP port " + port(),
        grpcServer == null ? "gRPC disabled" : "gRPC port " + grpcPort());
    return this;
  }

  /**
   * Returns the bound HTTP port (the actual ephemeral port when configured with {@code 0}), or
   * {@code -1} when the HTTP transport is disabled. Only meaningful after {@link #start()}.
   *
   * @return the bound port, or {@code -1} if HTTP is disabled
   */
  public int port() {
    return httpServer == null ? -1 : httpServer.port();
  }

  /**
   * Returns the bound gRPC port (the actual ephemeral port when configured with {@code 0}), or
   * {@code -1} when the gRPC transport is disabled (or the server is not started). Only meaningful
   * after {@link #start()}.
   *
   * @return the bound gRPC port, or {@code -1} if gRPC is disabled
   */
  public int grpcPort() {
    return grpcServer == null ? -1 : grpcServer.getPort();
  }

  @Override
  public void close() {
    // Idempotent: start() calls close() on a bind failure, and try-with-resources will call it
    // again, so guard against draining the orchestrator (and closing the store) twice.
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Stop accepting new requests on the enabled transports, drain in-flight gRPC calls, then drain
    // sagas.
    if (httpServer != null) {
      httpServer.stop();
    }
    shutdownGrpc();
    // gRPC does not own the executor we supplied it, so shut it down ourselves. shutdownGrpc() has
    // already drained in-flight calls, so no tasks remain and a plain shutdown() suffices.
    if (grpcExecutor != null) {
      grpcExecutor.shutdown();
    }
    closeSecurityProvider(securityProvider);
    orchestrator.close();
    logger.info("SagaServer stopped");
  }

  /**
   * Closes a security provider, releasing any resources it holds (e.g. a JWKS-refresh client),
   * logging and swallowing any failure so it never masks the orchestrator drain that follows.
   * Tolerates a {@code null} provider so the constructor's failure path can call it before the
   * field is set.
   */
  private static void closeSecurityProvider(@Nullable SagaSecurityProvider provider) {
    if (provider == null) {
      return;
    }
    try {
      provider.close();
    } catch (Exception e) {
      logger.warn("Failed to close security provider '{}'", provider.name(), e);
    }
  }

  /**
   * Gracefully shuts the gRPC server: mark it {@code NOT_SERVING} for any in-flight health probe,
   * stop accepting calls, drain in-flight ones up to {@link #grpcDrainMillis()}, then force-cancel
   * any stragglers. A no-op if gRPC is disabled or the server never started (a built-but-unbound
   * server holds no resources).
   */
  private void shutdownGrpc() {
    Server server = grpcServer;
    if (server == null || !grpcStarted) {
      return;
    }
    if (grpcHealth != null) {
      grpcHealth.enterTerminalState();
    }
    server.shutdown();
    try {
      if (!server.awaitTermination(grpcDrainMillis(), TimeUnit.MILLISECONDS)) {
        server.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      server.shutdownNow();
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
