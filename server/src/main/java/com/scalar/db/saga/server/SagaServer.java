package com.scalar.db.saga.server;

import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.server.api.CallbackResource;
import com.scalar.db.saga.server.api.ErrorMapper;
import com.scalar.db.saga.server.api.HealthResource;
import com.scalar.db.saga.server.api.HmacCallbackUrlProvider;
import com.scalar.db.saga.server.api.RateLimitHandler;
import com.scalar.db.saga.server.api.RateLimiter;
import com.scalar.db.saga.server.api.SagaAdminResource;
import com.scalar.db.saga.server.api.SagaResource;
import com.scalar.db.saga.server.grpc.AdminServiceImpl;
import com.scalar.db.saga.server.grpc.SagaRateLimitInterceptor;
import com.scalar.db.saga.server.grpc.SagaSecurityInterceptor;
import com.scalar.db.saga.server.grpc.SagaServiceImpl;
import com.scalar.db.saga.server.security.SagaSecurityHandler;
import com.scalar.db.saga.server.security.SagaSecurityProvider;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

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
  private static final long RATE_LIMIT_WINDOW_MILLIS = 60_000L;

  private final SagaServerConfig config;
  private final DefaultSagaOrchestrator orchestrator;
  private final SagaSecurityProvider securityProvider;
  // The shared per-principal saga-start limiter, or null when rate limiting is disabled. Shared by
  // both transports (REST before-handler and gRPC interceptor) so a caller's budget spans both.
  private final @Nullable RateLimiter rateLimiter;
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
    this.rateLimiter =
        config.maxStartRequestsPerMinute() > 0
            ? new RateLimiter(config.maxStartRequestsPerMinute(), RATE_LIMIT_WINDOW_MILLIS)
            : null;
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
   * authenticated/authorized by the same {@link SagaSecurityProvider} as REST, and — when rate
   * limiting is enabled — a {@link SagaRateLimitInterceptor} sharing the REST transport's {@link
   * RateLimiter}, so a caller's saga-start budget spans both transports. It also registers the
   * standard {@code grpc.health.v1.Health} service — deliberately <b>not</b> intercepted, so a
   * gRPC-only deployment stays probeable (e.g. by K8s-native gRPC probes) without a credential. The
   * wait-heavy bounded-sync calls run on a virtual-thread executor (cheap blocking); the
   * inbound-size and metadata caps bound abuse; server reflection is deliberately not registered
   * (it would expose the schema to any client).
   */
  private Server buildGrpcServer(ExecutorService executor, HealthStatusManager health) {
    SagaServiceImpl service =
        new SagaServiceImpl(orchestrator, config.syncTimeoutMillis(), config.syncMaxWaitMillis());
    AdminServiceImpl adminService = new AdminServiceImpl(orchestrator, adminDriveDeadlineMillis());
    SagaSecurityInterceptor security = new SagaSecurityInterceptor(securityProvider);
    return NettyServerBuilder.forAddress(new InetSocketAddress(config.host(), config.grpcPort()))
        .addService(intercepted(service, security))
        .addService(intercepted(adminService, security))
        .addService(health.getHealthService())
        .maxInboundMessageSize(config.grpcMaxInboundMessageBytes())
        .maxInboundMetadataSize(config.grpcMaxInboundMetadataBytes())
        .executor(executor)
        .permitKeepAliveTime(1, TimeUnit.MINUTES)
        .build();
  }

  /**
   * Wraps a service with the security interceptor and, when rate limiting is enabled, the
   * rate-limit interceptor. This is <b>load-bearing for every service that carries privileged RPCs,
   * the admin service above all</b>: a bare {@code addService(adminService)} would ship every
   * destructive admin RPC unauthenticated (identical to the deliberately-exempt health service).
   * The interceptors are applied per service, so each intercepted service must be wrapped here — a
   * missed wrap does not fail to compile, it silently exposes the service. {@code interceptForward}
   * runs the interceptors in list order, so authentication runs first — putting the identity on the
   * {@code Context} — before rate limiting reads it (the gRPC analogue of the REST handler order).
   */
  private ServerServiceDefinition intercepted(
      BindableService service, SagaSecurityInterceptor security) {
    List<ServerInterceptor> interceptors = new ArrayList<>();
    interceptors.add(security);
    if (rateLimiter != null) {
      interceptors.add(new SagaRateLimitInterceptor(rateLimiter));
    }
    return ServerInterceptors.interceptForward(service, interceptors);
  }

  private static DefaultSagaOrchestrator buildDefaultSagaOrchestrator(SagaServerConfig config) {
    Objects.requireNonNull(config, "config must not be null");
    DefaultSagaOrchestrator.Builder builder =
        DefaultSagaOrchestrator.newBuilder()
            .storeFactory(ScalarDbSagaStoreFactory.create(config.properties()))
            .ownerId(config.ownerId())
            .shutdownMode(config.shutdownMode())
            .shutdownTimeoutMillis(config.shutdownTimeoutMillis())
            .recoveryConfig(config.recoveryConfig())
            .retentionConfig(config.retentionConfig());
    config.services().forEach((name, service) -> addHttpEndpoint(builder, name, service));
    // Enable async-callback provisioning only when both the callback base URL and secret are set;
    // otherwise no provider is wired and registering an async definition fails fast (in the
    // engine).
    if (config.callbackBaseUrl().isPresent() && config.callbackSecret().isPresent()) {
      builder.callbackUrlProvider(
          new HmacCallbackUrlProvider(
              config.callbackBaseUrl().get(), config.callbackSecret().get(), Clock.systemUTC()));
    }
    return builder.build();
  }

  /**
   * Registers one configured service as an HTTP endpoint, applying the optional outbound policy.
   * {@code allowedHosts} and {@code maxBodyBytes} are applied only when configured, so an unset key
   * leaves the engine's own default in place rather than overwriting it with a sentinel.
   */
  private static void addHttpEndpoint(
      DefaultSagaOrchestrator.Builder builder,
      String name,
      SagaServerConfig.ServiceConfig service) {
    DefaultSagaOrchestrator.Builder.HttpEndpointBuilder endpoint =
        builder.httpEndpoint(name, service.baseUrl());
    if (!service.allowedHosts().isEmpty()) {
      endpoint.allowedHosts(service.allowedHosts().toArray(new String[0]));
    }
    if (service.maxBodyBytes() > 0) {
      endpoint.maxBodyBytes(service.maxBodyBytes());
    }
    endpoint.defaultHeaders(service.headers()).add();
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
   * Builds the Javalin app with a bounded Jetty thread pool <b>and</b> a bounded job queue, so a
   * burst of slow requests can exhaust neither request-handling threads nor memory. {@code
   * maxThreads} caps concurrency; the idle timeout lets the pool shrink back toward {@code
   * minThreads} when quiet; and once all threads are busy, at most {@code maxQueuedRequests} more
   * requests wait before the server sheds load (fast failure) rather than queueing unboundedly.
   */
  private static Javalin createHttpServer(SagaServerConfig config) {
    int queueCap = config.httpMaxQueuedRequests();
    // A fixed-capacity queue (initial == growBy == max == cap): it never grows past the cap, so the
    // backlog is memory-bounded and the pool rejects further work once threads and queue are full.
    BlockingArrayQueue<Runnable> jobQueue = new BlockingArrayQueue<>(queueCap, queueCap, queueCap);
    return Javalin.create(
        cfg ->
            cfg.jetty.threadPool =
                new QueuedThreadPool(
                    config.httpMaxThreads(),
                    config.httpMinThreads(),
                    (int) THREAD_POOL_IDLE_TIMEOUT_MILLIS,
                    jobQueue));
  }

  private void registerRoutes(Javalin httpServer) {
    // The RBAC handler authenticates every matched route according to the SagaOperation the route
    // declares; the two routes that carry no caller credential (the liveness probe, and the
    // async-callback route with its own per-step HMAC) are tagged with an auth-exempt operation
    // rather than listed here, so a route's policy travels with its registration.
    SagaSecurityHandler.register(httpServer, securityProvider);
    // Rate limiting runs after auth (it keys off the resolved principal) and only when enabled.
    // Both
    // are beforeMatched handlers, and this registration order is what puts the limiter after the
    // authenticator; Javalin would run either ahead of the other only if they were on different
    // stages. The same limiter also gates the gRPC transport (see buildGrpcServer), so the budget
    // is
    // per caller, not per port.
    if (rateLimiter != null) {
      RateLimitHandler.register(httpServer, rateLimiter);
    }
    HealthResource.register(httpServer);
    ErrorMapper.register(httpServer);
    SagaResource.register(httpServer, orchestrator, config.syncTimeoutMillis());
    SagaAdminResource.register(httpServer, orchestrator, adminDriveDeadlineMillis());
    // The async-callback route exists only when a callback secret is configured; without it there
    // is nothing to authenticate callbacks against, so async completion is not enabled.
    config
        .callbackSecret()
        .ifPresent(
            secret ->
                CallbackResource.register(
                    httpServer,
                    orchestrator,
                    secret,
                    config.callbackMaxAgeSeconds(),
                    Clock.systemUTC()));
  }

  /**
   * The bound on a single-saga admin inline drive: {@code sync.max_wait_millis} — the daemon's
   * standing ceiling on how long any request may hold a thread — tightened by {@code
   * sync.timeout_millis} when that is set. This mirrors the terms {@code
   * SagaServiceImpl.computeBoundMillis} applies on the request-thread paths, minus the per-call
   * gRPC client deadline, which has no REST analogue. Past the bound the durable transition is
   * already recorded and the response carries the saga's current state, so the bound only caps how
   * long the request waits, never correctness. Reusing {@code sync.max_wait_millis} keeps the drive
   * inside the shutdown drain window {@link #grpcDrainMillis()} derives from the same value.
   */
  private long adminDriveDeadlineMillis() {
    long bound = config.syncMaxWaitMillis();
    if (config.syncTimeoutMillis() > 0L) {
      bound = Math.min(bound, config.syncTimeoutMillis());
    }
    return bound;
  }

  /**
   * Fails fast if the server would start unauthenticated on a network-reachable interface: the
   * {@code noop} provider bound to a non-loopback host. The operator must configure a real
   * provider, bind to a loopback address, or explicitly enable insecure mode via {@code
   * insecure_mode.enabled=true}. This closes the insecure-by-default combination where an
   * unconfigured daemon would serve full-access requests to anyone on the network.
   */
  private void ensureSecureBindingOrAcknowledged() {
    if (config.securityProvider().equals("noop")
        && !LoopbackHost.isLoopback(config.host())
        && !config.insecureModeEnabled()) {
      throw new IllegalArgumentException(
          "Refusing to start unauthenticated on a network-reachable interface: '"
              + SagaServerConfig.SECURITY_PROVIDER_KEY
              + "="
              + config.securityProvider()
              + "' disables authentication, but '"
              + SagaServerConfig.HOST_KEY
              + "="
              + config.host()
              + "' is not a loopback address. Configure a real security provider (jwt or apikey),"
              + " bind '"
              + SagaServerConfig.HOST_KEY
              + "' to a loopback address, or set '"
              + SagaServerConfig.INSECURE_MODE_ENABLED_KEY
              + "=true' to acknowledge running without authentication on an exposed interface.");
    }
  }

  /**
   * Warns when the saga-start rate limit is configured under the {@code noop} provider, where it
   * cannot behave per-principal. {@code noop} resolves every request to the single {@code
   * "anonymous"} principal, so the per-principal limiter degrades to one global bucket shared by
   * all callers — one client can consume the whole budget for everyone. Not an error (a global cap
   * is the only sensible behavior without a per-caller identity), but the operator should know the
   * limit is only meaningful once a real provider (jwt or apikey) is configured.
   */
  private void warnIfRateLimitGlobalUnderNoop() {
    if (rateLimiter != null && config.securityProvider().equals("noop")) {
      logger.warn(
          "'{}={}' is set, but '{}={}' resolves every request to a single principal, so the"
              + " per-principal saga-start limit acts as one global bucket shared by all callers."
              + " Configure a real security provider (jwt or apikey) for the limit to apply"
              + " per-principal.",
          SagaServerConfig.MAX_START_REQUESTS_PER_MINUTE_KEY,
          config.maxStartRequestsPerMinute(),
          SagaServerConfig.SECURITY_PROVIDER_KEY,
          config.securityProvider());
    }
  }

  /**
   * Starts background recovery/retention tasks, binds the HTTP port, and begins serving.
   *
   * @return this server
   */
  public SagaServer start() {
    try {
      ensureSecureBindingOrAcknowledged();
      warnIfRateLimitGlobalUnderNoop();
      orchestrator.startBackgroundTasks();
      if (httpServer != null) {
        httpServer.start(config.host(), config.httpPort());
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
   * The graceful gRPC drain window (ms). Derived from {@code sync.max_wait_millis} so an in-flight
   * bounded-sync {@code StartSaga}/{@code AwaitSaga} call can reach its own wait ceiling before we
   * force-cancel it: a fixed 30s drain would cut a legitimate 60s (default) wait in half, and the
   * gap would widen further whenever an operator raises {@code sync.max_wait_millis}. Kept at a
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
   * Redirects {@code java.util.logging} into SLF4J, so every log line the process emits goes
   * through the same Logback configuration.
   *
   * <p>gRPC logs through {@code java.util.logging} directly rather than SLF4J. Without this bridge
   * its records skip Logback entirely: they print in the JUL default format instead of the
   * configured pattern, and the configured log level does not reach them — so an operator can
   * neither quiet gRPC transport noise nor turn gRPC detail up while debugging, and a log collector
   * configured for one format silently mis-parses the other. (Netty needs no such help; it detects
   * SLF4J on its own.)
   *
   * <p>Removing the root handlers first drops JUL's default {@code ConsoleHandler}, which would
   * otherwise keep printing every record a second time in its own format.
   *
   * <p>Called only from {@link SagaServerCommand#run}, deliberately. Installing a handler on the
   * JVM-wide JUL root logger is an application's decision to make, not a library's: an application
   * that embeds the engine must be free to configure its own logging, so this must not run merely
   * because a {@link SagaServer} was constructed.
   *
   * <p>The bridge is only cheap when paired with Logback's {@code LevelChangePropagator}, which the
   * shipped {@code logback.xml} enables: without it JUL builds a {@code LogRecord} for every call
   * even when the level is disabled, because JUL itself never learns the level is off.
   *
   * <p>Package-private for testing.
   */
  static void installJulToSlf4jBridge() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }
}
