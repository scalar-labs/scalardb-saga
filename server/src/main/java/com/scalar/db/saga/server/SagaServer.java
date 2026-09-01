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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.ssl.SslContextFactory;
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
  private static final long DRAIN_MIN_SECONDS = 30L;
  private static final long DRAIN_SLACK_MILLIS = 5_000L;
  private static final long THREAD_POOL_IDLE_TIMEOUT_MILLIS = 60_000L;
  private static final long RATE_LIMIT_WINDOW_MILLIS = 60_000L;

  private final SagaServerConfig config;
  private final DefaultSagaOrchestrator orchestrator;
  private final SagaSecurityProvider securityProvider;
  // The shared per-principal saga-start limiter, or null when rate limiting is disabled. Shared by
  // both transports (REST before-handler and gRPC interceptor) so a caller's budget spans both.
  private final @Nullable RateLimiter rateLimiter;
  // The validated TLS material, or null when TLS is disabled. Loaded before anything else is
  // wired, so a bad certificate or key fails construction — long before either port could bind.
  private final @Nullable TlsMaterial tlsMaterial;
  // Each transport is null when disabled; SagaServerConfig guarantees at least one is enabled.
  private final @Nullable Javalin httpServer;
  private final @Nullable ExecutorService httpVirtualThreads;
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
    // TLS material is validated first, in its own guarded step: the orchestrator is the only
    // resource alive yet, and both transports below consume the result.
    TlsMaterial tlsMaterial = null;
    if (config.tlsEnabled()) {
      try {
        tlsMaterial =
            TlsMaterial.load(
                config.tlsCertChainPath().orElseThrow(),
                config.tlsPrivateKeyPath().orElseThrow(),
                Clock.systemUTC());
      } catch (RuntimeException e) {
        orchestrator.close();
        throw e;
      }
    }
    this.tlsMaterial = tlsMaterial;
    // Created here rather than inside createHttpServer so the server owns it: Jetty never stops
    // an executor it was handed, and close() must be able to wait for handler bodies.
    ExecutorService httpVirtualThreads =
        config.httpEnabled() ? Executors.newVirtualThreadPerTaskExecutor() : null;
    Javalin httpServer =
        httpVirtualThreads == null
            ? null
            : createHttpServer(config, tlsMaterial, httpVirtualThreads);
    ExecutorService grpcExecutor =
        config.grpcEnabled() ? Executors.newVirtualThreadPerTaskExecutor() : null;
    this.httpServer = httpServer;
    this.httpVirtualThreads = httpVirtualThreads;
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
      // Release the executors, the security provider, and the store/DB connections held by the
      // orchestrator if startup wiring fails.
      if (httpVirtualThreads != null) {
        httpVirtualThreads.shutdown();
      }
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
    SagaServiceImpl service = new SagaServiceImpl(orchestrator, config::syncWaitBoundMillis);
    AdminServiceImpl adminService = new AdminServiceImpl(orchestrator, adminDriveDeadlineMillis());
    SagaSecurityInterceptor security = new SagaSecurityInterceptor(securityProvider);
    NettyServerBuilder builder =
        NettyServerBuilder.forAddress(new InetSocketAddress(config.host(), config.grpcPort()))
            .addService(intercepted(service, security))
            .addService(intercepted(adminService, security))
            .addService(health.getHealthService())
            .executor(executor)
            .permitKeepAliveTime(1, TimeUnit.MINUTES);
    applyGrpcTransportSettings(builder, config, tlsMaterial);
    return builder.build();
  }

  /**
   * Applies the transport settings: the two inbound caps and, when TLS material is present,
   * transport security. The message cap is the load-bearing one: it is derived from the store's
   * payload cap, so dropping it would leave gRPC on its own 4 MiB default and the daemon would
   * accept a message the store then refuses to persist, surfacing as a write error that names the
   * store rather than the transport that let it in.
   *
   * <p>These caps also anchor a client-side classification: the SDK maps a bare {@code
   * RESOURCE_EXHAUSTED} carrying no error body to the non-retryable {@code UNMAPPED_SERVER_STATUS}
   * on the premise that every transport-level source of that status (the two caps here, plus the
   * keepalive enforcement in {@link #buildGrpcServer}) can never succeed on retry. Adding a
   * transport limit that refuses work a retry could outlast means revisiting {@code
   * GrpcClientSupport.unresolvedOrBare} first.
   *
   * <p>Visible for testing, for the same reason as {@link #applyEngineSettings}: a builder does not
   * read its settings back, so the only way to observe the forwarding is to watch it receive them.
   */
  static void applyGrpcTransportSettings(
      NettyServerBuilder builder, SagaServerConfig config, @Nullable TlsMaterial tls) {
    builder
        .maxInboundMessageSize(config.grpcMaxInboundMessageBytes())
        .maxInboundMetadataSize(config.grpcMaxInboundMetadataBytes());
    if (tls != null) {
      // The stable TLS API (GrpcSslContexts is still experimental in grpc 1.82), fed the validated
      // material re-encoded as PEM rather than the file paths: Netty parses these streams instead
      // of re-reading the files, so a rotation landing between validation and this build cannot
      // make gRPC serve bytes TlsMaterial never vetted, or diverge from what Jetty serves. With no
      // tcnative on the classpath, gRPC selects the JDK provider automatically; ALPN h2 and the
      // TLS 1.3/1.2 defaults come with it.
      builder.useTransportSecurity(tls.certChainPemStream(), tls.privateKeyPemStream());
    }
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
            .storeFactory(ScalarDbSagaStoreFactory.create(config.properties()));
    applyEngineSettings(builder, config);
    return builder.build();
  }

  /**
   * Applies every engine setting the operator configured, which is the whole point of the daemon's
   * configuration surface: a key that parses but never reaches the builder leaves the daemon on the
   * engine default while the operator believes they changed it.
   *
   * <p>Visible for testing, and separate from {@link #buildDefaultSagaOrchestrator} for the same
   * reason. Nothing on {@link DefaultSagaOrchestrator} reads these values back, so the only way to
   * observe the forwarding is to watch the builder receive them; a test passes a mock. Keeping the
   * store factory in the caller is what lets that test run without a database.
   */
  static void applyEngineSettings(
      DefaultSagaOrchestrator.Builder builder, SagaServerConfig config) {
    builder
        .ownerId(config.ownerId())
        .shutdownMode(config.shutdownMode())
        .shutdownTimeoutMillis(config.shutdownTimeoutMillis())
        .defaultSagaTimeoutMillis(config.defaultSagaTimeoutMillis())
        .maxTimelineEvents(config.detailMaxTimelineEvents())
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
  }

  /**
   * Registers one configured service as an HTTP endpoint, applying the optional outbound policy.
   * {@code allowedHosts} and {@code maxBodyBytes} are applied only when configured, so an unset key
   * leaves the engine's own default in place rather than overwriting it with a sentinel. Visible
   * for testing, like {@link #applyEngineSettings}.
   */
  static void addHttpEndpoint(
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
    // The path is a resolved config value, so a secret reference pasted onto the definitions key
    // arrives here as the secret's plaintext; failures below name the key and describe the value
    // via Redaction instead of echoing it. A missing path is refused up front because that is the
    // failure a pasted secret actually produces; letting it fall through would echo the "path" in
    // the definition parser's message and cause.
    if (!Files.exists(path)) {
      throw new IllegalArgumentException(
          "Invalid value for '"
              + SagaServerConfig.DEFINITIONS_PATH_KEY
              + "': no such file or directory "
              + Redaction.redacted(path.toString()));
    }
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
      // Thrown without the cause: an IOException message here is the path itself, which is the
      // resolved value. The class name keeps the diagnosis (permission denied or a path that
      // disappeared) without the echo.
      throw new IllegalStateException(
          "Failed to load saga definitions from '"
              + SagaServerConfig.DEFINITIONS_PATH_KEY
              + "' ("
              + e.getClass().getSimpleName()
              + ") "
              + Redaction.redacted(path.toString()));
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
        throw SagaDefinitionException.stepClassNotSupportedOnServer(
            definition.getName(), step.getName());
      }
    }
    // The server-wide default saga timeout is deliberately NOT baked into the definition here: it
    // is applied by the engine at deadline computation (see Builder#defaultSagaTimeoutMillis), so
    // the stored form always equals the parsed file and changing the default never turns an
    // unchanged file into a same-version content conflict at boot.
    orchestrator.register(definition);
  }

  private static boolean isDefinitionFile(Path path) {
    Path fileName = path.getFileName();
    if (fileName == null) {
      // Only a filesystem root has no file name, and a root is never a definition file.
      return false;
    }
    String name = fileName.toString().toLowerCase(Locale.ROOT);
    return name.endsWith(".json") || name.endsWith(".yaml") || name.endsWith(".yml");
  }

  /**
   * Builds the Javalin app with a bounded Jetty thread pool <b>and</b> a bounded job queue, so a
   * burst of slow requests can exhaust neither request-handling threads nor memory. The idle
   * timeout lets the pool shrink back toward {@code minThreads} when quiet, and once the pool is
   * saturated at most {@code maxQueuedRequests} more requests wait before the server sheds load
   * (fast failure) rather than queueing unboundedly. When TLS is enabled, the listener is the HTTPS
   * connector built by {@link #tlsConnector}, which displaces Javalin's default plaintext one.
   *
   * <p><b>Handlers run on virtual threads.</b> The pool is given a virtual-thread executor, so
   * Jetty dispatches each blocking handler invocation onto a virtual thread and the platform thread
   * returns to the pool immediately. A request waiting on its saga therefore costs a parked virtual
   * thread rather than one of {@code maxThreads} OS threads, which is what lets a synchronous start
   * wait without the request pool being the limit. gRPC has always worked this way (its handler
   * executor is virtual); this brings HTTP into line.
   *
   * <p>Two consequences worth naming. First, {@code maxThreads} no longer caps how many requests
   * are in flight — it caps how many can be <em>dispatched</em> at once. Jetty's execution strategy
   * routes a blocking handler invocation to the virtual-thread executor, so it never enters the job
   * queue below; measured on a 4-thread pool, 300 concurrent slow requests all ran, where before
   * the change 2 ran and the rest were shed. So the pool and its queue no longer shed saga-induced
   * load at the front door, and <b>nothing currently bounds concurrent saga execution</b> — the
   * engine's executors are unbounded and the rate limiter is off by default. Bounding it is
   * admission control's job and admission control does not exist yet; until it does, overload
   * degrades into store-connection contention and latency rather than failing fast. The queue is
   * still kept: it bounds the dispatch backlog, which is memory, and it is what makes the pool
   * reject rather than grow when the producer side saturates.
   *
   * <p>Second, because handlers now block on virtual threads, store I/O on the request path can pin
   * a carrier with a natively-blocking driver; see {@code todos/070} (Java 25) and {@code
   * todos/071} (the store bulkhead).
   */
  // Package-private for testing that handlers really land on virtual threads, without booting a
  // server; the same reason grpcDrainMillis() is. A silent revert to platform threads would leave
  // the daemon healthy and this fix inert, so it is worth a direct assertion.
  static Javalin createHttpServer(
      SagaServerConfig config, @Nullable TlsMaterial tls, ExecutorService virtualThreads) {
    int queueCap = config.httpMaxQueuedRequests();
    // A fixed-capacity queue (initial == growBy == max == cap): it never grows past the cap, so the
    // backlog is memory-bounded and the pool rejects further work once threads and queue are full.
    BlockingArrayQueue<Runnable> jobQueue = new BlockingArrayQueue<>(queueCap, queueCap, queueCap);
    return Javalin.create(
        cfg -> {
          QueuedThreadPool threadPool =
              new QueuedThreadPool(
                  config.httpMaxThreads(),
                  config.httpMinThreads(),
                  (int) THREAD_POOL_IDLE_TIMEOUT_MILLIS,
                  jobQueue);
          // Jetty's AdaptiveExecutionStrategy routes BLOCKING invocations here, which is how the
          // handler body ends up on a virtual thread while the pool keeps its bounded queue.
          threadPool.setVirtualThreadsExecutor(virtualThreads);
          cfg.jetty.threadPool = threadPool;
          if (tls != null) {
            // Registering any connector suppresses Javalin's default plaintext one (it is created
            // only when the connector list is empty), so TLS-on cannot leak a plaintext listener.
            cfg.jetty.addConnector(
                (server, httpConfig) -> tlsConnector(server, httpConfig, config, tls));
          }
        });
  }

  /**
   * Builds the HTTPS connector from the validated material: an in-memory PKCS12 keystore (nothing
   * touches disk) under a throwaway password behind Jetty's {@code SslContextFactory}, chained
   * {@code SslConnectionFactory -> HttpConnectionFactory}. Host and port live on the connector
   * because Javalin ignores {@code start(host, port)} arguments once a custom connector exists —
   * see the TLS branch in {@link #start()}.
   */
  private static ServerConnector tlsConnector(
      org.eclipse.jetty.server.Server server,
      HttpConfiguration baseConfig,
      SagaServerConfig config,
      TlsMaterial tls) {
    // The keystore never leaves memory, so the password protects nothing durable — but Jetty
    // initializes its KeyManagerFactory with the keystore password, so the same value must go to
    // both calls or key retrieval fails.
    char[] password = UUID.randomUUID().toString().toCharArray();
    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStore(tls.keyStore(password));
    sslContextFactory.setKeyStorePassword(new String(password));
    // Copy before mutating: Javalin hands the same HttpConfiguration instance to every connector
    // callback and would back a default connector with it too.
    HttpConfiguration httpsConfig = new HttpConfiguration(baseConfig);
    // Populates isSecure() and the https scheme. sniHostCheck stays off: with it on, Jetty answers
    // clients that dial by IP — Kubernetes probes, the smoke test, port-forwards — with a 400
    // "Invalid SNI" instead of serving them.
    httpsConfig.addCustomizer(new SecureRequestCustomizer(false));
    ServerConnector connector =
        new ServerConnector(
            server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(httpsConfig));
    connector.setHost(config.host());
    connector.setPort(config.httpPort());
    return connector;
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
    SagaResource.register(httpServer, orchestrator, config.syncWaitBoundMillis(Long.MAX_VALUE));
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
   * The bound on a single-saga admin inline drive: the shared synchronous-wait bound, with no
   * caller-supplied cap. Past the bound the durable transition is already recorded and the response
   * carries the saga's current state, so the bound only caps how long the request waits, never
   * correctness. Deriving it from {@code sync.max_wait_millis} keeps the drive inside the shutdown
   * drain window {@link #grpcDrainMillis()} derives from the same value.
   */
  private long adminDriveDeadlineMillis() {
    return config.syncWaitBoundMillis(Long.MAX_VALUE);
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
   * Warns when TLS is on but the async-callback base URL is plain {@code http} on a non-loopback
   * host: participants would dial the daemon's TLS port over plaintext and die at handshake — at
   * the first async step in production, not at startup. A warning rather than an error because the
   * callback URL may legitimately point at separate plaintext infrastructure (an internal ingress
   * that terminates TLS elsewhere); loopback stays quiet for the same local-dev reason as the other
   * guards. A base URL that does not parse as a URI draws a warning too: nothing downstream ever
   * parses the value (the callback provider builds URLs by plain concatenation), so silence here
   * would be silence everywhere.
   */
  private void warnIfCallbackBaseUrlIsPlaintextUnderTls() {
    if (!config.tlsEnabled() || config.callbackBaseUrl().isEmpty()) {
      return;
    }
    URI baseUrl;
    try {
      baseUrl = URI.create(config.callbackBaseUrl().get());
    } catch (IllegalArgumentException e) {
      // The value and the exception both stay out of the log: each embeds the raw value, which may
      // be a mis-pasted resolved secret.
      logger.warn(
          "'{}' is not a parseable URI, so whether it uses plain http under '{}' could not be"
              + " checked. An unparseable callback URL fails at the first async step either way;"
              + " fix the value.",
          SagaServerConfig.CALLBACK_BASE_URL_KEY,
          SagaServerConfig.TLS_ENABLED_KEY);
      return;
    }
    String host = baseUrl.getHost();
    if ("http".equalsIgnoreCase(baseUrl.getScheme())
        && host != null
        && !LoopbackHost.isLoopback(host)) {
      logger.warn(
          "'{}' uses plain http while '{}' is true. If it points back at this server, participants"
              + " will dial the TLS port over plaintext and fail at handshake on the first async"
              + " step. Use an https URL, or make sure the URL terminates at separate plaintext"
              + " infrastructure.",
          SagaServerConfig.CALLBACK_BASE_URL_KEY,
          SagaServerConfig.TLS_ENABLED_KEY);
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
      warnIfCallbackBaseUrlIsPlaintextUnderTls();
      orchestrator.startBackgroundTasks();
      if (httpServer != null) {
        if (config.tlsEnabled()) {
          // The TLS connector registered in createHttpServer carries host and port itself, and
          // Javalin silently ignores start(host, port) arguments once a custom connector exists —
          // passing them here would suggest they do something.
          httpServer.start();
        } else {
          httpServer.start(config.host(), config.httpPort());
        }
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
    if (tlsMaterial != null) {
      // The positive confirmation an operator (and the smoke test) looks for at boot. No path:
      // like every configured value, a path value is not provably a path — a secret reference
      // mis-placed on a path key resolves to the secret itself (see TlsMaterial's javadoc).
      logger.info(
          "TLS enabled for {}",
          httpServer != null && grpcServer != null
              ? "HTTP and gRPC"
              : httpServer != null ? "HTTP" : "gRPC");
    }
    logger.info(
        "SagaServer started ({}, {})",
        httpServer == null ? "HTTP disabled" : "HTTP port " + port(),
        grpcServer == null ? "gRPC disabled" : "gRPC port " + grpcPort());
    if (httpServer != null) {
      // Emitted so the packaged image can be checked from outside, the way the smoke test checks
      // that the epoll native transport actually loaded. Losing the virtual-thread executor would
      // leave every request served and every probe green while silently restoring the request-pool
      // ceiling this removes, so the boot log is the only external evidence available.
      logger.info("HTTP handlers run on virtual threads");
    }
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
    // Stop accepting new requests on the enabled transports, drain in-flight calls, then drain
    // sagas. Order matters: every handler body must finish before orchestrator.close() closes the
    // store underneath it.
    if (httpServer != null) {
      try {
        // Jetty runs Graceful.shutdown() from Server.doStop() only when stopTimeout is positive.
        // Without it, stop() goes straight to stopping the connectors and an in-flight request's
        // socket dies under it — the caller sees a closed channel rather than its response. The
        // connector's own Graceful (its endpoint set must empty) tracks those requests, so this
        // works whether or not the handler body is on a virtual thread.
        //
        // Set here rather than at construction, and only for a server that actually started:
        // gracefully stopping a Jetty that never bound raises a checked ExecutionException, and
        // Javalin stops the server itself when start() fails to bind — so a stop timeout baked in
        // at construction replaces a port-in-use error with that, exactly when the operator needs
        // the real one.
        org.eclipse.jetty.server.Server jetty = httpServer.jettyServer().server();
        if (jetty.isRunning()) {
          jetty.setStopTimeout(httpDrainMillis());
        }
        // Blocks until in-flight requests have been answered; see httpDrainMillis().
        httpServer.stop();
      } catch (Exception e) {
        // Catch broadly, and deliberately. Two failures land here and neither may abort the drains
        // below: a drain that overruns its window (Jetty ends FAILED, but the connectors are
        // stopped and the port released), and stopping a server that never bound — start() calls
        // close() on a bind failure, and gracefully stopping a never-started Jetty raises a checked
        // ExecutionException that would otherwise replace the bind error the caller needs to see.
        logger.warn("HTTP drain did not complete cleanly", e);
      }
    }
    shutdownGrpc();
    // Neither Jetty nor gRPC stops an executor it was handed, so shut both down ourselves.
    // shutdownGrpc() has already drained in-flight calls, so no gRPC tasks remain and a plain
    // shutdown() suffices. For HTTP, awaitTermination is the backstop for a drain that overran:
    // Jetty abandons those handler bodies without interrupting them, and they must not still be
    // running when the store closes.
    if (httpVirtualThreads != null) {
      shutdownHttpVirtualThreads(httpVirtualThreads);
    }
    if (grpcExecutor != null) {
      grpcExecutor.shutdown();
    }
    closeSecurityProvider(securityProvider);
    orchestrator.close();
    logger.info("SagaServer stopped");
  }

  /**
   * Shuts down the executor that ran the HTTP handler bodies and waits for any that outlived the
   * Jetty drain, so none is still touching the store when {@link DefaultSagaOrchestrator#close()}
   * closes it. Bounded by the same drain window, and only ever reached with stragglers after that
   * window already overran, so it logs rather than blocking shutdown further.
   */
  private void shutdownHttpVirtualThreads(ExecutorService httpVirtualThreads) {
    httpVirtualThreads.shutdown();
    try {
      if (!httpVirtualThreads.awaitTermination(httpDrainMillis(), TimeUnit.MILLISECONDS)) {
        logger.warn("HTTP handlers still running after the drain window; closing the store anyway");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Interrupted while waiting for HTTP handlers to finish");
    }
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
   * {@value #DRAIN_MIN_SECONDS}s floor for small ceilings, and padded with {@value
   * #DRAIN_SLACK_MILLIS}ms of slack so the call unwinds before the deadline rather than at it.
   *
   * <p>Package-private for testing the derivation without binding a port or shutting down a server.
   */
  long grpcDrainMillis() {
    return drainMillis(config);
  }

  /**
   * The graceful HTTP drain window (ms) — the same derivation, for the same reason: a REST handler
   * blocked in a bounded synchronous start waits up to {@code sync.max_wait_millis}, so a shorter
   * window would cut a legitimate request short. Jetty applies it as the server's stop timeout, and
   * past it {@code stop()} abandons whatever is still running.
   *
   * <p>One derivation serves both transports deliberately.
   *
   * <p>Package-private for testing the derivation without binding a port.
   */
  long httpDrainMillis() {
    return drainMillis(config);
  }

  /**
   * The graceful drain window (ms) for either transport: the {@code sync.max_wait_millis} ceiling a
   * request may legitimately wait, padded with {@value #DRAIN_SLACK_MILLIS}ms of slack so the call
   * unwinds before the deadline rather than at it, and floored at {@value #DRAIN_MIN_SECONDS}s for
   * small ceilings. Static because {@link #createHttpServer} needs it before an instance exists.
   */
  static long drainMillis(SagaServerConfig config) {
    return Math.max(
        TimeUnit.SECONDS.toMillis(DRAIN_MIN_SECONDS),
        config.syncMaxWaitMillis() + DRAIN_SLACK_MILLIS);
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
