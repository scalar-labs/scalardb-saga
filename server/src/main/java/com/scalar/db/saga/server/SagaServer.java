package com.scalar.db.saga.server;

import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
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
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * needed — from the configured properties, then runs one configuration pass that validates and
 * registers the service files and declarative saga definitions (fatally, preserving fail-fast
 * boot). {@link #start()} starts background recovery/retention tasks, binds the enabled transports,
 * and — unless {@code reload.interval_seconds} is {@code 0} — begins re-running that pass
 * periodically, so service and definition changes apply without a restart. {@link #close()} stops
 * accepting requests, stops the reload pass, and then drains in-flight sagas via {@link
 * DefaultSagaOrchestrator#close()}.
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

  /**
   * The slice of the shutdown budget the in-flight reload pass may take before the saga drain
   * starts. Deliberately a small fixed window rather than the configured budget: the two waits are
   * sequential, an operator sizes the container's grace period from one number, and a pass is
   * milliseconds unless the store is slow — whereas the drain that follows needs the rest. A pass
   * still running when this elapses is interrupted, which the manager logs.
   */
  private static final long RELOAD_DRAIN_MILLIS = 5_000L;

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
  private final @Nullable ExecutorService grpcExecutor;
  private final @Nullable Server grpcServer;
  private final @Nullable HealthStatusManager grpcHealth;
  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile boolean grpcStarted;
  // The reload pipeline: the reconciler is also the boot loader; the manager is null when
  // reload.interval_seconds is 0 (startup-only loading).
  private final ConfigReconciler reconciler;
  private final @Nullable SagaConfigReloadManager reloadManager;

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
    Javalin httpServer = config.httpEnabled() ? createHttpServer(config, tlsMaterial) : null;
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
    // Same placeholder idiom: the catch below stops the manager only if it was built, and cannot
    // read the final field to find out.
    @Nullable SagaConfigReloadManager manager = null;
    try {
      provider = SecurityProviderFactory.create(config);
      this.securityProvider = provider;
      // Boot goes through the same pass reload uses: snapshot, validate (aggregated), apply. It is
      // the only thing that installs endpoints, so "any set a reload accepted also cold-boots a
      // fresh replica" holds by construction rather than by two paths agreeing.
      ConfigReconciler configReconciler =
          new ConfigReconciler(
              config.reloadConfig(),
              config.definitionsPath().orElse(null),
              config.callbackBaseUrl().isPresent() && config.callbackSecret().isPresent(),
              orchestrator.httpEndpointRegistrar(),
              new DefinitionStore() {
                @Override
                public void register(SagaDefinition definition) {
                  orchestrator.register(definition);
                }

                @Override
                public @Nullable SagaDefinition latest(String sagaName) {
                  return orchestrator.latestDefinition(sagaName);
                }

                @Override
                public boolean isRegistered(String sagaName, String version) {
                  return orchestrator.isDefinitionRegistered(sagaName, version);
                }
              },
              // What this replica's files describe is what it serves. The store keeps every
              // definition ever registered, so without this a saga could never be taken out of
              // service: removing its file would leave it startable here forever.
              orchestrator::servedDefinitions);
      this.reconciler = configReconciler;
      configReconciler.runOrThrow();
      // A daemon with no registered definitions cannot run any saga — fail fast rather than serve
      // a healthy but useless process. Reload cannot lift this later: the empty-transition guard
      // rejects a wind-down to zero, so a useful daemon always starts with at least one.
      if (configReconciler.appliedDefinitionCount() == 0) {
        throw new IllegalStateException(
            "No saga definitions registered. Set '"
                + SagaServerConfig.DEFINITIONS_PATH_KEY
                + "' to a file or directory containing at least one saga definition.");
      }
      logger.info("Registered {} saga definition(s)", configReconciler.appliedDefinitionCount());
      manager =
          config.reloadConfig().intervalSeconds() > 0
              ? new SagaConfigReloadManager(configReconciler, config.reloadConfig())
              : null;
      this.reloadManager = manager;
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
      // orchestrator if startup wiring fails. The reload manager may already exist — it is built
      // before the routes are wired, and wiring them can throw — and close() is not reached on
      // this path, so it is stopped here rather than left behind.
      if (manager != null) {
        manager.stop(System.nanoTime());
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
    SagaServiceImpl service =
        new SagaServiceImpl(orchestrator, config.syncTimeoutMillis(), config.syncMaxWaitMillis());
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
    // No endpoints here: the orchestrator is built with none, and the boot configuration pass
    // installs them through the same swap a reload uses. One conversion from service file to live
    // endpoint means a set that boots and a set that reloads cannot drift apart.
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
   * Builds the Javalin app with a bounded Jetty thread pool <b>and</b> a bounded job queue, so a
   * burst of slow requests can exhaust neither request-handling threads nor memory. {@code
   * maxThreads} caps concurrency; the idle timeout lets the pool shrink back toward {@code
   * minThreads} when quiet; and once all threads are busy, at most {@code maxQueuedRequests} more
   * requests wait before the server sheds load (fast failure) rather than queueing unboundedly.
   * When TLS is enabled, the listener is the HTTPS connector built by {@link #tlsConnector}, which
   * displaces Javalin's default plaintext one.
   */
  private static Javalin createHttpServer(SagaServerConfig config, @Nullable TlsMaterial tls) {
    int queueCap = config.httpMaxQueuedRequests();
    // A fixed-capacity queue (initial == growBy == max == cap): it never grows past the cap, so the
    // backlog is memory-bounded and the pool rejects further work once threads and queue are full.
    BlockingArrayQueue<Runnable> jobQueue = new BlockingArrayQueue<>(queueCap, queueCap, queueCap);
    return Javalin.create(
        cfg -> {
          cfg.jetty.threadPool =
              new QueuedThreadPool(
                  config.httpMaxThreads(),
                  config.httpMinThreads(),
                  (int) THREAD_POOL_IDLE_TIMEOUT_MILLIS,
                  jobQueue);
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
      if (reloadManager != null) {
        // After the transports: a replica never reloads before it is serving, so a bad candidate
        // set cannot wedge startup halfway (boot already applied the current set fatally above).
        reloadManager.start();
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

  /**
   * Runs one reload pass synchronously; for integration tests, which mutate the watched directories
   * and need a deterministic pass instead of waiting out the interval.
   *
   * @return whether the pass applied (or verified) cleanly
   */
  boolean reloadNow() {
    return reconciler.run();
  }

  @Override
  public void close() {
    // Idempotent: start() calls close() on a bind failure, and try-with-resources will call it
    // again, so guard against draining the orchestrator (and closing the store) twice.
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Reload stops first, before anything else winds down: a pass that ran during the drain could
    // swap the endpoint set out from under sagas that are still finishing their current step, and
    // stopping it here also guarantees no registration is in flight when the store closes.
    if (reloadManager != null) {
      reloadManager.stop(
          System.nanoTime()
              + TimeUnit.MILLISECONDS.toNanos(
                  Math.min(RELOAD_DRAIN_MILLIS, config.shutdownTimeoutMillis())));
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
