package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.ShutdownMode;
import com.scalar.db.saga.server.api.HmacCallbackUrlProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.NettyServerBuilder;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link SagaServer}'s startup wiring (definition loading + code-step rejection +
 * cleanup), using an injected mock {@link DefaultSagaOrchestrator} — no ScalarDB and no HTTP port.
 * The server parses each definition file itself (to reject code steps), so the files must be valid
 * declarative definitions; the mock {@code register(...)} is a no-op, so no service endpoints are
 * needed.
 */
class SagaServerTest {

  // One keytool subprocess for the whole class: every TLS test consumes this same generated pair.
  @TempDir static Path tlsDir;
  private static TlsTestCerts.PemPair tls;

  @BeforeAll
  static void generateTlsMaterial() {
    tls = TlsTestCerts.generateRsa(tlsDir, "tls");
  }

  /** A minimal valid declarative (service-step) definition as JSON. */
  private static String declarativeJson(String name) {
    return "{\"name\":\""
        + name
        + "\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\",\"service\":\"svc\","
        + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
        + "\"compensation\":{\"method\":\"POST\",\"path\":\"/y\"}}]}";
  }

  /** The same definition as YAML. */
  private static String declarativeYaml(String name) {
    return "name: "
        + name
        + "\nmode: SAGA\nsteps:\n  - name: s\n    service: svc\n    execution:\n"
        + "      method: POST\n      path: /x\n    compensation:\n      method: POST\n      path: /y\n";
  }

  /** A code-step definition (rejected in daemon mode). */
  private static String codeStepJson(String name) {
    return "{\"name\":\""
        + name
        + "\",\"steps\":[{\"name\":\"s\",\"stepClass\":\"com.example.Foo\"}]}";
  }

  /**
   * Writes the service file for the {@code svc} service every test definition references (the boot
   * pass cross-checks definition service references against the candidate service set), and returns
   * the services directory. Nested inside the definitions dir; the definitions walk skips
   * directories, so the nesting is invisible to it.
   */
  private static Path svcServices(Path dir) throws IOException {
    Path services = Files.createDirectories(dir.resolve("services"));
    Files.writeString(services.resolve("svc.properties"), "base_url=http://127.0.0.1:1\n");
    return services;
  }

  /**
   * A mock orchestrator with the endpoint-swap seam stubbed. The boot configuration pass installs
   * endpoints through it — it is the only thing that does, now that the builder no longer pre-loads
   * them — so every server built here needs it to answer. Lenient because a test whose construction
   * fails before the pass never reaches it.
   */
  private static DefaultSagaOrchestrator mockOrchestrator() {
    DefaultSagaOrchestrator orchestrator = mock(DefaultSagaOrchestrator.class);
    lenient().when(orchestrator.httpEndpointRegistrar()).thenReturn(services -> {});
    return orchestrator;
  }

  private static SagaServerConfig configWithDefinitionsPath(Path path) throws IOException {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, path.toString());
    // A nonexistent definitions path has no directory to host the services dir; those tests
    // exercise the definitions failure alone, so no services are configured for them.
    Path parent = Files.isDirectory(path) ? path : path.getParent();
    if (parent != null) {
      props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(parent).toString());
    }
    return SagaServerConfig.load(props);
  }

  /** A declarative definition carrying its own explicit saga timeout. */
  private static String declarativeJsonWithTimeout(String name, long timeoutMillis) {
    return "{\"name\":\""
        + name
        + "\",\"mode\":\"SAGA\",\"timeoutMillis\":"
        + timeoutMillis
        + ",\"steps\":[{\"name\":\"s\",\"service\":\"svc\","
        + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
        + "\"compensation\":{\"method\":\"POST\",\"path\":\"/y\"}}]}";
  }

  @Test
  void constructor_definitionWithoutTimeout_registersItUnmodified(@TempDir Path dir)
      throws Exception {
    // Arrange — a definition with no timeout, and a server default of 30s. The default is
    // enforced by the engine at execution (forwarded via applyEngineSettings), NOT baked into the
    // registered definition: the stored form must equal the parsed file, or changing the default
    // turns an unchanged file into a same-version content conflict at the next boot.
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.DEFAULT_SAGA_TIMEOUT_MILLIS_KEY, "30000");
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    // Act
    new SagaServer(SagaServerConfig.load(props), orchestrator);

    // Assert — the registered definition still has no timeout of its own
    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(orchestrator).register(captor.capture());
    assertThat(captor.getValue().getTimeoutMillis()).isZero();
  }

  @Test
  void constructor_definitionWithOwnTimeout_isNotOverriddenByDefault(@TempDir Path dir)
      throws Exception {
    // Arrange — a definition that sets its own timeout, and a different server default
    Files.writeString(dir.resolve("saga.json"), declarativeJsonWithTimeout("saga", 5000));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.DEFAULT_SAGA_TIMEOUT_MILLIS_KEY, "30000");
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    // Act
    new SagaServer(SagaServerConfig.load(props), orchestrator);

    // Assert — the definition's own timeout wins
    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(orchestrator).register(captor.capture());
    assertThat(captor.getValue().getTimeoutMillis()).isEqualTo(5000L);
  }

  @Test
  void constructor_noServerDefault_leavesDefinitionTimeoutUnbounded(@TempDir Path dir)
      throws Exception {
    // Arrange — no server default configured
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    // Act
    new SagaServer(configWithDefinitionsPath(dir), orchestrator);

    // Assert — timeout stays 0 (unbounded)
    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(orchestrator).register(captor.capture());
    assertThat(captor.getValue().getTimeoutMillis()).isZero();
  }

  @Test
  void constructor_definitionsDirectory_registersOnlyDefinitionFiles(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("a.json"), declarativeJson("a"));
    Files.writeString(dir.resolve("b.yaml"), declarativeYaml("b"));
    Files.writeString(dir.resolve("c.yml"), declarativeYaml("c"));
    Files.writeString(dir.resolve("d.txt"), "ignored");
    Files.writeString(dir.resolve("notes.md"), "ignored");
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    new SagaServer(configWithDefinitionsPath(dir), orchestrator);

    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(orchestrator, times(3)).register(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(SagaDefinition::getName)
        .containsExactlyInAnyOrder("a", "b", "c");
  }

  @Test
  void constructor_singleDefinitionFile_registersOnce(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    new SagaServer(configWithDefinitionsPath(dir.resolve("saga.json")), orchestrator);

    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(orchestrator, times(1)).register(captor.capture());
    assertThat(captor.getValue().getName()).isEqualTo("saga");
  }

  @Test
  void constructor_directoryWithDefinitionExtension_isIgnored(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("real.json"), declarativeJson("real"));
    Files.createDirectory(dir.resolve("nested.json")); // a directory that matches the extension
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    new SagaServer(configWithDefinitionsPath(dir), orchestrator);

    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(orchestrator, times(1)).register(captor.capture());
    assertThat(captor.getValue().getName()).isEqualTo("real");
  }

  @Test
  void constructor_noDefinitionsPath_throwsAndClosesOrchestrator() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    assertThatThrownBy(() -> new SagaServer(SagaServerConfig.load(props), orchestrator))
        .isInstanceOf(IllegalStateException.class);
    verify(orchestrator, never()).register(any(SagaDefinition.class));
    verify(orchestrator).close();
  }

  @Test
  void constructor_noDefinitionFiles_throwsAndClosesOrchestrator(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("notes.md"), "ignored"); // no .json/.yaml/.yml definitions
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), orchestrator))
        .isInstanceOf(IllegalStateException.class);
    verify(orchestrator, never()).register(any(SagaDefinition.class));
    verify(orchestrator).close();
  }

  @Test
  void constructor_nonexistentDefinitionsPath_throwsWithoutEchoingValue() {
    // A secret reference pasted onto the definitions key resolves to plaintext that exists nowhere
    // on disk, so the failure must name the key and keep the resolved "path" out of the message.
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    assertThatThrownBy(
            () ->
                new SagaServer(
                    configWithDefinitionsPath(Path.of("s3cr3t-plaintext-not-a-path")),
                    orchestrator))
        // The boot pass aggregates configuration errors, so the failure arrives as one
        // IllegalStateException whose message (and whose cause's identical message) is redacted.
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(SagaServerConfig.DEFINITIONS_PATH_KEY)
        .hasMessageNotContaining("s3cr3t")
        .rootCause()
        .hasMessageNotContaining("s3cr3t");
    verify(orchestrator, never()).register(any(SagaDefinition.class));
    verify(orchestrator).close();
  }

  @Test
  void constructor_unreadableDefinitionsDirectory_throwsWithoutEchoingPath(@TempDir Path dir) {
    // An existing directory whose listing fails: the AccessDeniedException message is the path
    // itself, so the failure must carry the exception class name instead of the cause. Skipped
    // where the permission change does not take, for example when running as root.
    assumeTrue(dir.toFile().setReadable(false) && !Files.isReadable(dir));
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();
    try {
      assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), orchestrator))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(SagaServerConfig.DEFINITIONS_PATH_KEY)
          .hasMessageContaining("AccessDeniedException")
          .hasMessageNotContaining(dir.toString())
          // The aggregating pass carries its cause with the same redacted message.
          .rootCause()
          .hasMessageNotContaining(dir.toString());
      verify(orchestrator).close();
    } finally {
      boolean unused = dir.toFile().setReadable(true); // let the @TempDir cleanup walk the dir
    }
  }

  @Test
  void constructor_codeStepDefinition_throwsAndClosesOrchestrator(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("code.json"), codeStepJson("code"));
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), orchestrator))
        // Aggregated by the boot pass; the message names the offending file.
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("code.json");
    verify(orchestrator, never()).register(any(SagaDefinition.class));
    verify(orchestrator).close();
  }

  @Test
  void constructor_definitionRegistrationFails_closesOrchestratorAndPropagates(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();
    doThrow(new IllegalStateException("bad definition"))
        .when(orchestrator)
        .register(any(SagaDefinition.class));

    assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), orchestrator))
        .isInstanceOf(IllegalStateException.class);
    verify(orchestrator).close();
  }

  @Test
  void start_portUnavailable_closesOrchestratorAndPropagates(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    // Hold an ephemeral port with another server so SagaServer's app.start(...) fails to bind.
    Javalin portHolder = Javalin.create().start("127.0.0.1", 0);
    try {
      Properties props = new Properties();
      props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
      props.setProperty(SagaServerConfig.HTTP_PORT_KEY, Integer.toString(portHolder.port()));
      props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
      props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
      DefaultSagaOrchestrator orchestrator = mockOrchestrator();
      SagaServer server = new SagaServer(SagaServerConfig.load(props), orchestrator);

      assertThatThrownBy(server::start).isInstanceOf(RuntimeException.class);

      verify(orchestrator).startBackgroundTasks();
      verify(orchestrator).close();
    } finally {
      portHolder.stop();
    }
  }

  @Test
  void close_stopsReloadBeforeTheTransportsAndBoundsItsShareOfTheBudget(@TempDir Path dir)
      throws Exception {
    // Two properties in one: reload must stop before anything drains, so a pass cannot swap the
    // endpoint set out from under a saga that is still finishing a step; and its wait must be a
    // small slice of the budget, because the saga drain that follows computes the full budget
    // again and an operator sizes the container grace period from that one number.
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.SHUTDOWN_TIMEOUT_MILLIS_KEY, "30000");
    props.setProperty(SagaServerConfig.RELOAD_INTERVAL_SECONDS_KEY, "30");
    SagaServer server = new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start();

    // Act — a close with nothing in flight
    long startNanos = System.nanoTime();
    server.close();
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert — an idle close returns promptly; it never waits out either budget
    assertThat(elapsedMillis).isLessThan(30_000L);
  }

  @Test
  void close_calledTwice_drainsOrchestratorOnce(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();
    SagaServer server = new SagaServer(configWithDefinitionsPath(dir), orchestrator);

    server.close();
    server.close();

    verify(orchestrator, times(1)).close();
  }

  @Test
  void start_noopProviderOnNonLoopbackHost_throwsWithoutAcknowledgement(@TempDir Path dir)
      throws Exception {
    // Arrange — default noop provider, bound to all interfaces (0.0.0.0), not acknowledged
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "0.0.0.0");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    SagaServer server = new SagaServer(SagaServerConfig.load(props), mockOrchestrator());

    // Act / Assert — refuses to start unauthenticated on a network-reachable interface
    assertThatThrownBy(server::start).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void start_noopOnNonLoopbackHost_startsWhenInsecureAcknowledged(@TempDir Path dir)
      throws Exception {
    // Arrange — the same exposed noop config, but the operator explicitly acknowledges it
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "0.0.0.0");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.INSECURE_MODE_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());

    // Act / Assert — the acknowledgement lets it bind
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      assertThat(server.port()).isGreaterThan(0);
    }
  }

  @Test
  void start_grpcDisabled_bindsOnlyHttp(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // HTTP is bound to an ephemeral port; gRPC is disabled, so grpcPort() reports the -1
      // sentinel.
      assertThat(server.port()).isGreaterThan(0);
      assertThat(server.grpcPort()).isEqualTo(-1);
    }
  }

  @Test
  void start_withExplicitMaxQueuedRequests_bindsWithBoundedQueue(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.HTTP_MAX_QUEUED_REQUESTS_KEY, "16"); // small explicit cap
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // The 4-arg QueuedThreadPool (bounded job queue) boots and binds the HTTP port; a bad queue
      // capacity would otherwise surface here as a startup failure.
      assertThat(server.port()).isGreaterThan(0);
    }
  }

  @Test
  void start_callbackConfigured_registersCallbackRoute(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    // Both callback keys: the config layer requires them together, so this is the only shape in
    // which the callback route exists.
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t");
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://127.0.0.1:8080");
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // The route is registered, so a bad-token request is authenticated-and-rejected (401) rather
      // than missing (404).
      assertThat(postComplete(server.port()).statusCode()).isEqualTo(401);
    }
  }

  @Test
  void start_noCallbackSecret_callbackRouteAbsent(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    // No callback secret configured → no callback route registered.
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      assertThat(postComplete(server.port()).statusCode()).isEqualTo(404);
    }
  }

  @Test
  void start_realProviderConfigured_callbackPathBypassesCallerAuth(@TempDir Path dir)
      throws Exception {
    // Arrange — a real (api-key) provider, so the RBAC before-handler enforces caller auth on gated
    // routes. No callback secret is set, so the callback route itself is not registered; the
    // exemption is by path pattern, so it applies regardless.
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Path keyFile = Files.writeString(dir.resolve("apikey.secret"), "s3cr3t-key");
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "apikey");
    props.setProperty(
        "scalar.db.saga.server.security.apikey.key.svc.secret", "${file:UTF-8:" + keyFile + "}");
    props.setProperty("scalar.db.saga.server.security.apikey.key.svc.roles", "saga:write");
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // A credential-less request to a gated route is rejected by the before-handler — proving the
      // provider actually enforces auth.
      assertThat(getSaga(server.port()).statusCode()).isEqualTo(401);
      // The same credential-less request to the callback path is exempt, so the before-handler lets
      // it through to route resolution: with no callback route registered that is a 404 — crucially
      // not the 401 an unexempt path returns. Before this exemption, a participant's async callback
      // (HMAC-authed, no caller credential) was 401'd before its HMAC check ever ran.
      assertThat(postComplete(server.port()).statusCode()).isEqualTo(404);
    }
  }

  private static HttpResponse<String> getSaga(int port) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/sagas/s1")).build(),
            HttpResponse.BodyHandlers.ofString());
  }

  private static HttpResponse<String> postComplete(int port) throws Exception {
    URI uri =
        URI.create(
            "http://localhost:" + port + "/sagas/s1/steps/step1/complete?token=deadbeef&iat=1");
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build(),
            HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void start_httpDisabled_bindsOnlyGrpc(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.HTTP_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // gRPC is bound to an ephemeral port; HTTP is disabled, so port() reports the -1 sentinel.
      assertThat(server.grpcPort()).isGreaterThan(0);
      assertThat(server.port()).isEqualTo(-1);
    }
  }

  @Test
  void start_grpcEnabled_healthServiceReportsServing(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    try (SagaServer server =
        new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      ManagedChannel channel =
          ManagedChannelBuilder.forAddress("localhost", server.grpcPort()).usePlaintext().build();
      try {
        // The standard grpc.health.v1.Health service is registered and reports the overall server
        // SERVING — what a K8s-native gRPC probe checks.
        HealthCheckResponse response =
            HealthGrpc.newBlockingStub(channel).check(HealthCheckRequest.getDefaultInstance());

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
      } finally {
        channel.shutdownNow();
      }
    }
  }

  private SagaServer serverWithSyncMaxWait(Path dir, long syncMaxWaitMillis) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, Long.toString(syncMaxWaitMillis));
    return new SagaServer(SagaServerConfig.load(props), mockOrchestrator());
  }

  @Test
  void grpcDrainMillis_syncMaxWaitBelowFloor_returnsFloor(@TempDir Path dir) throws Exception {
    // A small ceiling still drains for at least the 30s floor.
    SagaServer server = serverWithSyncMaxWait(dir, 1_000L);

    assertThat(server.grpcDrainMillis()).isEqualTo(30_000L);
  }

  @Test
  void grpcDrainMillis_syncMaxWaitAboveFloor_returnsCeilingPlusSlack(@TempDir Path dir)
      throws Exception {
    // A ceiling that (with slack) exceeds the floor widens the drain window past 30s, so a
    // legitimate bounded-sync call reaches its own wait ceiling before force-cancellation.
    SagaServer server = serverWithSyncMaxWait(dir, 60_000L);

    assertThat(server.grpcDrainMillis()).isEqualTo(65_000L);
  }

  @Test
  void grpcDrainMillis_raisedSyncMaxWait_widensWindow(@TempDir Path dir) throws Exception {
    SagaServer server = serverWithSyncMaxWait(dir, 120_000L);

    assertThat(server.grpcDrainMillis()).isEqualTo(125_000L);
  }

  /**
   * A config whose every engine setting differs from its default, and whose numeric values all
   * differ from one another, so a setter wired to the wrong getter fails rather than coincidentally
   * matching.
   */
  private static SagaServerConfig configWithEveryEngineSetting() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.OWNER_ID_KEY, "saga-daemon-7");
    props.setProperty(SagaServerConfig.SHUTDOWN_MODE_KEY, "WAIT_ALL_SAGAS");
    props.setProperty(SagaServerConfig.SHUTDOWN_TIMEOUT_MILLIS_KEY, "7001");
    props.setProperty(SagaServerConfig.DEFAULT_SAGA_TIMEOUT_MILLIS_KEY, "7006");
    props.setProperty(SagaServerConfig.DETAIL_MAX_TIMELINE_EVENTS_KEY, "7005");
    props.setProperty(SagaServerConfig.SYNC_TIMEOUT_MILLIS_KEY, "7002");
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, "7003");
    props.setProperty(SagaServerConfig.RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY, "7004");
    props.setProperty(SagaServerConfig.RECOVERY_MAX_RECOVERIES_PER_SWEEP_KEY, "251");
    props.setProperty(SagaServerConfig.RETENTION_MAX_PURGES_PER_PASS_KEY, "52");
    return SagaServerConfig.load(props);
  }

  @Test
  void applyEngineSettings_withEveryEngineSettingConfigured_forwardsEachToTheBuilder() {
    // Arrange
    // Nothing on the orchestrator reads these back, so the builder is the only place the
    // forwarding is observable. A dropped setter here leaves the daemon on the engine default
    // while the operator's key parses and validates, which is the gap the config surface exists
    // to close.
    SagaServerConfig config = configWithEveryEngineSetting();
    DefaultSagaOrchestrator.Builder builder =
        mock(DefaultSagaOrchestrator.Builder.class, RETURNS_SELF);

    // Act
    SagaServer.applyEngineSettings(builder, config);

    // Assert
    verify(builder).ownerId("saga-daemon-7");
    verify(builder).shutdownMode(ShutdownMode.WAIT_ALL_SAGAS);
    verify(builder).shutdownTimeoutMillis(7001L);
    verify(builder).defaultSagaTimeoutMillis(7006L);
    verify(builder).maxTimelineEvents(7005);
    verify(builder).recoveryConfig(config.recoveryConfig());
    verify(builder).retentionConfig(config.retentionConfig());
  }

  @Test
  void applyEngineSettings_withCallbackConfigured_wiresTheCallbackUrlProvider() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://daemon:8080");
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");
    SagaServerConfig config = SagaServerConfig.load(props);
    DefaultSagaOrchestrator.Builder builder =
        mock(DefaultSagaOrchestrator.Builder.class, RETURNS_SELF);

    // Act
    SagaServer.applyEngineSettings(builder, config);

    // Assert
    verify(builder).callbackUrlProvider(any(HmacCallbackUrlProvider.class));
  }

  @Test
  void applyEngineSettings_withoutCallbackConfigured_wiresNoCallbackUrlProvider() {
    // Arrange
    // Neither key set, so async completion stays disabled and registering an async definition
    // fails fast in the engine rather than handing out a URL nothing can authenticate.
    DefaultSagaOrchestrator.Builder builder =
        mock(DefaultSagaOrchestrator.Builder.class, RETURNS_SELF);

    // Act
    SagaServer.applyEngineSettings(builder, SagaServerConfig.load(new Properties()));

    // Assert
    verify(builder, never()).callbackUrlProvider(any());
  }

  @Test
  void applyGrpcTransportSettings_withCapsConfigured_forwardsEachToTheBuilder() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.GRPC_MAX_INBOUND_METADATA_BYTES_KEY, "16384");
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "2097152");
    SagaServerConfig config = SagaServerConfig.load(props);
    NettyServerBuilder builder = mock(NettyServerBuilder.class, RETURNS_SELF);

    // Act
    SagaServer.applyGrpcTransportSettings(builder, config, null);

    // Assert
    verify(builder).maxInboundMessageSize(2_097_152);
    verify(builder).maxInboundMetadataSize(16_384);
  }

  @Test
  void applyGrpcTransportSettings_withStorePayloadCapRaised_derivesTheMessageCapFromIt() {
    // Arrange
    // The message cap has no key of its own: it tracks the store's payload cap so no transport can
    // accept an input the store would then reject. Left to gRPC's own 4 MiB default, a 2 MiB
    // message would be accepted here and refused at persist time, blaming the store.
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, "524288");
    NettyServerBuilder builder = mock(NettyServerBuilder.class, RETURNS_SELF);

    // Act
    SagaServer.applyGrpcTransportSettings(builder, SagaServerConfig.load(props), null);

    // Assert
    verify(builder).maxInboundMessageSize(524_288);
  }

  @Test
  void applyGrpcTransportSettings_withCapsUnset_forwardsTheDaemonDefaults() {
    // Arrange
    // Unset must still be pushed down: the daemon's message default (1 MiB) is a quarter of gRPC's
    // own, so skipping the call would quietly quadruple what the transport accepts.
    NettyServerBuilder builder = mock(NettyServerBuilder.class, RETURNS_SELF);

    // Act
    SagaServer.applyGrpcTransportSettings(builder, SagaServerConfig.load(new Properties()), null);

    // Assert
    verify(builder).maxInboundMessageSize(SagaServerConfig.DEFAULT_MAX_EVENT_PAYLOAD_BYTES);
    verify(builder)
        .maxInboundMetadataSize(SagaServerConfig.DEFAULT_GRPC_MAX_INBOUND_METADATA_BYTES);
  }

  @Test
  void applyGrpcTransportSettings_withTlsMaterial_enablesTransportSecurityFromValidatedBytes() {
    // Arrange — real material: the builder must receive the validated bytes re-encoded as PEM,
    // never the file paths, so nothing re-reads the files after validation (a rotation landing
    // mid-boot would otherwise hand gRPC material the validator never saw).
    TlsMaterial material =
        TlsMaterial.load(tls.certChainPath(), tls.privateKeyPath(), Clock.systemUTC());
    NettyServerBuilder builder = mock(NettyServerBuilder.class, RETURNS_SELF);

    // Act
    SagaServer.applyGrpcTransportSettings(
        builder, SagaServerConfig.load(new Properties()), material);

    // Assert
    verify(builder).useTransportSecurity(any(InputStream.class), any(InputStream.class));
  }

  @Test
  void applyGrpcTransportSettings_withoutTlsMaterial_leavesTransportPlaintext() {
    // Arrange
    NettyServerBuilder builder = mock(NettyServerBuilder.class, RETURNS_SELF);

    // Act
    SagaServer.applyGrpcTransportSettings(builder, SagaServerConfig.load(new Properties()), null);

    // Assert
    verify(builder, never()).useTransportSecurity(any(InputStream.class), any(InputStream.class));
  }

  @Test
  void constructor_tlsEnabledWithBadMaterial_throwsAndClosesOrchestrator(@TempDir Path dir)
      throws Exception {
    // Arrange — paths that pass config validation but fail TlsMaterial's file validation
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Path bogusCert = dir.resolve("bogus.crt");
    Path bogusKey = dir.resolve("bogus.key");
    Files.writeString(bogusCert, "not pem\n");
    Files.writeString(bogusKey, "not pem\n");
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, bogusCert.toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, bogusKey.toString());
    DefaultSagaOrchestrator orchestrator = mockOrchestrator();

    // Act / Assert — fails in the constructor, long before any port could bind, and releases the
    // only resource alive at that point
    assertThatThrownBy(() -> new SagaServer(SagaServerConfig.load(props), orchestrator))
        .isInstanceOf(IllegalArgumentException.class);
    verify(orchestrator).close();
  }

  @Test
  void start_tlsEnabledUnderNoopOnNonLoopback_stillRefusesToStart(@TempDir Path dir)
      throws Exception {
    // TLS is confidentiality; the guard is authentication. Encrypting the transport must not
    // relax the refusal to serve unauthenticated on a network-reachable interface.
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "0.0.0.0");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, tls.certChainPath().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, tls.privateKeyPath().toString());
    SagaServer server = new SagaServer(SagaServerConfig.load(props), mockOrchestrator());

    // Act / Assert
    assertThatThrownBy(server::start).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void start_tlsEnabled_logsConfirmationAndReportsEphemeralPorts(@TempDir Path dir)
      throws Exception {
    // Arrange — TLS on both transports, ephemeral ports, loopback host so the noop guard passes
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, tls.certChainPath().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, tls.privateKeyPath().toString());

    // Act / Assert — the custom TLS connector must not break ephemeral-port readback, and the
    // positive INFO confirmation is what operators and the smoke test key on.
    try (LogCapture logs = LogCapture.of(SagaServer.class)) {
      try (SagaServer server =
          new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
        assertThat(server.port()).isGreaterThan(0);
        assertThat(server.grpcPort()).isGreaterThan(0);
      }
      assertThat(logs.events())
          .anySatisfy(
              event ->
                  assertThat(event.getFormattedMessage()).contains("TLS enabled for HTTP and gRPC"))
          // No log line may echo the configured path value — like every configured value, it can
          // be a mis-pasted secret.
          .noneSatisfy(
              event ->
                  assertThat(event.getFormattedMessage()).contains(tls.certChainPath().toString()));
    }
  }

  @Test
  void start_tlsWithPlaintextCallbackBaseUrl_warnsAtStartup(@TempDir Path dir) throws Exception {
    // Arrange — TLS on, callback base URL plain http on a non-loopback host: participants would
    // dial the TLS port over plaintext and fail at handshake on the first async step.
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, tls.certChainPath().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, tls.privateKeyPath().toString());
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://participants.example.com");
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");

    // Act
    try (LogCapture logs = LogCapture.of(SagaServer.class);
        SagaServer server =
            new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // Assert
      assertThat(logs.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage())
                    .contains(SagaServerConfig.CALLBACK_BASE_URL_KEY);
              });
    }
  }

  @Test
  void start_tlsWithHttpsCallbackBaseUrl_doesNotWarn(@TempDir Path dir) throws Exception {
    // Arrange — the well-configured counterpart of the test above
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, tls.certChainPath().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, tls.privateKeyPath().toString());
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "https://participants.example.com");
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");

    // Act
    try (LogCapture logs = LogCapture.of(SagaServer.class);
        SagaServer server =
            new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // Assert
      assertThat(logs.events())
          .noneSatisfy(
              event ->
                  assertThat(event.getFormattedMessage())
                      .contains(SagaServerConfig.CALLBACK_BASE_URL_KEY));
    }
  }

  @Test
  void start_tlsWithUnparseableCallbackBaseUrl_warnsAtStartup(@TempDir Path dir) throws Exception {
    // Arrange — a base URL URI.create rejects (space in the authority). Nothing downstream parses
    // the value, so this startup warning is the only diagnostic the operator gets. The warning
    // must name the key and never echo the value: like every configured value it can be a
    // mis-pasted secret.
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, svcServices(dir).toString());
    props.setProperty(SagaServerConfig.TLS_ENABLED_KEY, "true");
    props.setProperty(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY, tls.certChainPath().toString());
    props.setProperty(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY, tls.privateKeyPath().toString());
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://sv c:8080/s3cr3t-value");
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, "s3cr3t-key");

    // Act
    try (LogCapture logs = LogCapture.of(SagaServer.class);
        SagaServer server =
            new SagaServer(SagaServerConfig.load(props), mockOrchestrator()).start()) {
      // Assert
      assertThat(logs.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage())
                    .contains(SagaServerConfig.CALLBACK_BASE_URL_KEY)
                    .contains("not a parseable URI");
              })
          .noneSatisfy(event -> assertThat(event.getFormattedMessage()).contains("s3cr3t-value"));
    }
  }
}
