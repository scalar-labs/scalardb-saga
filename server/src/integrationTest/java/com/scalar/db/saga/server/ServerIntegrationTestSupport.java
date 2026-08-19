package com.scalar.db.saga.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Shared fixture for the daemon integration tests. Stands up a fake HTTP participant, then a {@link
 * SagaServer} (sqlite store, ephemeral port) wired to reach it as service {@value #SERVICE}, and
 * exposes HTTP-client helpers against the server. Subclasses register the participant's endpoints
 * ({@link #configureParticipant}) and write the saga definitions they need ({@link
 * #writeDefinitions}).
 *
 * <p>Both subclasses drive <b>declarative service-step</b> sagas — the daemon's intended step kind
 * (it ships as a container, so operators can't supply code-step classes). {@link
 * SagaRestApiIntegrationTest} asserts on the daemon's REST responses (inbound contract); {@link
 * SagaServiceStepIntegrationTest} asserts on the participant-side calls (outbound transport).
 */
abstract class ServerIntegrationTestSupport {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected static final String SERVICE = "account";

  private final HttpClient http = HttpClient.newHttpClient();
  private final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();

  private HttpServer participant;
  private Path tempDbPath;
  private Path definitionsDir;
  private SagaServer server;

  @BeforeEach
  void startServer() throws IOException {
    participant = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    configureParticipant(participant);
    participant.start();
    String baseUrl = "http://localhost:" + participant.getAddress().getPort();

    tempDbPath = Files.createTempFile("saga-daemon-it-", ".db");
    definitionsDir = Files.createTempDirectory("saga-daemon-it-defs-");
    writeDefinitions(definitionsDir);

    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, definitionsDir.toString());
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + SERVICE + SagaServerConfig.SERVICE_BASE_URL_SUFFIX,
        baseUrl);
    configureProperties(props);

    server = new SagaServer(SagaServerConfig.load(props)).start();
  }

  @AfterEach
  void stopServer() throws IOException {
    if (server != null) {
      server.close();
    }
    if (participant != null) {
      participant.stop(0);
    }
    deleteRecursively(definitionsDir);
    if (tempDbPath != null) {
      Files.deleteIfExists(tempDbPath);
    }
  }

  /** The actual bound gRPC port (ephemeral). Only meaningful after the server has started. */
  protected final int grpcPort() {
    return server.grpcPort();
  }

  /** The actual bound HTTP port (ephemeral). Only meaningful after the server has started. */
  protected final int httpPort() {
    return server.port();
  }

  /** Registers the participant's HTTP endpoints — the services a definition's steps call. */
  protected abstract void configureParticipant(HttpServer participant);

  /** Writes the saga definition files this test needs into {@code definitionsDir}. */
  protected abstract void writeDefinitions(Path definitionsDir) throws IOException;

  /**
   * Hook for a subclass to add or override server properties before the server starts (no-op by
   * default).
   */
  protected void configureProperties(Properties props) {}

  // --- optional apikey security wiring (shared by the admin integration tests) ----------------

  /** The header the {@code apikey} provider reads the credential from, once enabled. */
  protected static final String API_KEY_HEADER = "X-API-Key";

  /** The {@code saga:admin} key value {@link #enableApiKeyProvider} configures. */
  protected static final String ADMIN_KEY = "admin-key-secret-value";

  /** The {@code saga:write}-only key value {@link #enableApiKeyProvider} configures. */
  protected static final String WRITE_KEY = "write-key-secret-value";

  private static final String APIKEY_PREFIX = "scalar.db.saga.server.security.apikey.";

  /**
   * Turns on real authentication for a subclass's server: the {@code apikey} provider with a {@code
   * saga:admin} key ({@link #ADMIN_KEY}) and a {@code saga:write}-only key ({@link #WRITE_KEY}),
   * both presented in the {@value #API_KEY_HEADER} header. Call from {@link #configureProperties}
   * to run a test through the daemon's real RBAC wiring.
   */
  protected final void enableApiKeyProvider(Properties props) {
    props.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "apikey");
    props.setProperty(APIKEY_PREFIX + "header", API_KEY_HEADER);
    configureApiKey(props, "admin", ADMIN_KEY, "saga:admin");
    configureApiKey(props, "writer", WRITE_KEY, "saga:write");
  }

  private static void configureApiKey(Properties props, String name, String secret, String roles) {
    // Each key's secret must be a secret reference, so write it to a temp file and reference it.
    props.setProperty(APIKEY_PREFIX + "key." + name + ".secret", fileSecretReference(secret));
    props.setProperty(APIKEY_PREFIX + "key." + name + ".roles", roles);
  }

  private static String fileSecretReference(String secret) {
    try {
      Path file = Files.createTempFile("saga-admin-it-key", ".secret");
      file.toFile().deleteOnExit();
      Files.write(file, secret.getBytes(StandardCharsets.UTF_8));
      return "${file:UTF-8:" + file + "}";
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected final void writeDefinition(Path definitionsDir, String name, String json)
      throws IOException {
    Files.writeString(definitionsDir.resolve(name + ".json"), json);
  }

  /**
   * Substitutes {@code $svc} with {@link #SERVICE} so definitions can be written as readable JSON
   * text blocks (e.g. {@code "service": "$svc"}) instead of string concatenation.
   */
  protected static String withService(String json) {
    return json.replace("$svc", SERVICE);
  }

  /**
   * Registers a participant endpoint that counts calls and answers {@code status} with {@code {}}.
   */
  protected final void route(HttpServer participant, String path, int status) {
    route(participant, path, status, "{}");
  }

  /**
   * Registers a participant endpoint that counts calls and answers {@code status} with {@code
   * body}.
   */
  protected final void route(HttpServer participant, String path, int status, String body) {
    hits.put(path, new AtomicInteger());
    participant.createContext(
        path,
        ex -> {
          hits.get(path).incrementAndGet();
          respond(ex, status, body);
        });
  }

  /**
   * Registers a participant endpoint that counts calls and answers {@code failStatus} for its first
   * {@code failTimes} calls, then {@code okStatus} (a transient failure a retry policy rides out).
   */
  protected final void routeFlaky(
      HttpServer participant, String path, int failTimes, int failStatus, int okStatus) {
    AtomicInteger counter = new AtomicInteger();
    hits.put(path, counter);
    participant.createContext(
        path,
        ex -> respond(ex, counter.incrementAndGet() <= failTimes ? failStatus : okStatus, "{}"));
  }

  /** The number of times the participant endpoint {@code path} has been called. */
  protected final int hits(String path) {
    AtomicInteger counter = hits.get(path);
    return counter == null ? 0 : counter.get();
  }

  /**
   * Waits (up to ~2s) until the participant endpoint {@code path} has been called at least once.
   */
  protected final void awaitHit(String path) throws InterruptedException {
    for (int i = 0; i < 50; i++) {
      if (hits(path) >= 1) {
        return;
      }
      Thread.sleep(40);
    }
    throw new AssertionError("expected the participant to be called on " + path);
  }

  /** The {@code status} field of a saga REST response body. */
  protected final String status(HttpResponse<String> response) throws IOException {
    return MAPPER.readTree(response.body()).get("status").asText();
  }

  protected final HttpResponse<String> post(String path, String body) throws Exception {
    return post(path, body, Map.of());
  }

  /** POSTs {@code body} as JSON with the given extra request headers (e.g. an auth header). */
  protected final HttpResponse<String> post(String path, String body, Map<String, String> headers)
      throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(body));
    headers.forEach(request::header);
    return send(request.build());
  }

  protected final HttpResponse<String> put(String path, String body) throws Exception {
    return send(
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .PUT(BodyPublishers.ofString(body))
            .build());
  }

  protected final HttpResponse<String> get(String path) throws Exception {
    return get(path, Map.of());
  }

  /** GETs {@code path} with the given extra request headers (e.g. an auth header). */
  protected final HttpResponse<String> get(String path, Map<String, String> headers)
      throws Exception {
    HttpRequest.Builder request = HttpRequest.newBuilder(uri(path)).GET();
    headers.forEach(request::header);
    return send(request.build());
  }

  /** Polls {@code GET /sagas/{id}} until the saga leaves a non-terminal state, then returns it. */
  protected final String pollUntilTerminal(String sagaId) throws Exception {
    String lastBody = null;
    for (int i = 0; i < 50; i++) {
      HttpResponse<String> response = get("/sagas/" + sagaId);
      lastBody = response.body();
      // A poll can transiently get an error body instead of a snapshot — e.g. a retryable 503
      // while the asynchronous post-callback drive holds SQLite's single writer. An error body
      // has no "status" field, so only a 200 is read; anything else means poll again.
      if (response.statusCode() == 200) {
        String status = MAPPER.readTree(response.body()).get("status").asText();
        if (!status.equals("RUNNING") && !status.equals("COMPENSATING")) {
          return status;
        }
      }
      Thread.sleep(40);
    }
    throw new AssertionError(
        "Saga " + sagaId + " did not reach a terminal status in time; last response: " + lastBody);
  }

  private HttpResponse<String> send(HttpRequest request) throws Exception {
    return http.send(request, BodyHandlers.ofString());
  }

  private URI uri(String path) {
    return URI.create("http://localhost:" + server.port() + path);
  }

  protected static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static void deleteRecursively(Path dir) throws IOException {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(dir)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    }
  }
}
