package com.scalar.db.saga.daemon;

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
abstract class DaemonIntegrationTestSupport {

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
    props.setProperty(SagaServerConfig.PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, definitionsDir.toString());
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + SERVICE + SagaServerConfig.SERVICE_BASE_URL_SUFFIX,
        baseUrl);

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

  /** Registers the participant's HTTP endpoints — the services a definition's steps call. */
  protected abstract void configureParticipant(HttpServer participant);

  /** Writes the saga definition files this test needs into {@code definitionsDir}. */
  protected abstract void writeDefinitions(Path definitionsDir) throws IOException;

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
    return send(
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(body))
            .build());
  }

  protected final HttpResponse<String> put(String path, String body) throws Exception {
    return send(
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .PUT(BodyPublishers.ofString(body))
            .build());
  }

  protected final HttpResponse<String> get(String path) throws Exception {
    return send(HttpRequest.newBuilder(uri(path)).GET().build());
  }

  /** Polls {@code GET /sagas/{id}} until the saga leaves a non-terminal state, then returns it. */
  protected final String pollUntilTerminal(String sagaId) throws Exception {
    for (int i = 0; i < 50; i++) {
      String status = MAPPER.readTree(get("/sagas/" + sagaId).body()).get("status").asText();
      if (!status.equals("RUNNING") && !status.equals("COMPENSATING")) {
        return status;
      }
      Thread.sleep(40);
    }
    throw new AssertionError("Saga " + sagaId + " did not reach a terminal status in time");
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
