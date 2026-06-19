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
import java.util.Properties;
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
