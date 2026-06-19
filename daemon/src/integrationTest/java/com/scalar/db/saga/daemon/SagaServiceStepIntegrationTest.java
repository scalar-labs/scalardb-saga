package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the daemon's <b>outbound service-step transport</b> — a declarative {@code
 * service}-step definition is loaded at startup, its service is resolved to an HTTP endpoint via
 * {@code scalar.db.saga.server.service.<name>.base_url}, and starting the saga over REST drives a
 * real HTTP call to a fake participant (a {@link HttpServer}). Asserts that the participant is
 * actually hit on execution and, on a downstream failure, hit again on compensation — i.e. that the
 * daemon→participant wiring works. Runs through the production {@code new SagaServer(config)} path,
 * not an injected manager.
 *
 * <p>Counterpart: {@link SagaRestApiIntegrationTest} covers the <b>inbound</b> side — the REST
 * status-code/validation/async contract over inert code steps that make no outbound call. This
 * class is purely about the outbound HTTP transport and uses the simplest sagas that exercise it.
 */
class SagaServiceStepIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SERVICE = "account";

  // Single declarative SAGA step that calls the participant and completes.
  private static final String COMPLETING_SAGA = "declarative-saga";
  private static final String COMPLETING_DEF =
      "{\"name\":\""
          + COMPLETING_SAGA
          + "\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"debit\",\"service\":\""
          + SERVICE
          + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/debit\","
          + "\"output\":{\"debitId\":\"$.debit_id\"}},"
          + "\"compensation\":{\"method\":\"POST\",\"path\":\"/reverse\","
          + "\"jsonBody\":{\"id\":\"${debitId}\"}}}]}";

  // s1 (debit) succeeds, s2 (charge) returns 422 → backward recovery compensates s1 via /reverse.
  private static final String COMPENSATING_SAGA = "declarative-compensating-saga";
  private static final String COMPENSATING_DEF =
      "{\"name\":\""
          + COMPENSATING_SAGA
          + "\",\"mode\":\"SAGA\",\"recoveryStrategy\":\"BACKWARD\","
          + "\"defaultRetryPolicy\":{\"maxAttempts\":1,\"initialIntervalMillis\":1},"
          + "\"steps\":[{\"name\":\"debit\",\"service\":\""
          + SERVICE
          + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/debit\","
          + "\"output\":{\"debitId\":\"$.debit_id\"}},"
          + "\"compensation\":{\"method\":\"POST\",\"path\":\"/reverse\","
          + "\"jsonBody\":{\"id\":\"${debitId}\"}}},"
          + "{\"name\":\"charge\",\"service\":\""
          + SERVICE
          + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/charge\"},"
          + "\"compensation\":{\"method\":\"POST\",\"path\":\"/void\"}}]}";

  private final HttpClient http = HttpClient.newHttpClient();
  private final AtomicInteger debitHits = new AtomicInteger();
  private final AtomicInteger reverseHits = new AtomicInteger();

  private HttpServer participant;
  private Path tempDbPath;
  private Path definitionsDir;
  private SagaServer server;

  @BeforeEach
  void setUp() throws Exception {
    participant = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    participant.createContext(
        "/debit",
        ex -> {
          debitHits.incrementAndGet();
          respond(ex, 200, "{\"debit_id\":\"DBT-1\"}");
        });
    participant.createContext(
        "/reverse",
        ex -> {
          reverseHits.incrementAndGet();
          respond(ex, 200, "{}");
        });
    participant.createContext("/charge", ex -> respond(ex, 422, "{}"));
    participant.start();
    String baseUrl = "http://localhost:" + participant.getAddress().getPort();

    tempDbPath = Files.createTempFile("saga-daemon-decl-test-", ".db");
    definitionsDir = Files.createTempDirectory("saga-daemon-decl-defs-");
    Files.writeString(definitionsDir.resolve(COMPLETING_SAGA + ".json"), COMPLETING_DEF);
    Files.writeString(definitionsDir.resolve(COMPENSATING_SAGA + ".json"), COMPENSATING_DEF);

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
  void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    if (participant != null) {
      participant.stop(0);
    }
    Files.deleteIfExists(definitionsDir.resolve(COMPLETING_SAGA + ".json"));
    Files.deleteIfExists(definitionsDir.resolve(COMPENSATING_SAGA + ".json"));
    Files.deleteIfExists(definitionsDir);
    Files.deleteIfExists(tempDbPath);
  }

  @Test
  void startSync_declarativeServiceStep_callsParticipantAndCompletes() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPLETING_SAGA + "\"}");

    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(post.body()).get("status").asText()).isEqualTo("COMPLETED");
    assertThat(debitHits.get()).isEqualTo(1);
  }

  @Test
  void startSync_declarativeBusinessFailure_compensatesViaParticipant() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPENSATING_SAGA + "\"}");

    // s2 (charge) returned 422 → s1 (debit) compensated via /reverse → cleanly rolled back.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(post.body()).get("status").asText()).isEqualTo("COMPENSATED");
    assertThat(debitHits.get()).isEqualTo(1);
    assertThat(reverseHits.get()).isEqualTo(1);
  }

  private HttpResponse<String> post(String path, String body) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(body))
            .build(),
        BodyHandlers.ofString());
  }

  private static void respond(HttpExchange exchange, int status, String body)
      throws java.io.IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
