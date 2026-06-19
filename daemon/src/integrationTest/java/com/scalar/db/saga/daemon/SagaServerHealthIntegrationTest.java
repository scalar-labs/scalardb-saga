package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SagaServerHealthIntegrationTest {

  private Path tempDbPath;
  private SagaServer server;

  @BeforeEach
  void setUp() throws Exception {
    tempDbPath = Files.createTempFile("saga-daemon-test-", ".db");

    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    props.setProperty(SagaServerConfig.PORT_KEY, "0"); // ephemeral port

    server = new SagaServer(SagaServerConfig.load(props)).start();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    Files.deleteIfExists(tempDbPath);
  }

  @Test
  void health_serverRunning_returnsUp() throws Exception {
    // Arrange
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/health")).build();

    // Act
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("UP");
  }
}
