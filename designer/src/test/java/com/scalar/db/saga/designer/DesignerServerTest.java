package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DesignerServerTest {

  private static final String VALID =
      """
      {"name": "order", "version": "1", "steps": [{"name": "a", "service": "s",
        "execution": {"path": "/a"}, "compensation": {"path": "/u"}}]}
      """;

  @TempDir Path temp;
  private Path dir;
  private DesignerServer server;
  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void setUp() throws Exception {
    // The workspace is a subfolder, so a test can put a file just outside it.
    dir = Files.createDirectory(temp.resolve("workspace"));
    server =
        new DesignerServer(
            new DesignerApi(new Workspace(dir), null),
            new RequestGuard("127.0.0.1"),
            "127.0.0.1",
            0);
    server.start();
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  private URI uri(String path) {
    return URI.create("http://127.0.0.1:" + server.port() + path);
  }

  private HttpResponse<String> get(String path) throws Exception {
    return client.send(HttpRequest.newBuilder(uri(path)).GET().build(), BodyHandlers.ofString());
  }

  private HttpResponse<String> send(String method, String path, String body) throws Exception {
    return client.send(
        HttpRequest.newBuilder(uri(path))
            .method(method, BodyPublishers.ofString(body))
            .header("Content-Type", "application/json")
            .header("Origin", "http://127.0.0.1:" + server.port())
            .build(),
        BodyHandlers.ofString());
  }

  private static JsonNode json(String text) throws Exception {
    return DefinitionFormats.read(text, Format.JSON);
  }

  @Test
  void get_root_servesThePageWithSecurityHeaders() throws Exception {
    // Act
    HttpResponse<String> response = get("/");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("Saga Designer");
    assertThat(response.headers().firstValue("Content-Security-Policy"))
        .hasValueSatisfying(csp -> assertThat(csp).contains("default-src 'self'"));
    assertThat(response.headers().firstValue("X-Content-Type-Options")).hasValue("nosniff");
  }

  @Test
  void get_workspace_returnsJson() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);

    // Act
    HttpResponse<String> response = get("/api/workspace");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type"))
        .hasValueSatisfying(type -> assertThat(type).startsWith("application/json"));
    assertThat(json(response.body()).get("files").get(0).get("name").asText()).isEqualTo("order");
    assertThat(response.headers().firstValue("Cache-Control")).hasValue("no-store");
  }

  @Test
  void get_foreignHost_isRefusedForPagesAndApiAlike() throws Exception {
    // Act
    HttpResponse<String> page =
        client.send(
            HttpRequest.newBuilder(uri("/")).header("Host", "evil.example").GET().build(),
            BodyHandlers.ofString());
    HttpResponse<String> api =
        client.send(
            HttpRequest.newBuilder(uri("/api/workspace"))
                .header("Host", "evil.example")
                .GET()
                .build(),
            BodyHandlers.ofString());

    // Assert
    assertThat(page.statusCode()).isEqualTo(403);
    assertThat(api.statusCode()).isEqualTo(403);
    assertThat(json(api.body()).get("error").asText()).isEqualTo("Unexpected Host header");
  }

  @Test
  void post_plainTextBody_isRefused() throws Exception {
    // Act
    HttpResponse<String> response =
        client.send(
            HttpRequest.newBuilder(uri("/api/validate"))
                .POST(BodyPublishers.ofString("{}"))
                .header("Content-Type", "text/plain")
                .build(),
            BodyHandlers.ofString());

    // Assert
    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void post_fromAnotherOrigin_isRefused() throws Exception {
    // Act
    HttpResponse<String> response =
        client.send(
            HttpRequest.newBuilder(uri("/api/validate"))
                .POST(BodyPublishers.ofString("{}"))
                .header("Content-Type", "application/json")
                .header("Origin", "http://evil.example")
                .build(),
            BodyHandlers.ofString());

    // Assert
    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void post_validate_returnsProblems() throws Exception {
    // Arrange
    ObjectNode request = DefinitionFormats.json().createObjectNode();
    request.set("model", EditorModel.fromDefinition(json(VALID)));

    // Act
    HttpResponse<String> response = send("POST", "/api/validate", request.toString());

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(json(response.body()).get("problems")).isEmpty();
  }

  @Test
  void post_bodyThatIsNotJson_isABadRequest() throws Exception {
    // Act
    HttpResponse<String> response = send("POST", "/api/validate", "{oops");

    // Assert
    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(json(response.body()).has("error")).isTrue();
  }

  @Test
  void post_bodyOverTheLimit_isRefused() throws Exception {
    // Act
    HttpResponse<String> response =
        send("POST", "/api/validate", "{\"x\": \"" + "a".repeat(5 * 1024 * 1024) + "\"}");

    // Assert
    assertThat(response.statusCode()).isEqualTo(413);
  }

  @Test
  void get_missingFile_isNotFoundWithAJsonError() throws Exception {
    // Act
    HttpResponse<String> response = get("/api/files/a.json");

    // Assert
    assertThat(response.statusCode()).isEqualTo(404);
    assertThat(json(response.body()).get("error").asText()).contains("a.json");
  }

  @Test
  void get_traversalInTheFileName_isNotServed() throws Exception {
    // Arrange
    Files.writeString(temp.resolve("outside.json"), VALID);

    // Act
    HttpResponse<String> response = get("/api/files/..%2Foutside.json");

    // Assert
    assertThat(response.statusCode()).isIn(400, 404);
  }

  @Test
  void put_fileChangedOnDisk_isAConflictWithTheCurrentSha() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    model.put("version", "2");
    ObjectNode request = DefinitionFormats.json().createObjectNode();
    request.set("model", model);
    request.put("baseSha256", "stale");

    // Act
    HttpResponse<String> response = send("PUT", "/api/files/a.json", request.toString());

    // Assert
    assertThat(response.statusCode()).isEqualTo(409);
    assertThat(json(response.body()).get("currentSha256").asText())
        .isEqualTo(Workspace.sha256(VALID));
  }

  @Test
  void post_export_isOfferedAsAnAttachment() throws Exception {
    // Arrange
    ObjectNode request = DefinitionFormats.json().createObjectNode();
    request.set("model", EditorModel.fromDefinition(json(VALID)));

    // Act
    HttpResponse<String> response = send("POST", "/api/export", request.toString());

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Disposition"))
        .hasValue("attachment; filename=\"order.json\"");
    assertThat(json(response.body()).get("name").asText()).isEqualTo("order");
  }

  @Test
  void post_bundle_isAZip() throws Exception {
    // Arrange
    ObjectNode request = DefinitionFormats.json().createObjectNode();
    request.set("model", EditorModel.fromDefinition(json(VALID)));

    // Act
    HttpResponse<byte[]> response =
        client.send(
            HttpRequest.newBuilder(uri("/api/bundle"))
                .POST(BodyPublishers.ofString(request.toString()))
                .header("Content-Type", "application/json")
                .build(),
            BodyHandlers.ofByteArray());

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type")).hasValue("application/zip");
    assertThat(response.body()).startsWith((byte) 'P', (byte) 'K');
  }

  @Test
  void get_unexpectedFailure_isAServerErrorThatHidesTheDetail() throws Exception {
    // Arrange
    Files.delete(dir);

    // Act
    HttpResponse<String> response = get("/api/workspace");

    // Assert
    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(json(response.body()).get("error").asText())
        .isEqualTo("The designer failed; its log has the details")
        .doesNotContain(temp.toString());
  }

  @Test
  void contentSecurityPolicy_pageWithAnImportMapGiven_allowsExactlyThatMapByHash() {
    // Arrange
    String html = "<script type=\"importmap\">{\"imports\": {}}</script>";

    // Act
    String csp = DesignerServer.contentSecurityPolicy(html);

    // Assert
    // sha256 of {"imports": {}} in base64
    assertThat(csp)
        .contains("script-src 'self' 'sha256-CJt/FpW3RQlGvqBeO/YK74xrTm1ctS6rLMJu1bV4fMc='");
  }

  @Test
  void contentSecurityPolicy_pageWithoutAnImportMapGiven_allowsOnlyOwnScripts() {
    // Act
    String csp = DesignerServer.contentSecurityPolicy("<p>no scripts</p>");

    // Assert
    assertThat(csp).contains("script-src 'self';").contains("frame-ancestors 'none'");
  }
}
