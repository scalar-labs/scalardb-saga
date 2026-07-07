package com.scalar.db.saga.daemon.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.daemon.api.ErrorMapper;
import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.EnumSet;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SecurityHandler}'s RBAC enforcement end-to-end against a real Javalin dispatch,
 * with a stub {@link SagaSecurityProvider} keyed off an {@code X-Test-Role} header: absent → not
 * authenticated ({@code 401}); {@code read}/{@code write}/{@code admin} → an identity holding that
 * role. {@link ErrorMapper} renders the thrown auth exceptions, so this also locks in the
 * exception→status wiring (401/403).
 */
class SecurityHandlerTest {

  private final HttpClient http = HttpClient.newHttpClient();
  private Javalin app;

  @BeforeEach
  void setUp() {
    app = Javalin.create();
    SecurityHandler.register(app, new RoleHeaderProvider(), AuthExemptions.of("/exempt"));
    ErrorMapper.register(app);
    // A read-gated route (GET) that echoes the resolved principal, so a test can assert the
    // identity was stored on the request.
    app.get(
        "/read",
        ctx -> {
          SagaIdentity identity = ctx.attribute(SecurityHandler.IDENTITY_ATTRIBUTE);
          ctx.result("read:" + (identity == null ? "none" : identity.principal()));
        });
    // A write-gated route (POST).
    app.post("/write", ctx -> ctx.result("written"));
    // An exempt route reachable with no credential.
    app.get("/exempt", ctx -> ctx.result("open"));
    app.start(0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void exemptPath_noCredential_bypassesAuth() throws Exception {
    // Act — no X-Test-Role header at all
    HttpResponse<String> response = send("GET", "/exempt", null);

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("open");
  }

  @Test
  void gatedRoute_noCredential_returns401() throws Exception {
    // Act
    HttpResponse<String> response = send("GET", "/read", null);

    // Assert
    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.body()).contains("UNAUTHENTICATED");
  }

  @Test
  void readRoute_readRoleGiven_returns200AndStoresIdentity() throws Exception {
    // Act
    HttpResponse<String> response = send("GET", "/read", "read");

    // Assert — reached the handler, which read the identity from the request attribute
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("read:reader");
  }

  @Test
  void writeRoute_readRoleGiven_returns403() throws Exception {
    // Act — a read-only caller hitting a write endpoint
    HttpResponse<String> response = send("POST", "/write", "read");

    // Assert
    assertThat(response.statusCode()).isEqualTo(403);
    assertThat(response.body()).contains("FORBIDDEN");
  }

  @Test
  void writeRoute_writeRoleGiven_returns200() throws Exception {
    // Act
    HttpResponse<String> response = send("POST", "/write", "write");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("written");
  }

  @Test
  void writeRoute_adminRoleGiven_returns200_viaRoleHierarchy() throws Exception {
    // Act — ADMIN implies WRITE
    HttpResponse<String> response = send("POST", "/write", "admin");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void requiredRoleFor_readMethods_returnRead() {
    // Assert
    assertThat(SecurityHandler.requiredRoleFor("GET")).isEqualTo(SagaRole.READ);
    assertThat(SecurityHandler.requiredRoleFor("HEAD")).isEqualTo(SagaRole.READ);
  }

  @Test
  void requiredRoleFor_writeMethods_returnWrite() {
    // Assert
    assertThat(SecurityHandler.requiredRoleFor("POST")).isEqualTo(SagaRole.WRITE);
    assertThat(SecurityHandler.requiredRoleFor("PUT")).isEqualTo(SagaRole.WRITE);
    assertThat(SecurityHandler.requiredRoleFor("DELETE")).isEqualTo(SagaRole.WRITE);
  }

  private HttpResponse<String> send(String method, String path, @Nullable String role)
      throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
            .method(method, HttpRequest.BodyPublishers.noBody());
    if (role != null) {
      builder.header("X-Test-Role", role);
    }
    return http.send(builder.build(), BodyHandlers.ofString());
  }

  /**
   * A stub provider: an absent {@code X-Test-Role} header is unauthenticated; {@code read}/{@code
   * write}/{@code admin} map to an identity holding exactly that role.
   */
  private static final class RoleHeaderProvider implements SagaSecurityProvider {
    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      String role = request.header("X-Test-Role").orElse(null);
      if (role == null) {
        throw new SagaAuthenticationException("missing X-Test-Role");
      }
      return switch (role) {
        case "read" -> SagaIdentity.of("reader", Set.of(SagaRole.READ));
        case "write" -> SagaIdentity.of("writer", Set.of(SagaRole.WRITE));
        case "admin" -> SagaIdentity.of("root", EnumSet.allOf(SagaRole.class));
        default -> throw new SagaAuthenticationException("unknown role: " + role);
      };
    }

    @Override
    public String name() {
      return "role-header-stub";
    }
  }
}
