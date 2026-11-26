package com.scalar.db.saga.designer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DesignerApi.Download;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The designer's HTTP server: the bundled UI as static files, and the JSON API {@link DesignerApi}
 * implements, behind {@link RequestGuard}.
 */
final class DesignerServer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DesignerServer.class);

  /** Where the UI lives on the classpath. */
  static final String UI_RESOURCES = "/designer-ui";

  // Definitions are small; sample responses pasted into the designer data are the largest part.
  private static final long MAX_REQUEST_BYTES = 4L * 1024 * 1024;

  private static final Pattern IMPORT_MAP =
      Pattern.compile("<script type=\"importmap\">(.*?)</script>", Pattern.DOTALL);

  private final Javalin app;
  private final String host;
  private final int requestedPort;

  DesignerServer(DesignerApi api, RequestGuard guard, String host, int port) {
    this.host = host;
    this.requestedPort = port;
    Map<String, String> securityHeaders = securityHeaders(contentSecurityPolicy());
    this.app =
        Javalin.create(
            cfg -> {
              cfg.startup.showJavalinBanner = false;
              cfg.http.maxRequestSize = MAX_REQUEST_BYTES;
              cfg.staticFiles.add(
                  files -> {
                    files.hostedPath = "/";
                    files.directory = UI_RESOURCES;
                    files.location = Location.CLASSPATH;
                    files.headers = securityHeaders;
                  });
              cfg.routes.before(
                  ctx -> {
                    String refusal =
                        guard.refusal(
                            ctx.method().name(),
                            ctx.header("Host"),
                            ctx.header("Origin"),
                            ctx.header("Content-Type"));
                    if (refusal != null) {
                      throw new BadRequestException(403, refusal);
                    }
                    securityHeaders.forEach(ctx::header);
                  });
              cfg.routes.get("/api/workspace", ctx -> json(ctx, api.workspace()));
              cfg.routes.get(
                  "/api/files/{file}", ctx -> json(ctx, api.open(ctx.pathParam("file"))));
              cfg.routes.put(
                  "/api/files/{file}",
                  ctx -> json(ctx, api.save(ctx.pathParam("file"), body(ctx))));
              cfg.routes.post("/api/import", ctx -> json(ctx, api.importFile(body(ctx))));
              cfg.routes.post("/api/validate", ctx -> json(ctx, api.validate(body(ctx))));
              cfg.routes.post(
                  "/api/service-templates", ctx -> json(ctx, api.serviceTemplates(body(ctx))));
              cfg.routes.post("/api/export", ctx -> download(ctx, api.export(body(ctx))));
              cfg.routes.post("/api/bundle", ctx -> download(ctx, api.bundle(body(ctx))));
              cfg.routes.exception(
                  BadRequestException.class, (e, ctx) -> error(ctx, e.status(), e.reason()));
              cfg.routes.exception(
                  Workspace.ConflictException.class,
                  (e, ctx) -> {
                    ObjectNode body = errorBody(Workspace.ConflictException.MESSAGE);
                    body.put("currentSha256", e.currentSha256());
                    ctx.status(409).contentType("application/json").result(body.toString());
                  });
              cfg.routes.exception(
                  Exception.class,
                  (e, ctx) -> {
                    logger.error("Request {} {} failed", ctx.method(), ctx.path(), e);
                    // The detail, which can hold absolute paths, goes to the log only.
                    error(ctx, 500, "The designer failed; its log has the details");
                  });
            });
  }

  /** Starts listening. */
  void start() {
    app.start(host, requestedPort);
  }

  /** The port the server listens on, which differs from the requested one when that was 0. */
  int port() {
    return app.port();
  }

  @Override
  public void close() {
    app.stop();
  }

  private static JsonNode body(Context ctx) {
    try {
      JsonNode body = DefinitionFormats.json().readTree(ctx.body());
      if (body == null || !body.isObject()) {
        throw new BadRequestException("The request body must be a JSON object");
      }
      return body;
    } catch (JsonProcessingException e) {
      throw new BadRequestException("The request body is not JSON: " + e.getOriginalMessage());
    }
  }

  private static void json(Context ctx, JsonNode body) {
    ctx.contentType("application/json").result(body.toString());
  }

  private static void download(Context ctx, Download download) {
    // The name passed Workspace.isDefinitionFileName, or is built from it, so it holds no quote
    // or line break that could break out of the header.
    ctx.header("Content-Disposition", "attachment; filename=\"" + download.fileName() + "\"");
    ctx.contentType(download.contentType()).result(download.content());
  }

  private static void error(Context ctx, int status, String message) {
    ctx.status(status).contentType("application/json").result(errorBody(message).toString());
  }

  private static ObjectNode errorBody(String message) {
    ObjectNode body = DefinitionFormats.json().createObjectNode();
    body.put("error", message);
    return body;
  }

  private static Map<String, String> securityHeaders(String csp) {
    return Map.of(
        "Content-Security-Policy", csp,
        "X-Content-Type-Options", "nosniff",
        "Referrer-Policy", "no-referrer",
        "Cache-Control", "no-store");
  }

  /** The CSP for the bundled UI's page. */
  static String contentSecurityPolicy() {
    try (InputStream in = DesignerServer.class.getResourceAsStream(UI_RESOURCES + "/index.html")) {
      return contentSecurityPolicy(
          in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read the bundled UI", e);
    }
  }

  /**
   * The CSP for a page. The UI's import map has to be inline (browsers do not load one from a
   * file), so it is allowed by its hash, computed from the page rather than written down, so an
   * edit to the map can never silently break the page.
   */
  static String contentSecurityPolicy(String html) {
    String scripts = "'self'";
    Matcher importMap = IMPORT_MAP.matcher(html);
    if (importMap.find()) {
      scripts += " 'sha256-" + sha256Base64(importMap.group(1)) + "'";
    }
    return "default-src 'self'; script-src "
        + scripts
        + "; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none';"
        + " form-action 'none'; frame-ancestors 'none'";
  }

  private static String sha256Base64(String text) {
    try {
      return Base64.getEncoder()
          .encodeToString(
              MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }
}
