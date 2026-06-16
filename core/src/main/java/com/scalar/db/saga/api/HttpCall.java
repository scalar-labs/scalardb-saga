package com.scalar.db.saga.api;

import java.util.Map;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * An HTTP {@link CallSpec} (Layer 2b): a verb, a URL path (templated), optional query parameters
 * (templated), a request body (either a flat field map sent as JSON, or a raw templated string with
 * an explicit content type), and an output-extraction mapping.
 *
 * <p>At runtime the {@code ${key}} expressions in {@link #getPath()}, {@link #getQuery()}, {@link
 * #getJsonBody()}, and {@link #getStringBody()} are resolved from the saga context.
 *
 * <p><b>Request body.</b> A body-carrying verb uses <em>either</em> {@link #getJsonBody()} (a flat
 * field map serialized as a JSON object, content type {@code application/json} unless overridden by
 * {@link #getContentType()}) <em>or</em> {@link #getStringBody()} (a raw string template sent
 * verbatim with {@link #getContentType()}, defaulting to {@code application/json}). The two are
 * mutually exclusive — declaring both is a build-time error.
 *
 * <p><b>Output capture.</b> {@link #getOutput()} maps a context key to an extraction expression:
 * {@code $.path} navigates the JSON response object (e.g. {@code $.debit_id}); the special token
 * {@code $body} captures the entire raw response body as a {@code String} (decoded with the
 * response's charset, UTF-8 by default), which works for non-JSON responses too.
 *
 * <p>The framework sends the saga correlation headers and enforces the outbound HTTP policy (SSRF
 * allowlist + body limits).
 *
 * <p>Build with {@link #newBuilder(String)} (the path is required). The verb defaults to {@link
 * HttpMethod#POST}; {@link HttpMethod#GET} and {@link HttpMethod#DELETE} must not declare a request
 * body (neither {@link #getJsonBody()} nor {@link #getStringBody()}).
 */
@Immutable
public final class HttpCall extends CallSpec {

  /** The output token capturing the entire raw response body as a {@code String}. */
  public static final String BODY_OUTPUT = "$body";

  private final HttpMethod method;
  private final String path;
  private final Map<String, String> query;
  private final Map<String, String> jsonBody;
  private final @Nullable String stringBody;
  private final @Nullable String contentType;
  private final Map<String, String> output;

  private HttpCall(Builder builder) {
    this.method = builder.method;
    this.path = builder.path;
    this.query = Map.copyOf(builder.query);
    this.jsonBody = Map.copyOf(builder.jsonBody);
    this.stringBody = builder.stringBody;
    this.contentType = builder.contentType;
    this.output = Map.copyOf(builder.output);
  }

  /** Creates a builder for a call to {@code path} (resolved against the service's base URL). */
  public static Builder newBuilder(String path) {
    Objects.requireNonNull(path, "path must not be null");
    if (path.isBlank()) {
      throw new IllegalArgumentException("path must not be blank");
    }
    return new Builder(path);
  }

  @Override
  public Transport transport() {
    return Transport.HTTP;
  }

  /** The HTTP verb. */
  public HttpMethod getMethod() {
    return method;
  }

  /** The URL path template (e.g. {@code "/accounts/${accountId}/debit"}). */
  public String getPath() {
    return path;
  }

  /**
   * The query-parameter templates (name → {@code ${key}}-or-literal). Unmodifiable; may be empty.
   */
  public Map<String, String> getQuery() {
    return query;
  }

  /**
   * The request-body field templates (field → {@code ${key}}-or-literal), sent as a JSON object for
   * body-carrying verbs. Unmodifiable; always empty for {@link HttpMethod#GET}/{@link
   * HttpMethod#DELETE}.
   */
  public Map<String, String> getJsonBody() {
    return jsonBody;
  }

  /**
   * The raw request-body template (a literal string with {@code ${key}} substitutions), or {@code
   * null} when the body is taken from {@link #getJsonBody()} (or there is no body). Sent verbatim
   * with {@link #getContentType()}. Mutually exclusive with a non-empty {@link #getJsonBody()}.
   */
  public @Nullable String getStringBody() {
    return stringBody;
  }

  /**
   * The explicit request {@code Content-Type}, or {@code null} to use the implicit default ({@code
   * application/json} for the flat-map {@link #getJsonBody()} body and for a string {@link
   * #getStringBody()}). When set it overrides that default.
   */
  public @Nullable String getContentType() {
    return contentType;
  }

  /**
   * The output extraction mapping (context key → extraction expression). Each value is either a
   * {@code $.path} JSON-navigation expression or the {@link #BODY_OUTPUT} token (raw response body
   * as a {@code String}). Unmodifiable; may be empty.
   */
  public Map<String, String> getOutput() {
    return output;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof HttpCall that)) return false;
    return method == that.method
        && path.equals(that.path)
        && query.equals(that.query)
        && jsonBody.equals(that.jsonBody)
        && Objects.equals(stringBody, that.stringBody)
        && Objects.equals(contentType, that.contentType)
        && output.equals(that.output);
  }

  @Override
  public int hashCode() {
    return Objects.hash(method, path, query, jsonBody, stringBody, contentType, output);
  }

  @Override
  public String toString() {
    return "HttpCall{method="
        + method
        + ", path='"
        + path
        + "', query="
        + query
        + ", jsonBody="
        + jsonBody
        + ", stringBody="
        + stringBody
        + ", contentType="
        + contentType
        + ", output="
        + output
        + '}';
  }

  /** Builder for {@link HttpCall}. */
  public static final class Builder {

    private final String path;
    private HttpMethod method = HttpMethod.POST;
    private Map<String, String> query = Map.of();
    private Map<String, String> jsonBody = Map.of();
    private @Nullable String stringBody;
    private @Nullable String contentType;
    private Map<String, String> output = Map.of();

    private Builder(String path) {
      this.path = path;
    }

    /** Sets the HTTP verb. Defaults to {@link HttpMethod#POST}. */
    public Builder method(HttpMethod method) {
      this.method = Objects.requireNonNull(method, "method must not be null");
      return this;
    }

    /** Sets the query-parameter templates. Defensively copied. */
    public Builder query(Map<String, String> query) {
      this.query = Map.copyOf(query);
      return this;
    }

    /** Sets the request-body field templates. Defensively copied. */
    public Builder jsonBody(Map<String, String> jsonBody) {
      this.jsonBody = Map.copyOf(jsonBody);
      return this;
    }

    /**
     * Sets a raw request-body template (a literal string with {@code ${key}} substitutions), sent
     * verbatim with {@link #contentType(String)} (defaulting to {@code application/json}). Mutually
     * exclusive with {@link #jsonBody(Map)}.
     */
    public Builder stringBody(String stringBody) {
      this.stringBody = Objects.requireNonNull(stringBody, "stringBody must not be null");
      return this;
    }

    /**
     * Overrides the request {@code Content-Type} (otherwise the implicit {@code application/json}
     * for a flat-map {@link #jsonBody(Map)} or string {@link #stringBody(String)}).
     */
    public Builder contentType(String contentType) {
      this.contentType = Objects.requireNonNull(contentType, "contentType must not be null");
      return this;
    }

    /** Sets the output extraction mapping. Defensively copied. */
    public Builder output(Map<String, String> output) {
      this.output = Map.copyOf(output);
      return this;
    }

    /**
     * Builds the {@link HttpCall}.
     *
     * @throws IllegalStateException if both a flat-map {@link #jsonBody(Map)} body and a string
     *     {@link #stringBody(String)} are set, or if a body-less verb ({@link
     *     HttpMethod#GET}/{@link HttpMethod#DELETE}) declares any request body (a {@link
     *     #jsonBody(Map)} map or a {@link #stringBody(String)} string)
     */
    public HttpCall build() {
      if (!jsonBody.isEmpty() && stringBody != null) {
        throw new IllegalStateException(
            "a flat-map request body and a raw string body are mutually exclusive; set only one");
      }
      if (!method.hasBody() && (!jsonBody.isEmpty() || stringBody != null)) {
        throw new IllegalStateException(
            method + " must not declare a request body; put parameters in the path or query");
      }
      return new HttpCall(this);
    }
  }
}
