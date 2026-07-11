package com.scalar.db.saga.definition;

import com.scalar.db.saga.api.HttpMethod;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  /** Placeholder syntax for context-value interpolation; shared with the transport resolver. */
  public static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]*)\\}");

  private final HttpMethod method;
  private final String path;
  private final Map<String, String> query;
  private final Map<String, String> jsonBody;
  private final @Nullable String stringBody;
  private final @Nullable String contentType;
  private final Map<String, String> output;
  private final boolean async;
  private final long callbackTimeoutMillis;

  private HttpCall(Builder builder) {
    this.method = builder.method;
    this.path = builder.path;
    this.query = Map.copyOf(builder.query);
    this.jsonBody = Map.copyOf(builder.jsonBody);
    this.stringBody = builder.stringBody;
    this.contentType = builder.contentType;
    this.output = Map.copyOf(builder.output);
    this.async = builder.async;
    this.callbackTimeoutMillis = builder.callbackTimeoutMillis;
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
  public boolean isAsync() {
    return async;
  }

  @Override
  public long callbackTimeoutMillis() {
    return callbackTimeoutMillis;
  }

  /**
   * The {@code ${key}} keys referenced across the path, query values, JSON-body values, and string
   * body. Keys are matched exactly as the runtime resolver reads them (no trimming); empty {@code
   * ${}} placeholders are ignored.
   */
  @Override
  public Set<String> referencedContextKeys() {
    Set<String> keys = new HashSet<>();
    collectKeys(path, keys);
    query.values().forEach(value -> collectKeys(value, keys));
    jsonBody.values().forEach(value -> collectKeys(value, keys));
    collectKeys(stringBody, keys);
    return keys;
  }

  private static void collectKeys(@Nullable String template, Set<String> into) {
    if (template == null) {
      return;
    }
    Matcher matcher = PLACEHOLDER.matcher(template);
    while (matcher.find()) {
      String key = matcher.group(1);
      if (!key.isEmpty()) {
        into.add(key);
      }
    }
  }

  /** The context keys bound from the response, i.e. the {@link #getOutput()} keys. */
  @Override
  public Set<String> producedContextKeys() {
    return output.keySet();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof HttpCall that)) return false;
    return method == that.method
        && async == that.async
        && callbackTimeoutMillis == that.callbackTimeoutMillis
        && path.equals(that.path)
        && query.equals(that.query)
        && jsonBody.equals(that.jsonBody)
        && Objects.equals(stringBody, that.stringBody)
        && Objects.equals(contentType, that.contentType)
        && output.equals(that.output);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        method,
        path,
        query,
        jsonBody,
        stringBody,
        contentType,
        output,
        async,
        callbackTimeoutMillis);
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
        + ", async="
        + async
        + ", callbackTimeoutMillis="
        + callbackTimeoutMillis
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
    private boolean async = false;
    private long callbackTimeoutMillis = 0;

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
     * Marks this call async-capable: the participant may respond {@code 202 Accepted} and complete
     * the step later via an external callback (daemon mode), rather than returning the result in
     * the response. Defaults to {@code false}. Valid only on a forward phase — a {@code
     * ServiceStep} rejects an async {@code compensation}/{@code cancellation} call.
     */
    public Builder async(boolean async) {
      this.async = async;
      return this;
    }

    /**
     * Sets the callback-wait deadline in milliseconds for an async call (see {@link
     * #async(boolean)}): how long to wait for the callback after a {@code 202} before the step is
     * timed out. {@code 0} (the default) means wait indefinitely, bounded only by the saga-level
     * timeout. Only valid on an async call — {@link #build()} rejects a positive value without
     * {@link #async(boolean)}.
     */
    public Builder callbackTimeoutMillis(long callbackTimeoutMillis) {
      this.callbackTimeoutMillis = callbackTimeoutMillis;
      return this;
    }

    /**
     * Builds the {@link HttpCall}.
     *
     * @throws IllegalStateException if both a flat-map {@link #jsonBody(Map)} body and a string
     *     {@link #stringBody(String)} are set; if a body-less verb ({@link HttpMethod#GET}/{@link
     *     HttpMethod#DELETE}) declares any request body (a {@link #jsonBody(Map)} map or a {@link
     *     #stringBody(String)} string); or if an {@link #output(Map)} expression is neither {@link
     *     #BODY_OUTPUT} nor a {@code $.path} expression with non-empty segments; if {@code
     *     callbackTimeoutMillis} is negative; or if {@code callbackTimeoutMillis} is positive on a
     *     non-async call
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
      if (callbackTimeoutMillis < 0) {
        throw new IllegalStateException(
            "callbackTimeoutMillis must be >= 0, got " + callbackTimeoutMillis);
      }
      if (callbackTimeoutMillis > 0 && !async) {
        throw new IllegalStateException(
            "callbackTimeoutMillis is only valid on an async call; set async(true) or remove it");
      }
      validateOutputExpressions(output);
      return new HttpCall(this);
    }

    /**
     * Validates each output expression: it must be the {@link #BODY_OUTPUT} token, or a {@code
     * $.path} expression whose dot-separated segments are all non-empty. A malformed path (missing
     * the {@code $.} prefix, or with a leading/trailing/doubled dot) is rejected here, at build
     * time, rather than surfacing later as a confusing extraction error.
     */
    private static void validateOutputExpressions(Map<String, String> output) {
      for (Map.Entry<String, String> entry : output.entrySet()) {
        String expression = entry.getValue();
        if (BODY_OUTPUT.equals(expression)) {
          continue;
        }
        if (!expression.startsWith("$.")) {
          throw new IllegalStateException(
              "output expression for '"
                  + entry.getKey()
                  + "' must be '"
                  + BODY_OUTPUT
                  + "' or a '$.path' expression, got '"
                  + expression
                  + "'");
        }
        for (String segment : expression.substring(2).split("\\.", -1)) {
          if (segment.isEmpty()) {
            throw new IllegalStateException(
                "output path '"
                    + expression
                    + "' for '"
                    + entry.getKey()
                    + "' has an empty segment");
          }
        }
      }
    }
  }
}
