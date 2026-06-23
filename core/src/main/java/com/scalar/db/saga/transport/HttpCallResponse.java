package com.scalar.db.saga.transport;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The response of an HTTP call ({@link HttpExchange}): status code, headers, and body, with typed
 * accessors. Header lookups are case-insensitive (per the HTTP spec).
 *
 * <p>The body is read once (subject to the {@link OutboundHttpPolicy} size limit) and can be viewed
 * as a JSON object ({@link #bodyJsonObject()}), a JSON array ({@link #bodyJsonArray()}),
 * charset-decoded text ({@link #bodyString()}), or raw bytes ({@link #bodyBytes()}). JSON decoding
 * uses the framework's hardened {@link ObjectMapper} (default typing disabled).
 */
public final class HttpCallResponse {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
  private static final TypeReference<List<Object>> LIST_TYPE = new TypeReference<>() {};

  private final int status;
  private final Map<String, List<String>> headers; // case-insensitive keys
  private final byte[] body;
  private final ObjectMapper mapper;

  HttpCallResponse(
      int status, Map<String, List<String>> headers, byte[] body, ObjectMapper mapper) {
    this.status = status;
    TreeMap<String, List<String>> caseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    headers.forEach((name, values) -> caseInsensitive.put(name, List.copyOf(values)));
    this.headers = caseInsensitive;
    this.body = body.clone();
    this.mapper = mapper;
  }

  /** The HTTP status code. */
  public int status() {
    return status;
  }

  /** Whether the status is 2xx. */
  public boolean isSuccess() {
    return HttpStatusClassifier.isSuccess(status);
  }

  /** The first value of the named response header (case-insensitive), if present. */
  public Optional<String> header(String name) {
    List<String> values = headers.get(name);
    return values == null || values.isEmpty() ? Optional.empty() : Optional.of(values.get(0));
  }

  /** All values of the named response header (case-insensitive); empty if absent. */
  public List<String> headers(String name) {
    List<String> values = headers.get(name);
    return values == null ? List.of() : values;
  }

  /** All response headers, keyed by name (case-insensitive) to the list of values. */
  public Map<String, List<String>> headers() {
    return Map.copyOf(headers);
  }

  /**
   * Decodes the body as a value of the given type. An empty body decodes the literal empty input,
   * which Jackson maps to {@code null} for most types.
   *
   * @throws HttpCallException (non-retryable) if the body cannot be decoded as {@code type}
   */
  public <T> T bodyJson(Class<T> type) throws HttpCallException {
    try {
      return mapper.readValue(body, type);
    } catch (IOException e) {
      throw new HttpCallException("Failed to decode response body as " + type.getName(), e, false);
    }
  }

  /**
   * Decodes the body as a JSON object. An empty body yields an empty map.
   *
   * @throws HttpCallException (non-retryable) if the body is not a JSON object
   */
  public Map<String, Object> bodyJsonObject() throws HttpCallException {
    if (body.length == 0) {
      return Map.of();
    }
    try {
      return mapper.readValue(body, MAP_TYPE);
    } catch (IOException e) {
      throw new HttpCallException("Failed to decode response body as a JSON object", e, false);
    }
  }

  /**
   * Decodes the body as a JSON array. An empty body yields an empty list.
   *
   * @throws HttpCallException (non-retryable) if the body is not a JSON array
   */
  public List<Object> bodyJsonArray() throws HttpCallException {
    if (body.length == 0) {
      return List.of();
    }
    try {
      return mapper.readValue(body, LIST_TYPE);
    } catch (IOException e) {
      throw new HttpCallException("Failed to decode response body as a JSON array", e, false);
    }
  }

  /**
   * The body decoded as text using the charset from the response {@code Content-Type} (defaulting
   * to UTF-8). Prefer this over {@code new String(bodyBytes())}, which uses the platform default
   * charset.
   */
  public String bodyString() {
    return new String(body, charset());
  }

  /** The raw response body bytes. */
  public byte[] bodyBytes() {
    return body.clone();
  }

  private Charset charset() {
    Optional<String> contentType = header(HttpHeaders.CONTENT_TYPE);
    if (contentType.isPresent()) {
      String value = contentType.get().toLowerCase(Locale.ROOT);
      int start = value.indexOf("charset=");
      if (start >= 0) {
        String name = value.substring(start + "charset=".length()).trim();
        int end = name.indexOf(';'); // strip any trailing parameters
        if (end >= 0) {
          name = name.substring(0, end).trim();
        }
        name = name.replace("\"", ""); // strip optional quotes
        try {
          if (!name.isEmpty()) {
            return Charset.forName(name);
          }
        } catch (IllegalArgumentException e) {
          // forName throws IllegalCharsetNameException (malformed) or UnsupportedCharsetException
          // (unknown) — both IllegalArgumentException; fall through to the UTF-8 default below.
        }
      }
    }
    return StandardCharsets.UTF_8;
  }
}
