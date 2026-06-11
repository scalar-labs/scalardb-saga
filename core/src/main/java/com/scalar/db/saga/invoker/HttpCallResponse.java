package com.scalar.db.saga.invoker;

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
 * The response of an {@link HttpRequestSpec#send() HTTP call}: status code, headers, and body, with
 * typed accessors. Header lookups are case-insensitive (per the HTTP spec).
 *
 * <p>The body is read once (subject to the {@link OutboundHttpPolicy} size limit) and can be viewed
 * as a JSON object ({@link #jsonObject()}), a JSON array ({@link #jsonArray()}), charset-decoded
 * text ({@link #rawString()}), or raw bytes ({@link #rawBytes()}). JSON decoding uses the
 * framework's hardened {@link ObjectMapper} (default typing disabled).
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

  /**
   * Decodes the body as a JSON object. An empty body yields an empty map.
   *
   * @throws HttpCallException (non-retryable) if the body is not a JSON object
   */
  public Map<String, Object> jsonObject() throws HttpCallException {
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
  public List<Object> jsonArray() throws HttpCallException {
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
   * to UTF-8). Prefer this over {@code new String(rawBytes())}, which uses the platform default
   * charset.
   */
  public String rawString() {
    return new String(body, charset());
  }

  /** The raw response body bytes. */
  public byte[] rawBytes() {
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
        if (!name.isEmpty() && Charset.isSupported(name)) {
          return Charset.forName(name);
        }
      }
    }
    return StandardCharsets.UTF_8;
  }
}
