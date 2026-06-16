package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.HttpCall;
import com.scalar.db.saga.api.SagaContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the declarative expression syntax shared by all transports (Layer 2b):
 *
 * <ul>
 *   <li>{@code ${key}} in a request/path/query value → the saga context value for {@code key}. A
 *       value that is exactly one {@code ${key}} keeps the value's type (e.g. an {@code Integer});
 *       a value with embedded {@code ${...}} (or none) resolves to a string. A {@code ${key}} with
 *       no context value is a non-retryable error (a definition/data mistake, not transient).
 *   <li>{@code $.path} in an output value → a field navigated from the JSON response object.
 *   <li>{@code $body} in an output value → the entire raw response body as a {@code String}
 *       (decoded with the response charset, UTF-8 by default), which works for non-JSON responses.
 * </ul>
 */
final class DeclarativeExpressions {

  /** The output token capturing the entire raw response body as a {@code String}. */
  static final String BODY_OUTPUT = HttpCall.BODY_OUTPUT;

  private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]*)\\}");
  private static final Pattern SINGLE_PLACEHOLDER = Pattern.compile("^\\$\\{([^}]+)\\}$");

  private DeclarativeExpressions() {}

  /**
   * Resolves a value, preserving the context value's type when it is exactly one {@code ${key}}.
   */
  static Object resolveObject(String template, SagaContext context) throws TransportException {
    Matcher single = SINGLE_PLACEHOLDER.matcher(template);
    if (single.matches()) {
      return require(single.group(1), context);
    }
    return resolveString(template, context);
  }

  /** Resolves a value to a string, substituting every {@code ${key}} occurrence. */
  static String resolveString(String template, SagaContext context) throws TransportException {
    Matcher matcher = PLACEHOLDER.matcher(template);
    StringBuilder result = new StringBuilder();
    while (matcher.find()) {
      Object value = require(matcher.group(1), context);
      matcher.appendReplacement(result, Matcher.quoteReplacement(String.valueOf(value)));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  /**
   * Resolves a URL path template, percent-encoding each substituted {@code ${key}} value as a
   * single path segment so a context value cannot inject path/query/fragment delimiters (or break
   * the request on a space). Literal template text — the {@code /} separators and any static {@code
   * ?}/{@code #} — is left untouched. A {@code /} inside a value is encoded to {@code %2F}, so the
   * value stays one segment and cannot traverse the path (e.g. {@code ../../admin}).
   */
  static String resolvePath(String template, SagaContext context) throws TransportException {
    Matcher matcher = PLACEHOLDER.matcher(template);
    StringBuilder result = new StringBuilder();
    while (matcher.find()) {
      String encoded = encodePathSegment(String.valueOf(require(matcher.group(1), context)));
      matcher.appendReplacement(result, Matcher.quoteReplacement(encoded));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String encodePathSegment(String value) {
    // URLEncoder targets application/x-www-form-urlencoded, where space is '+'; remap it to %20 for
    // a valid URL path segment. Everything else URLEncoder escapes (/, ?, #, %, control chars, ...)
    // is exactly what must not pass through literally into the path.
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  /** Resolves every value in {@code templates} to a string (e.g. query parameters). */
  static Map<String, String> resolveStringMap(Map<String, String> templates, SagaContext context)
      throws TransportException {
    Map<String, String> resolved = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : templates.entrySet()) {
      resolved.put(entry.getKey(), resolveString(entry.getValue(), context));
    }
    return resolved;
  }

  /** Resolves every value in {@code templates}, preserving types (e.g. a JSON request body). */
  static Map<String, Object> resolveObjectMap(Map<String, String> templates, SagaContext context)
      throws TransportException {
    Map<String, Object> resolved = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : templates.entrySet()) {
      resolved.put(entry.getKey(), resolveObject(entry.getValue(), context));
    }
    return resolved;
  }

  /**
   * Extracts each output expression from {@code response} into a context-key → value map. A {@code
   * $.path} value navigates the JSON response object; the {@link #BODY_OUTPUT} token captures the
   * entire raw response body as a {@code String}. The JSON object is decoded lazily, only when a
   * {@code $.path} expression is present, so a {@link #BODY_OUTPUT}-only mapping works for a
   * non-JSON response.
   */
  static Map<String, Object> extractOutput(
      Map<String, String> outputMapping, HttpCallResponse response) throws TransportException {
    Map<String, Object> output = new LinkedHashMap<>();
    Map<String, Object> jsonObject = null;
    for (Map.Entry<String, String> entry : outputMapping.entrySet()) {
      String expression = entry.getValue();
      if (BODY_OUTPUT.equals(expression)) {
        output.put(entry.getKey(), response.bodyString());
        continue;
      }
      if (jsonObject == null) {
        try {
          jsonObject = response.bodyJsonObject();
        } catch (HttpCallException e) {
          throw new TransportException("HTTP transport error: " + e.getMessage(), e, false);
        }
      }
      output.put(entry.getKey(), extractPath(expression, jsonObject));
    }
    return output;
  }

  /** Extracts each {@code $.path} from a decoded JSON {@code response} object. */
  static Map<String, Object> extractOutput(
      Map<String, String> outputMapping, Map<String, Object> response) throws TransportException {
    Map<String, Object> output = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : outputMapping.entrySet()) {
      output.put(entry.getKey(), extractPath(entry.getValue(), response));
    }
    return output;
  }

  private static Object require(String key, SagaContext context) throws TransportException {
    return context
        .get(key, Object.class)
        .orElseThrow(
            () -> new TransportException("No saga context value for '${" + key + "}'", false));
  }

  private static Object extractPath(String path, Map<String, Object> response)
      throws TransportException {
    if (!path.startsWith("$.")) {
      throw new TransportException("Output path must start with '$.': '" + path + "'", false);
    }
    Object current = response;
    for (String segment : path.substring(2).split("\\.", -1)) {
      if (!(current instanceof Map<?, ?> map)) {
        throw new TransportException(
            "Cannot navigate output path '" + path + "' into a non-object at '" + segment + "'",
            false);
      }
      if (!map.containsKey(segment)) {
        throw new TransportException(
            "Response is missing field '" + segment + "' for output path '" + path + "'", false);
      }
      current = map.get(segment);
    }
    if (current == null) {
      throw new TransportException("Output path '" + path + "' resolved to null", false);
    }
    return current;
  }
}
