package com.scalar.db.saga.exception;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java-8 substitute for {@code Map.of(...)} used at exception throw sites to build the metadata map
 * for a {@link SagaErrorCode}. {@code api/} is compiled under {@code --release 8}, so the Java-9+
 * {@code Map.of} factories are unavailable.
 *
 * <p>Returns an unmodifiable {@code Map<String,String>} that the {@link SagaRuntimeException}
 * constructor consumes as-is (it takes its own defensive copy). {@link LinkedHashMap} preserves
 * throw-site argument order for readability in a debugger; {@link SagaErrorCode#buildMessage}
 * iterates in schema-declared order regardless.
 *
 * <p>Overloads cover arity 0-3, which matches every current {@link ErrorMetadataSchema}. Extend as
 * arity grows.
 */
public final class ErrorMetadata {

  private ErrorMetadata() {}

  public static Map<String, String> of() {
    return Collections.emptyMap();
  }

  public static Map<String, String> of(String k1, String v1) {
    return Collections.singletonMap(k1, v1);
  }

  public static Map<String, String> of(String k1, String v1, String k2, String v2) {
    Map<String, String> m = new LinkedHashMap<>();
    m.put(k1, v1);
    m.put(k2, v2);
    return Collections.unmodifiableMap(m);
  }

  public static Map<String, String> of(
      String k1, String v1, String k2, String v2, String k3, String v3) {
    Map<String, String> m = new LinkedHashMap<>();
    m.put(k1, v1);
    m.put(k2, v2);
    m.put(k3, v3);
    return Collections.unmodifiableMap(m);
  }
}
