package com.scalar.db.saga.server.security;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * The transport-agnostic input a {@link SagaSecurityProvider} authenticates: the caller's
 * credential-bearing headers plus request metadata (operation and source address) for the provider
 * and the audit trail.
 *
 * <p>Deliberately not tied to HTTP: the REST before-handler builds one from the Javalin request
 * headers, and the gRPC interceptor builds one from call {@code Metadata} — so a single {@link
 * SagaSecurityProvider} authenticates both transports. Header lookup is <b>case-insensitive</b>
 * ({@code Authorization} vs {@code authorization}); providers read whichever header carries their
 * credential ({@code Authorization: Bearer …} for JWT, a configured API-key header for API keys).
 */
public final class SagaAuthRequest {

  private final String operation;
  private final @Nullable String remoteAddress;
  private final Function<String, @Nullable String> headerLookup;

  private SagaAuthRequest(
      String operation,
      @Nullable String remoteAddress,
      Function<String, @Nullable String> headerLookup) {
    this.operation = operation;
    this.remoteAddress = remoteAddress;
    this.headerLookup = headerLookup;
  }

  /**
   * Builds a request whose headers are looked up through {@code headerLookup}, a function the
   * caller supplies (given a header name, returns its value or {@code null}). Use this when the
   * underlying transport already offers case-insensitive header access (e.g. Javalin's {@code
   * ctx.header}), so no copy is made.
   *
   * @param operation a short label for the operation being authorized, for audit/logging (e.g. an
   *     HTTP {@code "POST /sagas"} or a gRPC method name)
   * @param remoteAddress the caller's source address if known, else {@code null}
   * @param headerLookup a case-insensitive header accessor (name → value or {@code null})
   * @return the request
   */
  public static SagaAuthRequest fromHeaderLookup(
      String operation,
      @Nullable String remoteAddress,
      Function<String, @Nullable String> headerLookup) {
    Objects.requireNonNull(operation, "operation must not be null");
    Objects.requireNonNull(headerLookup, "headerLookup must not be null");
    return new SagaAuthRequest(operation, remoteAddress, headerLookup);
  }

  /**
   * Builds a request from a header map, normalizing keys to lower case so lookup is
   * case-insensitive regardless of the map's own casing. Use this when the transport hands over
   * headers as a plain map (e.g. gRPC {@code Metadata} flattened to entries).
   *
   * @param operation a short label for the operation being authorized, for audit/logging
   * @param remoteAddress the caller's source address if known, else {@code null}
   * @param headers the request headers (keys matched case-insensitively)
   * @return the request
   */
  public static SagaAuthRequest fromHeaders(
      String operation, @Nullable String remoteAddress, Map<String, String> headers) {
    Objects.requireNonNull(operation, "operation must not be null");
    Objects.requireNonNull(headers, "headers must not be null");
    // Copy into a lower-cased table so the lookup is independent of the source map's casing and
    // immune to later mutation of the caller's map.
    Map<String, String> normalized = new HashMap<>();
    headers.forEach((name, value) -> normalized.put(name.toLowerCase(Locale.ROOT), value));
    return new SagaAuthRequest(
        operation, remoteAddress, name -> normalized.get(name.toLowerCase(Locale.ROOT)));
  }

  /** Returns the operation label being authorized (for audit/logging). */
  public String operation() {
    return operation;
  }

  /** Returns the caller's source address, if the transport exposed one. */
  public Optional<String> remoteAddress() {
    return Optional.ofNullable(remoteAddress);
  }

  /**
   * Returns the value of the named request header (case-insensitive), or empty if absent. A
   * provider reads the header carrying its credential — e.g. {@code header("Authorization")}.
   */
  public Optional<String> header(String name) {
    Objects.requireNonNull(name, "name must not be null");
    return Optional.ofNullable(headerLookup.apply(name));
  }
}
