package com.scalar.db.saga.daemon;

import org.jspecify.annotations.Nullable;

/**
 * Whether a URL/bind host literal denotes loopback ({@code localhost}, {@code 127.0.0.0/8}, {@code
 * ::1}).
 *
 * <p>Matched on the literal, never resolved via DNS, so parsing stays offline and a spoofable name
 * cannot pass as loopback. The IPv4 clause is a numeric {@code 127.0.0.0/8} parse rather than a
 * {@code "127."} prefix test: a bare prefix also accepts the DNS hostname {@code 127.attacker.org}
 * (and {@code 127.0.0.1.evil.com}, {@code 127.foo}), which an attacker can point at their own host
 * and serve a plaintext JWKS from, reintroducing the key-swap bypass the literal match exists to
 * prevent. The parse is fail-closed: anything non-numeric is not loopback.
 *
 * <p>Shared by {@link JwtConfig}'s JWKS-https trust-anchor exception and {@link SagaServer}'s
 * network-exposure bind warning so the two cannot drift.
 */
public final class LoopbackHost {

  private LoopbackHost() {}

  /** Whether {@code host} (a URL or bind host literal, never DNS-resolved) is loopback. */
  public static boolean isLoopback(@Nullable String host) {
    if (host == null || host.isEmpty()) {
      return false;
    }
    String literal = host;
    if (literal.startsWith("[") && literal.endsWith("]")) {
      // URL.getHost keeps the brackets around an IPv6 literal (e.g. "[::1]").
      literal = literal.substring(1, literal.length() - 1);
    }
    return literal.equalsIgnoreCase("localhost")
        || literal.equals("::1")
        || isIpv4Loopback(literal);
  }

  /**
   * Whether {@code literal} is a dotted-decimal IPv4 address in {@code 127.0.0.0/8}. Fail-closed:
   * requires exactly four numeric octets ({@code 0}-{@code 255}) with the first equal to {@code
   * 127}, so a DNS name that merely starts with {@code "127."} does not qualify.
   */
  private static boolean isIpv4Loopback(String literal) {
    String[] octets = literal.split("\\.", -1);
    if (octets.length != 4) {
      return false;
    }
    for (int i = 0; i < 4; i++) {
      int value = parseOctet(octets[i]);
      if (value < 0) {
        return false;
      }
      if (i == 0 && value != 127) {
        return false;
      }
    }
    return true;
  }

  /**
   * Parses a 1-3 digit decimal octet, or returns -1 if {@code s} is not a {@code 0}-{@code 255}.
   */
  private static int parseOctet(String s) {
    if (s.isEmpty() || s.length() > 3) {
      return -1;
    }
    int value = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < '0' || c > '9') {
        return -1;
      }
      value = value * 10 + (c - '0');
    }
    return value <= 255 ? value : -1;
  }
}
