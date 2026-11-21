package com.scalar.db.saga.designer;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Refuses requests that do not come from the designer's own page in a browser on this machine.
 *
 * <p>The designer has no authentication and can write files, so two attacks matter, both made by
 * some other web page open in the same browser:
 *
 * <ul>
 *   <li><b>Cross-site requests.</b> A page elsewhere can send a request to {@code localhost}. A
 *       state-changing request must therefore carry {@code Content-Type: application/json}, which a
 *       cross-site page can only send after a CORS preflight that the designer never answers, and
 *       an {@code Origin} that is the designer's own.
 *   <li><b>DNS rebinding.</b> A page can point its own host name at {@code 127.0.0.1} and then read
 *       responses as same-origin. Its requests still carry its own host name, so any {@code Host}
 *       other than a loopback name or the address the designer was bound to is refused.
 * </ul>
 *
 * <p>The {@code Host} check stays on for every bind, a wildcard one included: a rebinding page
 * reaches a designer on {@code 0.0.0.0} just as well as one on {@code 127.0.0.1}. On a wildcard
 * bind, the machine's own addresses are accepted; any other name, such as a host name used from a
 * VM, has to be allowed with {@code --allowed-host}.
 */
final class RequestGuard {

  private static final Set<String> LOOPBACK_NAMES = Set.of("localhost", "127.0.0.1", "[::1]");
  private static final Set<String> SAFE_METHODS = Set.of("GET", "HEAD");

  private final Set<String> allowedHosts;

  /**
   * Creates a guard for a designer bound to {@code bindHost}.
   *
   * @param bindHost the address passed to {@code --host}
   * @param extraHosts further host names to accept, from {@code --allowed-host}
   * @param localAddresses the machine's own addresses, accepted when {@code bindHost} is a wildcard
   */
  RequestGuard(
      String bindHost, Collection<String> extraHosts, Collection<InetAddress> localAddresses) {
    Set<String> hosts = new HashSet<>();
    LOOPBACK_NAMES.forEach(name -> hosts.add(canonical(name)));
    if (isWildcard(bindHost)) {
      for (InetAddress address : localAddresses) {
        hosts.add(name(address));
      }
    } else {
      hosts.add(canonical(bindHost));
    }
    for (String extra : extraHosts) {
      hosts.add(canonical(extra));
    }
    this.allowedHosts = Set.copyOf(hosts);
  }

  /** A guard that accepts only loopback names and {@code bindHost}. */
  RequestGuard(String bindHost) {
    this(bindHost, Set.of(), Set.of());
  }

  /** Whether {@code host} is an address that listens on every interface. */
  static boolean isWildcard(String host) {
    String name = canonical(host);
    return name.equals("0.0.0.0") || name.equals("[0:0:0:0:0:0:0:0]");
  }

  /** An address as {@link #canonical} writes it: IPv6 in brackets, in full, without a scope. */
  private static String name(InetAddress address) {
    String text = address.getHostAddress().toLowerCase(Locale.ROOT);
    if (address instanceof Inet6Address) {
      int scope = text.indexOf('%');
      return "[" + (scope < 0 ? text : text.substring(0, scope)) + "]";
    }
    return text;
  }

  /**
   * One spelling per host, so that {@code [::1]}, {@code [0:0:0:0:0:0:0:1]} and {@code ::1} all
   * match: lower case, and an IPv6 literal in brackets in Java's full form.
   */
  private static String canonical(String host) {
    String lower = host.toLowerCase(Locale.ROOT);
    String inner =
        lower.startsWith("[") && lower.endsWith("]")
            ? lower.substring(1, lower.length() - 1)
            : lower;
    if (inner.contains(":")) {
      try {
        return name(Inet6Address.ofLiteral(inner));
      } catch (IllegalArgumentException e) {
        return "[" + inner + "]";
      }
    }
    return lower;
  }

  /**
   * Checks one request.
   *
   * @return why the request is refused, or {@code null} to let it through
   */
  @Nullable String refusal(
      String method,
      @Nullable String hostHeader,
      @Nullable String originHeader,
      @Nullable String contentType) {
    if (hostHeader == null || hostHeader.isBlank()) {
      return "Missing Host header";
    }
    String hostName = canonical(hostName(hostHeader));
    if (!allowedHosts.contains(hostName)) {
      return "Unexpected Host header";
    }
    if (SAFE_METHODS.contains(method.toUpperCase(Locale.ROOT))) {
      return null;
    }
    if (contentType == null
        || !contentType.toLowerCase(Locale.ROOT).strip().startsWith("application/json")) {
      return "A request that changes something must be JSON";
    }
    if (originHeader != null && !originHeader.equalsIgnoreCase("http://" + hostHeader)) {
      return "Cross-origin request";
    }
    return null;
  }

  /** The host part of a Host header: {@code 127.0.0.1:7070} → {@code 127.0.0.1}. */
  private static String hostName(String hostHeader) {
    if (hostHeader.startsWith("[")) {
      int end = hostHeader.indexOf(']');
      return end < 0 ? hostHeader : hostHeader.substring(0, end + 1);
    }
    int colon = hostHeader.indexOf(':');
    return colon < 0 ? hostHeader : hostHeader.substring(0, colon);
  }
}
