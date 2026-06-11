package com.scalar.db.saga.invoker;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Outbound HTTP safety policy: an optional SSRF host allowlist and a maximum body size. Shared by
 * {@code HttpServiceInvoker} and, in a later change, the declarative {@code HttpTransportAdapter}.
 *
 * <p>When the allowlist is empty, all hosts are permitted (service endpoints are already
 * pre-configured by name — the allowlist is defense in depth). Body limits are always enforced.
 */
final class OutboundHttpPolicy {

  static final long DEFAULT_MAX_BODY_BYTES = 1024L * 1024L; // 1 MB

  private final Set<String> allowedHosts; // empty = allow all; stored trimmed + lowercased
  private final long maxBodyBytes;

  private OutboundHttpPolicy(Set<String> allowedHosts, long maxBodyBytes) {
    this.allowedHosts = allowedHosts;
    this.maxBodyBytes = maxBodyBytes;
  }

  /** A policy that allows all hosts with the default 1 MB body limit. */
  static OutboundHttpPolicy allowAll() {
    return new OutboundHttpPolicy(Set.of(), DEFAULT_MAX_BODY_BYTES);
  }

  static Builder newBuilder() {
    return new Builder();
  }

  /** Returns {@code true} if the URI's host may be called. An empty allowlist permits any host. */
  boolean isAllowed(URI uri) {
    if (allowedHosts.isEmpty()) {
      return true;
    }
    String host = uri.getHost();
    return host != null && allowedHosts.contains(host.toLowerCase(Locale.ROOT));
  }

  long maxBodyBytes() {
    return maxBodyBytes;
  }

  static final class Builder {

    private final Set<String> allowedHosts = new LinkedHashSet<>();
    private long maxBodyBytes = DEFAULT_MAX_BODY_BYTES;

    private Builder() {}

    Builder allowedHosts(String... hosts) {
      for (String host : hosts) {
        Objects.requireNonNull(host, "host must not be null");
        String normalized = host.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
          throw new IllegalArgumentException("host must not be blank");
        }
        allowedHosts.add(normalized);
      }
      return this;
    }

    Builder maxBodyBytes(long maxBodyBytes) {
      if (maxBodyBytes <= 0) {
        throw new IllegalArgumentException("maxBodyBytes must be > 0, got " + maxBodyBytes);
      }
      this.maxBodyBytes = maxBodyBytes;
      return this;
    }

    OutboundHttpPolicy build() {
      return new OutboundHttpPolicy(Set.copyOf(allowedHosts), maxBodyBytes);
    }
  }
}
