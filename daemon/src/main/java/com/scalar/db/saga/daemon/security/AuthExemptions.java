package com.scalar.db.saga.daemon.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * An allowlist of request paths that bypass the RBAC before-handler — the <b>exemption hook</b>,
 * the single agreed interface between the security layer (PR C) and routes that carry their own
 * auth.
 *
 * <p>Two kinds of route are exempt from caller-facing (JWT/API-key) auth:
 *
 * <ul>
 *   <li>the <b>liveness probe</b> ({@code GET /health}) — an infrastructure probe holds no user
 *       credential;
 *   <li>the <b>async-callback route</b> ({@code POST /sagas/{id}/steps/{stepName}/complete}, from
 *       PR B) — authenticated by its per-step HMAC callback token, not by a user credential. PR B
 *       exposes {@code CallbackResource.PATH} precisely so it can be registered here.
 * </ul>
 *
 * <p>Patterns use Javalin path syntax: a {@code {param}} segment matches one path segment and a
 * {@code <param>} segment matches one or more (across {@code /}). Matching is against the concrete
 * request path, independent of Javalin's route-matching internals, so it is stable regardless of
 * before-handler ordering. Immutable and thread-safe.
 */
public final class AuthExemptions {

  private final List<Pattern> patterns;

  private AuthExemptions(List<Pattern> patterns) {
    this.patterns = patterns;
  }

  /** An empty allowlist — no path is exempt. */
  public static AuthExemptions none() {
    return new AuthExemptions(List.of());
  }

  /**
   * Builds an allowlist from Javalin-style path patterns (e.g. {@code "/health"}, {@code
   * "/sagas/{id}/steps/{stepName}/complete"}).
   *
   * @param pathPatterns the exempt route patterns
   * @return the allowlist
   */
  public static AuthExemptions of(String... pathPatterns) {
    Objects.requireNonNull(pathPatterns, "pathPatterns must not be null");
    List<Pattern> compiled = new ArrayList<>(pathPatterns.length);
    for (String pattern : pathPatterns) {
      compiled.add(compile(pattern));
    }
    return new AuthExemptions(List.copyOf(compiled));
  }

  /**
   * Returns whether {@code requestPath} (a concrete path, e.g. {@code
   * /sagas/abc/steps/reserve/complete}) matches any exempt pattern and should bypass RBAC.
   *
   * @param requestPath the concrete request path
   * @return {@code true} if the path is exempt
   */
  public boolean isExempt(String requestPath) {
    Objects.requireNonNull(requestPath, "requestPath must not be null");
    String normalized = stripTrailingSlash(requestPath);
    for (Pattern pattern : patterns) {
      if (pattern.matcher(normalized).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compiles a Javalin path pattern into an anchored regex: {@code {param}} → one segment ({@code
   * [^/]+}), {@code <param>} → one-or-more segments ({@code .+}), every literal character quoted.
   */
  private static Pattern compile(String pathPattern) {
    Objects.requireNonNull(pathPattern, "pathPattern must not be null");
    String trimmed = stripTrailingSlash(pathPattern);
    StringBuilder regex = new StringBuilder("^");
    for (String segment : splitSegments(trimmed)) {
      regex.append('/');
      if (segment.startsWith("{") && segment.endsWith("}")) {
        regex.append("[^/]+");
      } else if (segment.startsWith("<") && segment.endsWith(">")) {
        regex.append(".+");
      } else {
        regex.append(Pattern.quote(segment));
      }
    }
    regex.append('$');
    return Pattern.compile(regex.toString());
  }

  /**
   * Splits a path into its non-empty {@code /}-delimited segments. Hand-rolled rather than {@code
   * String.split} to avoid that method's surprising trailing-empty-token behavior (and the Error
   * Prone {@code StringSplitter} warning).
   */
  private static List<String> splitSegments(String path) {
    List<String> segments = new ArrayList<>();
    int start = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == '/') {
        if (i > start) {
          segments.add(path.substring(start, i));
        }
        start = i + 1;
      }
    }
    if (path.length() > start) {
      segments.add(path.substring(start));
    }
    return segments;
  }

  /**
   * Drops a single trailing slash so {@code /health} and {@code /health/} match alike (but keeps
   * root {@code /}).
   */
  private static String stripTrailingSlash(String path) {
    if (path.length() > 1 && path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }
}
