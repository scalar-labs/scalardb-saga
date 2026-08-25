package com.scalar.db.saga.server;

import com.scalar.db.saga.server.SagaServerConfig.ServiceConfig;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the {@code services_path} directory: one {@code <service-name>.properties} file per
 * downstream service, with prefix-free keys ({@code base_url}, {@code allowed_hosts}, {@code
 * max_body_bytes}, {@code header.<HeaderName>}). The service name is the file name minus the
 * required {@code .properties} extension.
 *
 * <p>Directory hygiene, built for the mounted-ConfigMap layout this directory is expected to be:
 * entries whose name starts with {@code .} are ignored (kubelet's {@code ..data} indirection and
 * its timestamped directories), and a visible symlink — the shape kubelet publishes every key in,
 * {@code account.properties -> ..data/account.properties} — must resolve to a regular file still
 * inside the directory. Any other entry (a stray non-{@code .properties} file, or a symlink
 * escaping the directory, which would be a second route to reading arbitrary files) is an error.
 * Files are size-capped so a mis-placed large file cannot stall loading.
 *
 * <p>Every value guard the prefixed {@code service.<name>.*} format enforced lives here now: blank
 * rejection, the engine-reserved and JDK-restricted header names, case-colliding header names, and
 * comma-list hygiene. Secret references resolve through {@link ServiceSecretResolver}, confined to
 * the secrets root; error messages follow the module's {@link Redaction} contract and never echo a
 * resolved value.
 *
 * <p>Per-file parsing fails fast on the first problem in that file; the directory loader stops at
 * the first failing file. (The reload pass that later reuses this parser aggregates errors across
 * files itself — per-file fail-fast, cross-file aggregation.)
 */
final class ServiceFileParser {

  private static final Logger logger = LoggerFactory.getLogger(ServiceFileParser.class);

  static final String PROPERTIES_EXTENSION = ".properties";

  /**
   * Cap on one config file, service or definition. A legitimate file is a handful of lines;
   * anything near this cap is a mis-placed artifact, and reading it whole would only defer the
   * failure to a confusing place. The reconciler bounds definition files by the same value: the two
   * mounts are the same kind of thing, and one cap is one number for an operator to know.
   */
  static final long MAX_FILE_BYTES = 1024 * 1024;

  /**
   * The shape a service name (and so a service file's base name) must have: the same conservative
   * set the owner id uses, because service names land in definitions, log lines, and error messages
   * verbatim.
   */
  private static final Pattern SERVICE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9._-]{1,128}");

  private static final String BASE_URL_KEY = "base_url";
  private static final String ALLOWED_HOSTS_KEY = "allowed_hosts";
  private static final String MAX_BODY_BYTES_KEY = "max_body_bytes";
  private static final String HEADER_KEY_PREFIX = "header.";

  /**
   * Header names the engine issues itself, so configuring one as a service header cannot change
   * what a participant receives: the engine's value wins. {@code X-Saga-Callback-Url} is set only
   * on the async-step requests that need one, so configuring it is worse than inert — the rest of
   * the requests would carry a callback URL the engine never issued. Rejected at parse, where the
   * error can name the offending key, rather than leaving an operator to wonder why their header
   * never arrives. Compared case-insensitively, matching the HTTP spec and the engine's own header
   * merge. The names mirror the engine's internal {@code HttpHeaders}, not visible from here.
   */
  private static final Set<String> RESERVED_HEADERS = reservedHeaders();

  private static Set<String> reservedHeaders() {
    Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    names.addAll(List.of("X-Saga-Id", "X-Saga-Step", "X-Saga-Callback-Url"));
    return Collections.unmodifiableSet(names);
  }

  /** The system property with which the JDK opens individual {@link #JDK_RESTRICTED_HEADERS}. */
  private static final String ALLOW_RESTRICTED_HEADERS_PROPERTY =
      "jdk.httpclient.allowRestrictedHeaders";

  /**
   * Header names {@code java.net.http} owns for framing, connection management, and routing, and so
   * refuses outright: {@code HttpRequest.Builder.header()} throws {@link IllegalArgumentException}
   * on them. Unlike a {@link #RESERVED_HEADERS} name, one of these does not merely fail to arrive —
   * the engine cannot build the request at all, so every call to the service fails permanently and
   * compensates. Nothing catches that at startup: the throw lands on the first outbound call, where
   * it is reported against the URI rather than the config key that caused it, while liveness and
   * readiness stay green. Rejecting at parse turns a silent, service-wide outage into a startup
   * error naming the key.
   *
   * <p>Whoever sets {@link #ALLOW_RESTRICTED_HEADERS_PROPERTY} takes a name back off this set, so
   * the check forbids exactly what the JDK forbids rather than a fixed five; {@code Host} is worth
   * opening to route a participant through a shared ingress. Read from the system property, which
   * is where the JDK looks first — a name opened through {@code conf/net.properties} instead is not
   * visible here, since {@code sun.net.NetProperties} is not exported.
   */
  private static final Set<String> JDK_RESTRICTED_HEADERS =
      jdkRestrictedHeaders(System.getProperty(ALLOW_RESTRICTED_HEADERS_PROPERTY));

  /**
   * Returns the restricted header names the JDK still refuses once {@code allowRestrictedHeaders} —
   * the raw {@link #ALLOW_RESTRICTED_HEADERS_PROPERTY} value, or null when unset — has opened the
   * names it lists.
   *
   * <p>Deliberately mirrors {@code jdk.internal.net.http.common.Utils.getDisallowedHeaders()} quirk
   * for quirk: the whole value is trimmed once and split on commas, but the tokens themselves are
   * not trimmed, so {@code "host, connection"} opens only {@code host}. Trimming the tokens here
   * would be the friendlier reading and exactly the wrong one — it would accept a config key the
   * JDK then rejects at send time, which is the bug this check exists to prevent. Token matching is
   * case-insensitive, as it is there.
   *
   * @param allowRestrictedHeaders the raw system property value, or null when unset
   * @return the names still refused, compared case-insensitively
   */
  static Set<String> jdkRestrictedHeaders(@Nullable String allowRestrictedHeaders) {
    Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    names.addAll(List.of("Connection", "Content-Length", "Expect", "Host", "Upgrade"));
    if (allowRestrictedHeaders != null) {
      for (String token : allowRestrictedHeaders.trim().split(",", -1)) {
        names.remove(token);
      }
    }
    return Collections.unmodifiableSet(names);
  }

  private ServiceFileParser() {}

  /**
   * Parses every service file in {@code servicesPath} into one {@link ServiceConfig} per service,
   * applying the directory hygiene and, when {@code allowedHostsCeiling} is non-empty, requiring
   * every service's {@code allowed_hosts} to be a non-empty subset of it (an empty allowlist means
   * allow-all, which a ceiling by definition forbids).
   *
   * @throws IllegalArgumentException on an unreadable directory, a stray entry, or any per-file
   *     parse failure (prefixed with the file name)
   */
  static Map<String, ServiceConfig> parseDirectory(
      Path servicesPath, ServiceSecretResolver secrets, List<String> allowedHostsCeiling) {
    Map<String, ServiceConfig> services = new LinkedHashMap<>();
    for (Map.Entry<String, ServiceFile> entry : listServiceFiles(servicesPath).entrySet()) {
      ServiceFile file = entry.getValue();
      ServiceConfig service = parseFile(entry.getKey(), file.fileName(), file.target(), secrets);
      requireWithinCeiling(entry.getKey(), service, allowedHostsCeiling);
      services.put(entry.getKey(), service);
    }
    return services;
  }

  /**
   * One directory entry the hygiene walk admitted: the visible file name (for error attribution)
   * and the fully resolved target the containment check vouched for — every read must go through
   * the target, never re-open the visible entry (see {@link #parseFile}).
   */
  record ServiceFile(String fileName, Path target) {}

  /**
   * The hygiene walk alone: lists the directory's service files as {@code name → entry}, in name
   * order, applying the dot-entry, symlink-containment, stray-entry, and name-shape rules.
   * Structural problems (an unreadable directory, a stray entry, an escaping symlink) throw;
   * per-file content problems are the caller's to surface, so the reload pass can aggregate them
   * across files while boot fails fast.
   */
  static Map<String, ServiceFile> listServiceFiles(Path servicesPath) {
    // The path value is redacted, and the cause is named by class rather than message:
    // services_path is a resolved configuration value, so a secret reference pasted onto that key
    // arrives here as its plaintext — and a filesystem exception's message is the path itself.
    // These messages reach the reload WARN on every pass that rejects.
    if (!Files.isDirectory(servicesPath)) {
      throw new IllegalArgumentException(
          "'"
              + SagaServerConfig.SERVICES_PATH_KEY
              + "' is not a readable directory "
              + Redaction.redacted(servicesPath.toString()));
    }
    Path realServicesPath;
    try {
      realServicesPath = servicesPath.toRealPath();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "'"
              + SagaServerConfig.SERVICES_PATH_KEY
              + "' cannot be resolved ("
              + e.getClass().getSimpleName()
              + ") "
              + Redaction.redacted(servicesPath.toString()));
    }
    Map<String, ServiceFile> files = new LinkedHashMap<>();
    List<Path> entries;
    try (Stream<Path> stream = Files.list(servicesPath)) {
      entries = stream.sorted().toList();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "'"
              + SagaServerConfig.SERVICES_PATH_KEY
              + "' cannot be listed ("
              + e.getClass().getSimpleName()
              + ") "
              + Redaction.redacted(servicesPath.toString()));
    }
    for (Path entry : entries) {
      String fileName = Objects.requireNonNull(entry.getFileName()).toString();
      if (fileName.startsWith(".")) {
        // kubelet's ..data symlink and ..<timestamp> directories, and ordinary dotfiles.
        continue;
      }
      Path target;
      if (Files.isSymbolicLink(entry)) {
        // Kubelet publishes every visible key of a projected volume as a symlink through its
        // ..data indirection (account.properties -> ..data/account.properties), so a visible
        // symlink is the expected shape of the mounted-ConfigMap layout, not an anomaly. It just
        // must not become a second route to reading an arbitrary file under a service's name;
        // requiring the resolved target to stay inside the directory forbids exactly that.
        target = requireContainedRegularTarget(entry, fileName, realServicesPath);
      } else if (Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
        try {
          target = entry.toRealPath();
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "services_path entry '" + fileName + "' cannot be resolved (" + e.getMessage() + ")",
              e);
        }
      } else {
        throw new IllegalArgumentException(
            "services_path entry '"
                + fileName
                + "' is not a regular file. Every non-dot entry must be a <service-name>"
                + PROPERTIES_EXTENSION
                + " file so a stray artifact cannot be silently skipped.");
      }
      if (!fileName.endsWith(PROPERTIES_EXTENSION)) {
        throw new IllegalArgumentException(
            "services_path entry '"
                + fileName
                + "' is not a "
                + PROPERTIES_EXTENSION
                + " file. Every non-dot entry must be a <service-name>"
                + PROPERTIES_EXTENSION
                + " file so a stray artifact cannot be silently skipped.");
      }
      String name = fileName.substring(0, fileName.length() - PROPERTIES_EXTENSION.length());
      if (!SERVICE_NAME_PATTERN.matcher(name).matches()) {
        throw new IllegalArgumentException(
            "Service file '"
                + fileName
                + "' has an invalid service name; the base name must match "
                + SERVICE_NAME_PATTERN.pattern());
      }
      files.put(name, new ServiceFile(fileName, target));
    }
    return files;
  }

  /**
   * Requires a visible symlink entry to resolve to a regular file still inside {@code
   * realServicesPath} after full symlink resolution — the same containment {@link
   * ServiceSecretResolver} applies to the secrets root. Kubelet's projected-volume chain ({@code
   * account.properties -> ..data/account.properties -> ..<timestamp>/account.properties}) stays
   * within the mount directory, so it passes; a link escaping the directory, dangling, or landing
   * on anything but a regular file is the arbitrary-file route this check exists to forbid.
   *
   * @return the resolved target, which the caller must read instead of {@code entry}: re-opening
   *     the entry would follow the symlink afresh, so a swap between this check and the read could
   *     redirect the read to a file the containment never validated
   */
  private static Path requireContainedRegularTarget(
      Path entry, String fileName, Path realServicesPath) {
    Path target;
    try {
      target = entry.toRealPath();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "services_path entry '"
              + fileName
              + "' is a symlink that cannot be resolved ("
              + e.getMessage()
              + ")",
          e);
    }
    if (!target.startsWith(realServicesPath)
        || !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
      throw new IllegalArgumentException(
          "services_path entry '"
              + fileName
              + "' is a symlink that does not resolve to a regular file inside services_path,"
              + " which would be a route to reading an arbitrary file under a service's name."
              + " Only the mounted-volume indirection, a link resolving within the directory, is"
              + " allowed.");
    }
    return target;
  }

  /**
   * Parses one service file. Fails fast on the first problem, with every message prefixed by the
   * visible file name so the caller can aggregate across files without losing attribution.
   *
   * <p>{@code file} must be the fully resolved path the directory scan validated. It is opened once
   * with {@code NOFOLLOW_LINKS} and the size cap is enforced on the bytes read through that handle:
   * the directory can change between validation and read (the reload pass repeats this parse
   * indefinitely), and no such change may redirect or unbound the read the validation vouched for.
   */
  static ServiceConfig parseFile(
      String name, String fileName, Path file, ServiceSecretResolver secrets) {
    return parseFile(name, fileName, readBounded(fileName, file), secrets);
  }

  /**
   * Reads one service file through the resolved target, bounding the read at {@link
   * #MAX_FILE_BYTES}. The bound is enforced on the bytes actually read rather than on a prior size
   * check: the file can grow between a stat and the read.
   */
  private static byte[] readBounded(String fileName, Path file) {
    try (InputStream in =
        Files.newInputStream(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      byte[] bytes = in.readNBytes((int) MAX_FILE_BYTES + 1);
      if (bytes.length > MAX_FILE_BYTES) {
        throw new IllegalArgumentException(
            "Service file '"
                + fileName
                + "' exceeds the "
                + MAX_FILE_BYTES
                + "-byte cap; a service file is a handful of lines");
      }
      return bytes;
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Service file '" + fileName + "' cannot be read (" + e.getMessage() + ")", e);
    }
  }

  /**
   * Parses one service file from bytes already read, so a caller that also hashes the file hashes
   * and parses the very same snapshot. Reading twice would let a writer change the file in between,
   * leaving the recorded hash describing content that was never applied — and the reconciler
   * repeats this parse against a directory a writer keeps updating.
   */
  static ServiceConfig parseFile(
      String name, String fileName, byte[] content, ServiceSecretResolver secrets) {
    Properties properties = new Properties();
    try {
      properties.load(
          new StringReader(
              StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(content)).toString()));
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Service file '" + fileName + "' cannot be read (" + e.getMessage() + ")", e);
    }

    String baseUrl = null;
    List<String> allowedHosts = List.of();
    long maxBodyBytes = 0;
    Map<String, String> headers = new LinkedHashMap<>();
    for (String key : new TreeSet<>(properties.stringPropertyNames())) {
      String raw = properties.getProperty(key);
      if (raw != null && raw.contains("${env:")) {
        // Legal but self-defeating in a service file: the environment cannot change in a running
        // pod, so an env-sourced value never rotates without a restart — the thing this directory
        // exists to avoid. The key comes from an unvalidated file, so it is sanitized against log
        // forging (CRLF) before it reaches the log.
        logger.warn(
            "Service file '{}' key '{}' uses ${{env:...}}; environment variables cannot change in"
                + " a running process, so this value will not pick up rotation",
            fileName,
            Redaction.oneLine(key));
      }
      // Unquoted: the delegated validators quote the whole key themselves, so quoting here would
      // double up in their messages.
      String qualifiedKey = "service file " + fileName + " key " + key;
      // Every setting's value goes through the resolver before validation, as every daemon
      // property did in the prefixed format; only the unknown-key error fires on the raw value,
      // since naming the typo helps more than resolving it.
      switch (key) {
        case BASE_URL_KEY -> {
          baseUrl =
              SagaServerConfig.requireNonBlank(qualifiedKey, resolve(secrets, raw, qualifiedKey));
          try {
            // The same rules every endpoint construction enforces, surfaced here so a bad URL is
            // a per-file validation error instead of an apply-time one. The value is not echoed:
            // it may have been resolved from a secret reference.
            HttpServiceConfig.validateBaseUrl(baseUrl);
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                qualifiedKey + ": " + e.getMessage() + " " + Redaction.redacted(baseUrl));
          }
        }
        case ALLOWED_HOSTS_KEY -> {
          allowedHosts =
              SagaServerConfig.parseCommaSeparated(
                  qualifiedKey,
                  SagaServerConfig.requireNonBlank(
                      qualifiedKey, resolve(secrets, raw, qualifiedKey)));
          allowedHosts.forEach(host -> requireHostShape(qualifiedKey, host));
        }
        case MAX_BODY_BYTES_KEY ->
            maxBodyBytes =
                SagaServerConfig.parseBoundedLong(
                    resolve(secrets, raw, qualifiedKey), qualifiedKey, 0L, 1L);
        default -> {
          if (!key.startsWith(HEADER_KEY_PREFIX)) {
            throw new IllegalArgumentException(
                "Unknown setting '"
                    + key
                    + "' in service file '"
                    + fileName
                    + "'. Valid settings are "
                    + BASE_URL_KEY
                    + ", "
                    + ALLOWED_HOSTS_KEY
                    + ", "
                    + MAX_BODY_BYTES_KEY
                    + ", and "
                    + HEADER_KEY_PREFIX
                    + "<HeaderName>.");
          }
          String header = key.substring(HEADER_KEY_PREFIX.length());
          if (header.isBlank()) {
            throw new IllegalArgumentException(
                "Service file '" + fileName + "' key '" + key + "' has no header name.");
          }
          if (RESERVED_HEADERS.contains(header)) {
            throw new IllegalArgumentException(
                "Service file '"
                    + fileName
                    + "' sets header '"
                    + header
                    + "', which the engine issues itself. Its value wins on every request the"
                    + " engine sets it on, so configuring it here either has no effect or sends a"
                    + " header the engine never issued. Remove the key.");
          }
          if (JDK_RESTRICTED_HEADERS.contains(header)) {
            throw new IllegalArgumentException(
                "Service file '"
                    + fileName
                    + "' sets header '"
                    + header
                    + "', which the JDK's HTTP client — not the engine — refuses to send: it owns"
                    + " that name for framing, connection management, and routing. Left in place,"
                    + " every call to service '"
                    + name
                    + "' would fail permanently and compensate. Remove the key, or start the"
                    + " daemon with -D"
                    + ALLOW_RESTRICTED_HEADERS_PROPERTY
                    + "="
                    + header.toLowerCase(Locale.ROOT)
                    + " to allow it (comma-separated for several, with no spaces around the"
                    + " commas).");
          }
          String duplicate = findSameNameIgnoringCase(headers, header);
          if (duplicate != null) {
            throw new IllegalArgumentException(
                "Service file '"
                    + fileName
                    + "' sets header '"
                    + duplicate
                    + "' and '"
                    + header
                    + "', which differ only in case. HTTP header names are case-insensitive, so"
                    + " only one of the two would be sent, and which one is not deterministic."
                    + " Remove one of them.");
          }
          headers.put(
              header,
              SagaServerConfig.requireNonBlank(qualifiedKey, resolve(secrets, raw, qualifiedKey)));
        }
      }
    }
    if (baseUrl == null) {
      throw new IllegalArgumentException(
          "Service file '"
              + fileName
              + "' has no '"
              + BASE_URL_KEY
              + "', so there is nothing for a declarative step to call.");
    }
    return new ServiceConfig(baseUrl, allowedHosts, maxBodyBytes, headers);
  }

  /**
   * Resolves {@code raw} through the confined resolver with failures attributed to {@code
   * qualifiedKey}. The resolver knows only the reference it is handed, not which file and key it
   * came from, and some of its failures (an invalid charset name, an unparsable path) escape its
   * own message wrapping entirely — so the attribution this class promises is added here.
   */
  private static String resolve(ServiceSecretResolver secrets, String raw, String qualifiedKey) {
    try {
      return secrets.resolve(raw);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          qualifiedKey + " cannot be resolved (" + e.getMessage() + ")", e);
    }
  }

  /**
   * Enforces the operator ceiling: with a ceiling set, every service must declare a non-empty
   * {@code allowed_hosts} that is a subset of it. Empty means allow-all, which is precisely what a
   * ceiling exists to forbid.
   */
  static void requireWithinCeiling(String name, ServiceConfig service, List<String> ceiling) {
    if (ceiling.isEmpty()) {
      return;
    }
    if (service.allowedHosts().isEmpty()) {
      throw new IllegalArgumentException(
          "Service '"
              + name
              + "' has no allowed_hosts, but egress.allowed_hosts_ceiling is set: allow-all is not"
              + " permitted under a ceiling. Declare the hosts this service may call.");
    }
    for (String host : service.allowedHosts()) {
      if (!ceiling.contains(host)) {
        // Redacted like every other rejected value: allowed_hosts is resolved before it is
        // checked, so a secret reference pasted onto this key arrives here as its plaintext, and
        // this message is logged on every pass that rejects.
        throw new IllegalArgumentException(
            "Service '"
                + name
                + "' allows a host outside egress.allowed_hosts_ceiling "
                + Redaction.redacted(host)
                + ". A service file cannot authorize egress beyond the operator ceiling.");
      }
    }
  }

  /**
   * Rejects an {@code allowed_hosts} entry that is not shaped like a host, mirroring exactly what
   * the engine's outbound policy enforces: a port suffix would silently never match, since the
   * allowlist is compared against {@code URI.getHost()}, and an IPv6 literal keeps its brackets.
   *
   * <p>Checking it HERE rather than letting the engine reject it at apply time is what keeps a
   * resolved value out of the log. The engine's message names the offending host, and this module
   * cannot redact a message the engine composes — so the rule is that nothing unvalidated is ever
   * handed across that boundary. The same reasoning already applies to {@code base_url}.
   */
  private static void requireHostShape(String qualifiedKey, String host) {
    String normalized = host.trim().toLowerCase(Locale.ROOT);
    boolean bracketed = normalized.startsWith("[");
    boolean malformed =
        normalized.isEmpty()
            || (bracketed ? !normalized.endsWith("]") : normalized.indexOf(':') >= 0);
    if (malformed) {
      throw new IllegalArgumentException(
          qualifiedKey
              + " has an entry that is not a host name "
              + Redaction.redacted(host)
              + ". Give a host without a port (an IPv6 literal keeps its brackets); the allowlist"
              + " is matched against the request URI's host.");
    }
  }

  /**
   * Returns the header name already collected for this service that differs from {@code header}
   * only in case, or null when there is none. A scan rather than a second case-insensitive index:
   * one service's header set is a handful of entries, and the map keeps the operator's own spelling
   * for the error message.
   */
  private static @Nullable String findSameNameIgnoringCase(
      Map<String, String> headers, String header) {
    for (String existing : headers.keySet()) {
      if (existing.equalsIgnoreCase(header)) {
        return existing;
      }
    }
    return null;
  }
}
