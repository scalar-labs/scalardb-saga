package com.scalar.db.saga.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * Filesystem hygiene for the two mounted configuration directories, {@code services_path} and
 * {@code definitions_path}.
 *
 * <p>This is a security boundary, which is why it is one class rather than a rule each directory
 * implements for itself: a mounted directory is written by whoever controls the ConfigMap, and the
 * daemon must read exactly the files that directory publishes and nothing else. A hardening fix
 * belongs in one place, and so does the review of it — two copies drift, and the divergence is
 * invisible until someone diffs them.
 *
 * <p>The rules, in the order an entry meets them: resolve every entry fully, require what a symlink
 * resolves to stay inside the directory and be a regular file, hand back that resolved target so
 * the caller never re-opens the visible entry, and bound the read on the bytes actually read. What
 * differs between the two directories — which extensions count, whether a stray entry is rejected
 * or skipped, whether a single file may stand in for a directory — is the callers' business and
 * stays with them.
 */
final class WatchedFiles {

  /**
   * Cap on one configuration file, service or definition. A legitimate file is a handful of lines;
   * anything near this cap is a mis-placed artifact, and reading it whole would only defer the
   * failure to a confusing place.
   */
  static final long MAX_FILE_BYTES = 1024 * 1024;

  private WatchedFiles() {}

  /**
   * Fully resolves a configured directory, for use as the containment root.
   *
   * <p>Path values are redacted and causes named by class, here and throughout: these are resolved
   * configuration values, so a secret reference pasted onto one of these keys arrives as its
   * plaintext — and a filesystem exception's message is the path itself. The messages reach the
   * reload WARN on every pass that rejects.
   */
  static Path realPathOf(Path directory, String configKey) {
    try {
      return directory.toRealPath();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "'"
              + configKey
              + "' cannot be resolved ("
              + e.getClass().getSimpleName()
              + ") "
              + Redaction.redacted(directory.toString()));
    }
  }

  /** Lists a watched directory's entries in name order, so a pass is deterministic. */
  static List<Path> listSorted(Path directory, String configKey) {
    try (Stream<Path> stream = Files.list(directory)) {
      return stream.sorted().toList();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "'"
              + configKey
              + "' cannot be listed ("
              + e.getClass().getSimpleName()
              + ") "
              + Redaction.redacted(directory.toString()));
    }
  }

  /**
   * Resolves one directory entry to the path a caller may read.
   *
   * <p>Kubelet publishes every visible key of a projected volume as a symlink through its {@code
   * ..data} indirection ({@code account.properties -> ..data/account.properties -> ..
   * <timestamp>/account.properties}), so a visible symlink is the expected shape of a mounted
   * ConfigMap, not an anomaly. It just must not become a second route to reading an arbitrary file
   * under a config file's name, so a symlink is admitted only when it resolves to a regular file
   * still inside the directory — the same containment {@link ServiceSecretResolver} applies to the
   * secrets root.
   *
   * @return the resolved target, which the caller must read instead of the entry: re-opening the
   *     entry would follow its symlink afresh, so a swap between this check and the read could
   *     redirect the read to a file the containment never validated. {@code null} when the entry is
   *     neither a symlink nor a regular file, which the two directories treat differently.
   */
  static @Nullable Path resolveEntry(Path entry, String fileName, Path realRoot, String configKey) {
    if (Files.isSymbolicLink(entry)) {
      Path target;
      try {
        target = entry.toRealPath();
      } catch (IOException e) {
        throw new IllegalArgumentException(
            configKey
                + " entry '"
                + Redaction.oneLine(fileName)
                + "' is a symlink that cannot be resolved ("
                + e.getClass().getSimpleName()
                + ")");
      }
      if (!target.startsWith(realRoot) || !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
        throw new IllegalArgumentException(
            configKey
                + " entry '"
                + Redaction.oneLine(fileName)
                + "' is a symlink that does not resolve to a regular file inside "
                + configKey
                + ", which would be a route to reading an arbitrary file under a configuration"
                + " file's name. Only the mounted-volume indirection, a link resolving within the"
                + " directory, is allowed.");
      }
      return target;
    }
    if (Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
      try {
        return entry.toRealPath();
      } catch (IOException e) {
        throw new IllegalArgumentException(
            configKey
                + " entry '"
                + Redaction.oneLine(fileName)
                + "' cannot be resolved ("
                + e.getClass().getSimpleName()
                + ")");
      }
    }
    return null;
  }

  /**
   * Reads a watched file through the target {@link #resolveEntry} validated, bounding the read at
   * {@code cap}.
   *
   * <p>The bound is enforced on the bytes actually read rather than on a prior size check: a file
   * can grow between a stat and the read, and the reload pass repeats indefinitely against
   * directories a writer keeps updating.
   */
  static byte[] read(String fileName, Path target, long cap) {
    try (InputStream in =
        Files.newInputStream(target, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      byte[] content = in.readNBytes((int) cap + 1);
      if (content.length > cap) {
        throw new IllegalArgumentException(
            "File '"
                + Redaction.oneLine(fileName)
                + "' exceeds the "
                + cap
                + "-byte cap on watched configuration files.");
      }
      return content;
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "File '"
              + Redaction.oneLine(fileName)
              + "' cannot be read ("
              + e.getClass().getSimpleName()
              + ").");
    }
  }

  /** The visible name of a path, which is what every message attributes a problem to. */
  static String fileNameOf(Path path) {
    return Objects.requireNonNull(path.getFileName()).toString();
  }
}
