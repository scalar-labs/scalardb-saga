package com.scalar.db.saga.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/** Store-properties and temp-file helpers shared by the harnesses. */
final class HarnessSupport {

  private HarnessSupport() {}

  /**
   * The base store properties: the given file when set, else a fresh throwaway SQLite database (the
   * second element is the temp file to delete on close, {@code null} when a file was given).
   */
  static StoreSetup storeProperties(@Nullable Path propertiesFile) {
    Properties props = new Properties();
    if (propertiesFile != null) {
      try (InputStream in = Files.newInputStream(propertiesFile)) {
        props.load(in);
      } catch (IOException e) {
        throw new UncheckedIOException("cannot read properties file " + propertiesFile, e);
      }
      return new StoreSetup(props, null, propertiesFile.toString());
    }
    Path dbPath;
    try {
      dbPath = Files.createTempFile("saga-bench-", ".db");
    } catch (IOException e) {
      throw new UncheckedIOException("cannot create temp SQLite database", e);
    }
    String url = "jdbc:sqlite:" + dbPath.toAbsolutePath() + "?busy_timeout=10000";
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty("scalar.db.contact_points", url);
    return new StoreSetup(props, dbPath, url);
  }

  /** Base store properties plus which temp file (if any) to delete and a description. */
  record StoreSetup(Properties properties, @Nullable Path tempDbPath, String description) {}

  static void applyOverrides(Properties props, Map<String, String> overrides) {
    overrides.forEach(props::setProperty);
  }

  static void deleteQuietly(@Nullable Path path) {
    if (path == null) {
      return;
    }
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      // Best effort: a leftover temp file is not worth failing shutdown over.
    }
  }

  static void deleteRecursivelyQuietly(@Nullable Path dir) {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(dir)) {
      paths.sorted(Comparator.reverseOrder()).forEach(HarnessSupport::deleteQuietly);
    } catch (IOException e) {
      // Best effort, as above.
    }
  }
}
