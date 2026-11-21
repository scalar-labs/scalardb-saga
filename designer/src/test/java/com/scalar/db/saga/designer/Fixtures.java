package com.scalar.db.saga.designer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/** The definition files the repository ships, which every round-trip test opens. */
final class Fixtures {

  private Fixtures() {}

  /** Every definition file under the getting-started and core test folders. */
  static List<Path> definitionFiles() {
    String root = System.getProperty("designer.fixtures.root");
    if (root == null) {
      throw new IllegalStateException("designer.fixtures.root is not set; run through Gradle");
    }
    try (Stream<Path> a = Files.list(Path.of(root, "getting-started/conf/definitions"));
        Stream<Path> b = Files.list(Path.of(root, "core/src/test/resources/sagas"))) {
      List<Path> files =
          Stream.concat(a, b)
              .filter(p -> Workspace.isDefinitionFileName(String.valueOf(p.getFileName())))
              .sorted()
              .toList();
      if (files.size() < 10) {
        throw new IllegalStateException("Expected the repository's definitions, found " + files);
      }
      return files;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
