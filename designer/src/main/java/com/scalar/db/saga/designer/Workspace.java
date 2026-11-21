package com.scalar.db.saga.designer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The folder of definition files the designer edits, and its {@code .designer/} folder of
 * designer-only data.
 *
 * <p>The designer edits a stricter set of files than the server loads from {@code
 * definitions_path}: both take a regular file directly in the folder, named {@code .json}, {@code
 * .yaml} or {@code .yml}, not starting with a dot, but the designer also wants a plain name (ASCII
 * letters, digits, {@code .}, {@code _} and {@code -}, at most 200 characters before the
 * extension), since the name travels in URLs and download headers. {@link #listUneditable} names
 * the files the server loads but the designer will not edit, so none is hidden from the user. Users
 * often mount the folder they edit as the server's {@code definitions_path}, which is why every
 * write is atomic and every temporary file starts with a dot: the server skips dotfiles, and never
 * sees a half-written file.
 */
final class Workspace {

  private static final Logger logger = LoggerFactory.getLogger(Workspace.class);

  /** The folder of designer-only data, a dotfile so that the server ignores it. */
  static final String DESIGNER_DIR = ".designer";

  // Plain names only: no separators, no leading dot, a definition extension. The real-path check
  // in resolve() is what actually keeps reads and writes inside the folder; this keeps names sane.
  private static final Pattern FILE_NAME =
      Pattern.compile("[A-Za-z0-9_][A-Za-z0-9._-]{0,199}\\.(?i:json|yaml|yml)");

  private static final String TEMP_MARKER = ".designer-tmp-";
  private static final Duration STALE_TEMP_AGE = Duration.ofHours(1);

  private final Path root;

  /**
   * Opens a workspace folder.
   *
   * @throws IOException if the folder does not exist or cannot be resolved
   */
  Workspace(Path folder) throws IOException {
    this.root = folder.toRealPath();
    if (!Files.isDirectory(root)) {
      throw new IOException("Not a directory: " + folder);
    }
  }

  Path root() {
    return root;
  }

  /** Returns the definition file names in the folder, sorted. */
  List<String> list() throws IOException {
    List<String> names = new ArrayList<>();
    try (Stream<Path> entries = Files.list(root)) {
      for (Path entry : (Iterable<Path>) entries::iterator) {
        String name = String.valueOf(entry.getFileName());
        if (FILE_NAME.matcher(name).matches() && isRegularFileInside(entry)) {
          names.add(name);
        }
      }
    }
    names.sort(null);
    return names;
  }

  /**
   * Lists the files the server would load as definitions but whose names the designer does not
   * accept, such as {@code order saga.json}, sorted by name.
   */
  List<String> listUneditable() throws IOException {
    List<String> names = new ArrayList<>();
    try (Stream<Path> entries = Files.list(root)) {
      for (Path entry : (Iterable<Path>) entries::iterator) {
        String name = String.valueOf(entry.getFileName());
        String lower = name.toLowerCase(Locale.ROOT);
        boolean deployable =
            !name.startsWith(".")
                && (lower.endsWith(".json") || lower.endsWith(".yaml") || lower.endsWith(".yml"));
        if (deployable && !FILE_NAME.matcher(name).matches() && Files.isRegularFile(entry)) {
          names.add(name);
        }
      }
    }
    names.sort(null);
    return names;
  }

  /** Whether a file name is one the designer reads and writes. */
  static boolean isDefinitionFileName(String fileName) {
    return FILE_NAME.matcher(fileName).matches();
  }

  /** A file's content and the SHA-256 that a later write compares against. */
  record FileContent(String text, String sha256) {}

  /**
   * Reads a definition file.
   *
   * @return the content, or {@code null} if no such file exists
   * @throws BadRequestException if the name is not a definition file name, or leads outside
   */
  @Nullable FileContent readDefinition(String fileName) throws IOException {
    return read(resolve(fileName, false));
  }

  /** Reads the designer data of a definition file, or returns null if it has none. */
  @Nullable FileContent readDesignerData(String fileName) throws IOException {
    return read(resolve(fileName, true));
  }

  /**
   * Replaces a definition file, if it still has the content the caller read.
   *
   * @param expectedSha256 the SHA-256 of the content the caller read, or {@code null} if the caller
   *     expects the file not to exist yet
   * @return the SHA-256 of the written content
   * @throws ConflictException if the file's content is not what the caller expects
   */
  String writeDefinition(String fileName, String text, @Nullable String expectedSha256)
      throws IOException {
    Path target = resolve(fileName, false);
    // Not atomic with the move below. Another process writing in between is out of scope: the
    // check exists for a file changed minutes ago by git or an editor, not for a race.
    requireUnchanged(read(target), expectedSha256);
    return write(target, text);
  }

  /**
   * Checks that a file still has the content the caller read.
   *
   * @param current the file's content now, or {@code null} if it does not exist
   * @param expectedSha256 the SHA-256 the caller read, or {@code null} if it expects no file
   * @throws ConflictException if the two differ
   */
  static void requireUnchanged(@Nullable FileContent current, @Nullable String expectedSha256) {
    String currentSha = current == null ? null : current.sha256();
    if (currentSha == null ? expectedSha256 != null : !sameSha(currentSha, expectedSha256)) {
      throw new ConflictException(currentSha);
    }
  }

  /** Replaces the designer data of a definition file. */
  String writeDesignerData(String fileName, String text) throws IOException {
    Path target = resolve(fileName, true);
    Files.createDirectories(root.resolve(DESIGNER_DIR));
    return write(target, text);
  }

  /**
   * Removes temporary files an interrupted write left behind, if they are over an hour old, so a
   * write still in progress elsewhere is never touched.
   */
  void removeStaleTemporaryFiles() {
    for (Path dir : List.of(root, root.resolve(DESIGNER_DIR))) {
      if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
        continue;
      }
      try (Stream<Path> entries = Files.list(dir)) {
        Instant cutoff = Instant.now().minus(STALE_TEMP_AGE);
        for (Path entry : (Iterable<Path>) entries::iterator) {
          if (String.valueOf(entry.getFileName()).startsWith(TEMP_MARKER)) {
            FileTime modified = Files.getLastModifiedTime(entry, LinkOption.NOFOLLOW_LINKS);
            if (modified.toInstant().isBefore(cutoff)) {
              Files.deleteIfExists(entry);
            }
          }
        }
      } catch (IOException e) {
        logger.warn("Could not remove old temporary files in {}: {}", dir, e.toString());
      }
    }
  }

  /** The write was refused because the file changed since the caller read it. */
  static final class ConflictException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    static final String MESSAGE = "The file changed on disk since it was opened";

    private final @Nullable String currentSha256;

    ConflictException(@Nullable String currentSha256) {
      super(MESSAGE);
      this.currentSha256 = currentSha256;
    }

    /** The SHA-256 of the file now, or {@code null} if it no longer exists. */
    @Nullable String currentSha256() {
      return currentSha256;
    }
  }

  // Constant time. The SHA is not a secret, so this is not needed for safety; it is what SpotBugs
  // expects of any hash comparison, and costs nothing.
  private static boolean sameSha(String a, @Nullable String b) {
    return b != null
        && MessageDigest.isEqual(
            a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
  }

  static String sha256(String text) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  // The name is checked against FILE_NAME (no separators, no leading dot) and the resolved real
  // path against the workspace, so a user-supplied name cannot reach outside the folder.
  private Path resolve(String fileName, boolean designerData) throws IOException {
    if (!isDefinitionFileName(fileName)) {
      throw new BadRequestException("Not a definition file name: '" + fileName + "'");
    }
    Path dir = designerData ? root.resolve(DESIGNER_DIR) : root;
    if (designerData
        && Files.exists(dir, LinkOption.NOFOLLOW_LINKS)
        && !Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException(DESIGNER_DIR + " exists but is not a folder");
    }
    Path target = dir.resolve(fileName);
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && !isRegularFileInside(target)) {
      throw new BadRequestException("'" + fileName + "' is not a regular file in the workspace");
    }
    return target;
  }

  /** A regular file whose real path, symlinks resolved, is inside the workspace. */
  private boolean isRegularFileInside(Path path) {
    try {
      Path real = path.toRealPath();
      return real.startsWith(root) && Files.isRegularFile(real);
    } catch (IOException e) {
      return false;
    }
  }

  // Reading a file whole into memory is fine for a definition; this keeps a stray huge file in the
  // folder from exhausting the heap. It matches the designer's own request size limit, so anything
  // the designer can save, it can also read back.
  static final long MAX_FILE_BYTES = 4L * 1024 * 1024;

  /**
   * Reads a file as UTF-8.
   *
   * @return the content, or {@code null} if there is no such file
   * @throws BadRequestException if the file is too large or is not UTF-8 text
   */
  private static @Nullable FileContent read(Path path) throws IOException {
    String name = String.valueOf(path.getFileName());
    try {
      if (Files.size(path) > MAX_FILE_BYTES) {
        throw new BadRequestException(
            "'" + name + "' is larger than " + MAX_FILE_BYTES / (1024 * 1024) + " MiB");
      }
      String text = Files.readString(path, StandardCharsets.UTF_8);
      return new FileContent(text, sha256(text));
    } catch (NoSuchFileException e) {
      return null;
    } catch (CharacterCodingException e) {
      throw new BadRequestException("'" + name + "' is not UTF-8 text");
    }
  }

  /**
   * Replaces a file atomically. A symlink is written through, so the link stays a link and its
   * target gets the content; the file keeps its permissions. resolve() has already checked that a
   * link's target is inside the workspace.
   */
  private static String write(Path path, String text) throws IOException {
    Path target = Files.exists(path, LinkOption.NOFOLLOW_LINKS) ? path.toRealPath() : path;
    Path temp = target.resolveSibling(TEMP_MARKER + UUID.randomUUID());
    try {
      Files.writeString(temp, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
      copyPermissions(target, temp);
      try {
        Files.move(
            temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        // The temporary file is in the same folder, so this only happens on file systems with no
        // atomic rename at all; a plain replace is the best they offer.
        Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temp);
    }
    return sha256(text);
  }

  private static void copyPermissions(Path from, Path to) throws IOException {
    if (!Files.exists(from)) {
      return;
    }
    try {
      Files.setPosixFilePermissions(to, Files.getPosixFilePermissions(from));
    } catch (UnsupportedOperationException e) {
      // Not a POSIX file system: there are no permission bits to keep.
    }
  }
}
