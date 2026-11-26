package com.scalar.db.saga.designer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WorkspaceTest {

  @TempDir Path dir;
  private Workspace workspace;

  @BeforeEach
  void setUp() throws IOException {
    workspace = new Workspace(dir);
  }

  @Test
  void constructor_missingFolderGiven_throwsIOException() {
    // Act Assert
    assertThatThrownBy(() -> new Workspace(dir.resolve("missing"))).isInstanceOf(IOException.class);
  }

  @Test
  void constructor_fileGiven_throwsIOException() throws IOException {
    // Arrange
    Path file = Files.writeString(dir.resolve("a.json"), "{}");

    // Act Assert
    assertThatThrownBy(() -> new Workspace(file)).isInstanceOf(IOException.class);
  }

  @Test
  void list_mixedEntries_returnsOnlyDefinitionFilesSorted() throws IOException {
    // Arrange
    Files.writeString(dir.resolve("b.yaml"), "name: b");
    Files.writeString(dir.resolve("a.json"), "{}");
    Files.writeString(dir.resolve("c.YML"), "name: c");
    Files.writeString(dir.resolve(".hidden.json"), "{}");
    Files.writeString(dir.resolve("README.md"), "#");
    Files.writeString(dir.resolve("a.json.bak"), "{}");
    Files.createDirectory(dir.resolve("folder.json"));
    Files.createDirectory(dir.resolve(Workspace.DESIGNER_DIR));

    // Act Assert
    assertThat(workspace.list()).containsExactly("a.json", "b.yaml", "c.YML");
  }

  @Test
  void list_symlinkLeadingOutside_isLeftOut(@TempDir Path outside) throws IOException {
    // Arrange
    Path target = Files.writeString(outside.resolve("x.json"), "{}");
    Files.createSymbolicLink(dir.resolve("x.json"), target);

    // Act Assert
    assertThat(workspace.list()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "../a.json",
        "a/b.json",
        ".a.json",
        "a.txt",
        "a.json/",
        "",
        "..json",
        "/a.json",
        "a\\b.json",
        "a .json"
      })
  void readDefinition_unacceptableNameGiven_throwsBadRequest(String name) {
    // Act Assert
    assertThatThrownBy(() -> workspace.readDefinition(name))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void readDefinition_symlinkLeadingOutside_throwsBadRequest(@TempDir Path outside)
      throws IOException {
    // Arrange
    Path target = Files.writeString(outside.resolve("x.json"), "{}");
    Files.createSymbolicLink(dir.resolve("x.json"), target);

    // Act Assert
    assertThatThrownBy(() -> workspace.readDefinition("x.json"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void listUneditable_namesTheServerLoadsButTheDesignerRefuses_returnsThemSorted()
      throws IOException {
    // Arrange
    Files.writeString(dir.resolve("order saga.json"), "{}");
    Files.writeString(dir.resolve("café.YAML"), "{}");
    Files.writeString(dir.resolve("plain.json"), "{}");
    Files.writeString(dir.resolve(".hidden json.json"), "{}");
    Files.writeString(dir.resolve("read me.txt"), "");
    Files.createDirectory(dir.resolve("a folder.json"));

    // Act
    List<String> names = workspace.listUneditable();

    // Assert
    assertThat(names).containsExactly("café.YAML", "order saga.json");
  }

  @Test
  void readDefinition_missingFile_returnsNull() throws IOException {
    // Act Assert
    assertThat(workspace.readDefinition("a.json")).isNull();
  }

  @Test
  void readDefinition_existingFile_returnsTextAndSha256() throws IOException {
    // Arrange
    Files.writeString(dir.resolve("a.json"), "{}");

    // Act
    Workspace.FileContent content = requireNonNull(workspace.readDefinition("a.json"));

    // Assert
    assertThat(content.text()).isEqualTo("{}");
    assertThat(content.sha256())
        .isEqualTo("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a");
  }

  @Test
  void writeDefinition_newFileExpected_createsIt() throws IOException {
    // Act
    String sha = workspace.writeDefinition("a.json", "{}", null);

    // Assert
    assertThat(Files.readString(dir.resolve("a.json"))).isEqualTo("{}");
    assertThat(sha).isEqualTo(Workspace.sha256("{}"));
  }

  @Test
  void writeDefinition_matchingSha_replacesTheFileAndLeavesNoTemporaryFile() throws IOException {
    // Arrange
    Files.writeString(dir.resolve("a.json"), "{}");

    // Act
    workspace.writeDefinition("a.json", "{\"a\": 1}", Workspace.sha256("{}"));

    // Assert
    assertThat(Files.readString(dir.resolve("a.json"))).isEqualTo("{\"a\": 1}");
    try (var entries = Files.list(dir)) {
      assertThat(entries).hasSize(1);
    }
  }

  @Test
  void writeDefinition_symlinkInsideTheWorkspace_writesThroughToItsTarget() throws IOException {
    // Arrange
    Files.writeString(dir.resolve("order-v2.json"), "{}");
    Files.createSymbolicLink(dir.resolve("current.json"), Path.of("order-v2.json"));

    // Act
    workspace.writeDefinition("current.json", "{\"a\": 1}", Workspace.sha256("{}"));

    // Assert
    assertThat(Files.isSymbolicLink(dir.resolve("current.json"))).isTrue();
    assertThat(Files.readString(dir.resolve("order-v2.json"))).isEqualTo("{\"a\": 1}");
  }

  @Test
  void writeDefinition_fileWithItsOwnPermissions_keepsThem() throws IOException {
    // Arrange
    Path file = Files.writeString(dir.resolve("a.json"), "{}");
    Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw-r-----");
    Files.setPosixFilePermissions(file, permissions);

    // Act
    workspace.writeDefinition("a.json", "{\"a\": 1}", Workspace.sha256("{}"));

    // Assert
    assertThat(Files.getPosixFilePermissions(file)).isEqualTo(permissions);
  }

  @Test
  void readDefinition_fileOverTheSizeLimit_throwsBadRequest() throws IOException {
    // Arrange
    try (var channel =
        FileChannel.open(
            dir.resolve("a.json"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      channel.write(ByteBuffer.wrap(new byte[] {' '}), Workspace.MAX_FILE_BYTES);
    }

    // Act Assert
    assertThatThrownBy(() -> workspace.readDefinition("a.json"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void readDefinition_fileThatIsNotUtf8_throwsBadRequest() throws IOException {
    // Arrange
    Files.write(dir.resolve("a.json"), new byte[] {'"', (byte) 0xE9, '"'});

    // Act Assert
    assertThatThrownBy(() -> workspace.readDefinition("a.json"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void writeDefinition_fileChangedSinceRead_throwsConflictWithTheCurrentSha() throws IOException {
    // Arrange
    Files.writeString(dir.resolve("a.json"), "{\"changed\": true}");

    // Act Assert
    assertThatThrownBy(() -> workspace.writeDefinition("a.json", "{}", Workspace.sha256("{}")))
        .isInstanceOfSatisfying(
            Workspace.ConflictException.class,
            e -> assertThat(e.currentSha256()).isEqualTo(Workspace.sha256("{\"changed\": true}")));
    assertThat(Files.readString(dir.resolve("a.json"))).isEqualTo("{\"changed\": true}");
  }

  @Test
  void writeDefinition_newFileExpectedButOneExists_throwsConflict() throws IOException {
    // Arrange
    Files.writeString(dir.resolve("a.json"), "{}");

    // Act Assert
    assertThatThrownBy(() -> workspace.writeDefinition("a.json", "{\"a\": 1}", null))
        .isInstanceOf(Workspace.ConflictException.class);
  }

  @Test
  void writeDefinition_fileDeletedSinceRead_throwsConflictWithNoCurrentSha() {
    // Act Assert
    assertThatThrownBy(() -> workspace.writeDefinition("a.json", "{}", Workspace.sha256("{}")))
        .isInstanceOfSatisfying(
            Workspace.ConflictException.class, e -> assertThat(e.currentSha256()).isNull());
  }

  @Test
  void writeDesignerData_noFolderYet_createsTheHiddenFolder() throws IOException {
    // Act
    workspace.writeDesignerData("a.json", "{}");

    // Assert
    assertThat(Files.readString(dir.resolve(".designer/a.json"))).isEqualTo("{}");
    assertThat(workspace.readDesignerData("a.json")).isNotNull();
  }

  @Test
  void readDesignerData_designerPathIsAFile_throwsIOException() throws IOException {
    // Arrange
    Files.writeString(dir.resolve(Workspace.DESIGNER_DIR), "not a folder");

    // Act Assert
    assertThatThrownBy(() -> workspace.readDesignerData("a.json")).isInstanceOf(IOException.class);
  }

  @Test
  void removeStaleTemporaryFiles_oldAndNewTemporaryFiles_removesOnlyTheOldOnes()
      throws IOException {
    // Arrange
    Path old = Files.writeString(dir.resolve(".designer-tmp-old"), "x");
    Files.setLastModifiedTime(old, FileTime.from(Instant.now().minus(Duration.ofHours(2))));
    Path recent = Files.writeString(dir.resolve(".designer-tmp-new"), "x");
    Files.createDirectory(dir.resolve(Workspace.DESIGNER_DIR));
    Path oldData = Files.writeString(dir.resolve(".designer/.designer-tmp-old"), "x");
    Files.setLastModifiedTime(oldData, FileTime.from(Instant.now().minus(Duration.ofHours(2))));
    Path other = Files.writeString(dir.resolve(".other"), "x");
    Files.setLastModifiedTime(other, FileTime.from(Instant.now().minus(Duration.ofHours(2))));

    // Act
    workspace.removeStaleTemporaryFiles();

    // Assert
    assertThat(old).doesNotExist();
    assertThat(oldData).doesNotExist();
    assertThat(recent).exists();
    assertThat(other).exists();
  }

  @Test
  void isDefinitionFileName_plainNamesGiven_acceptsOnlyDefinitionNames() {
    // Act Assert
    assertThat(Workspace.isDefinitionFileName("order-saga_v1.2.json")).isTrue();
    assertThat(Workspace.isDefinitionFileName("order.Yml")).isTrue();
    assertThat(Workspace.isDefinitionFileName("order.properties")).isFalse();
    assertThat(Workspace.isDefinitionFileName("-order.json")).isFalse();
  }
}
