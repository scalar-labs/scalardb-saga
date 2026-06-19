package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.saga.api.SagaManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link SagaServer}'s startup wiring (definition loading + cleanup), using an
 * injected mock {@link SagaManager} — no ScalarDB and no HTTP port.
 */
class SagaServerTest {

  private static SagaServerConfig configWithDefinitionsPath(Path path) {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, path.toString());
    return SagaServerConfig.load(props);
  }

  @Test
  void constructor_definitionsDirectory_registersOnlyDefinitionFiles(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("a.json"), "{}");
    Files.writeString(dir.resolve("b.yaml"), "{}");
    Files.writeString(dir.resolve("c.yml"), "{}");
    Files.writeString(dir.resolve("d.txt"), "ignored");
    Files.writeString(dir.resolve("notes.md"), "ignored");
    SagaManager manager = mock(SagaManager.class);

    new SagaServer(configWithDefinitionsPath(dir), manager);

    ArgumentCaptor<Path> captor = ArgumentCaptor.forClass(Path.class);
    verify(manager, times(3)).register(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(p -> p.getFileName().toString())
        .containsExactlyInAnyOrder("a.json", "b.yaml", "c.yml");
  }

  @Test
  void constructor_singleDefinitionFile_registersOnce(@TempDir Path dir) throws Exception {
    Path file = Files.writeString(dir.resolve("saga.json"), "{}");
    SagaManager manager = mock(SagaManager.class);

    new SagaServer(configWithDefinitionsPath(file), manager);

    verify(manager, times(1)).register(file);
  }

  @Test
  void constructor_noDefinitionsPath_registersNothing() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.PORT_KEY, "0");
    SagaManager manager = mock(SagaManager.class);

    new SagaServer(SagaServerConfig.load(props), manager);

    verify(manager, never()).register(any(Path.class));
  }

  @Test
  void constructor_definitionRegistrationFails_closesManagerAndPropagates(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("saga.json"), "{}");
    SagaManager manager = mock(SagaManager.class);
    doThrow(new IllegalStateException("bad definition")).when(manager).register(any(Path.class));

    assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), manager))
        .isInstanceOf(IllegalStateException.class);
    verify(manager).close();
  }
}
