package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaDefinitionException;
import io.javalin.Javalin;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link SagaServer}'s startup wiring (definition loading + code-step rejection +
 * cleanup), using an injected mock {@link DefaultSagaOrchestrator} — no ScalarDB and no HTTP port.
 * The server parses each definition file itself (to reject code steps), so the files must be valid
 * declarative definitions; the mock {@code register(...)} is a no-op, so no service endpoints are
 * needed.
 */
class SagaServerTest {

  /** A minimal valid declarative (service-step) definition as JSON. */
  private static String declarativeJson(String name) {
    return "{\"name\":\""
        + name
        + "\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\",\"service\":\"svc\","
        + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
        + "\"compensation\":{\"method\":\"POST\",\"path\":\"/y\"}}]}";
  }

  /** The same definition as YAML. */
  private static String declarativeYaml(String name) {
    return "name: "
        + name
        + "\nmode: SAGA\nsteps:\n  - name: s\n    service: svc\n    execution:\n"
        + "      method: POST\n      path: /x\n    compensation:\n      method: POST\n      path: /y\n";
  }

  /** A code-step definition (rejected in daemon mode). */
  private static String codeStepJson(String name) {
    return "{\"name\":\""
        + name
        + "\",\"steps\":[{\"name\":\"s\",\"stepClass\":\"com.example.Foo\"}]}";
  }

  private static SagaServerConfig configWithDefinitionsPath(Path path) {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, path.toString());
    return SagaServerConfig.load(props);
  }

  @Test
  void constructor_definitionsDirectory_registersOnlyDefinitionFiles(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("a.json"), declarativeJson("a"));
    Files.writeString(dir.resolve("b.yaml"), declarativeYaml("b"));
    Files.writeString(dir.resolve("c.yml"), declarativeYaml("c"));
    Files.writeString(dir.resolve("d.txt"), "ignored");
    Files.writeString(dir.resolve("notes.md"), "ignored");
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);

    new SagaServer(configWithDefinitionsPath(dir), manager);

    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(manager, times(3)).register(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(SagaDefinition::getName)
        .containsExactlyInAnyOrder("a", "b", "c");
  }

  @Test
  void constructor_singleDefinitionFile_registersOnce(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);

    new SagaServer(configWithDefinitionsPath(dir.resolve("saga.json")), manager);

    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(manager, times(1)).register(captor.capture());
    assertThat(captor.getValue().getName()).isEqualTo("saga");
  }

  @Test
  void constructor_directoryWithDefinitionExtension_isIgnored(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("real.json"), declarativeJson("real"));
    Files.createDirectory(dir.resolve("nested.json")); // a directory that matches the extension
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);

    new SagaServer(configWithDefinitionsPath(dir), manager);

    ArgumentCaptor<SagaDefinition> captor = ArgumentCaptor.forClass(SagaDefinition.class);
    verify(manager, times(1)).register(captor.capture());
    assertThat(captor.getValue().getName()).isEqualTo("real");
  }

  @Test
  void constructor_noDefinitionsPath_throwsAndClosesManager() {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.PORT_KEY, "0");
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);

    assertThatThrownBy(() -> new SagaServer(SagaServerConfig.load(props), manager))
        .isInstanceOf(IllegalStateException.class);
    verify(manager, never()).register(any(SagaDefinition.class));
    verify(manager).close();
  }

  @Test
  void constructor_noDefinitionFiles_throwsAndClosesManager(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("notes.md"), "ignored"); // no .json/.yaml/.yml definitions
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);

    assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), manager))
        .isInstanceOf(IllegalStateException.class);
    verify(manager, never()).register(any(SagaDefinition.class));
    verify(manager).close();
  }

  @Test
  void constructor_codeStepDefinition_throwsAndClosesManager(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("code.json"), codeStepJson("code"));
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);

    assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), manager))
        .isInstanceOf(SagaDefinitionException.class);
    verify(manager, never()).register(any(SagaDefinition.class));
    verify(manager).close();
  }

  @Test
  void constructor_definitionRegistrationFails_closesManagerAndPropagates(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);
    doThrow(new IllegalStateException("bad definition"))
        .when(manager)
        .register(any(SagaDefinition.class));

    assertThatThrownBy(() -> new SagaServer(configWithDefinitionsPath(dir), manager))
        .isInstanceOf(IllegalStateException.class);
    verify(manager).close();
  }

  @Test
  void start_portUnavailable_closesManagerAndPropagates(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    // Hold an ephemeral port with another server so SagaServer's app.start(...) fails to bind.
    Javalin portHolder = Javalin.create().start(0);
    try {
      Properties props = new Properties();
      props.setProperty(SagaServerConfig.PORT_KEY, Integer.toString(portHolder.port()));
      props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
      DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);
      SagaServer server = new SagaServer(SagaServerConfig.load(props), manager);

      assertThatThrownBy(server::start).isInstanceOf(RuntimeException.class);

      verify(manager).startBackgroundTasks();
      verify(manager).close();
    } finally {
      portHolder.stop();
    }
  }

  @Test
  void close_calledTwice_drainsManagerOnce(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    DefaultSagaOrchestrator manager = mock(DefaultSagaOrchestrator.class);
    SagaServer server = new SagaServer(configWithDefinitionsPath(dir), manager);

    server.close();
    server.close();

    verify(manager, times(1)).close();
  }
}
