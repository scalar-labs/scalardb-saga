package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.exception.SagaDefinitionException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Startup-failure coverage: a declarative definition that references a service with no configured
 * {@code service.<name>.base_url} makes {@link SagaServer} fail to start — the engine rejects the
 * unregistered service while registering the definition. This per-reference guard (together with
 * fail-fast on zero definitions) is what makes a separate "zero endpoints configured" check
 * unnecessary: any daemon that starts necessarily has every referenced service configured.
 *
 * <p>Standalone rather than on {@link DaemonIntegrationTestSupport}, which configures the endpoint
 * and asserts a successful start — the opposite scenario.
 */
class SagaServerStartupIntegrationTest {

  // References service "account", but the test configures no service.account.base_url.
  private static final String DEFINITION =
      "{\"name\":\"saga\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\",\"service\":\"account\","
          + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
          + "\"compensation\":{\"method\":\"POST\",\"path\":\"/x\"}}]}";

  private Path tempDbPath;
  private Path definitionsDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDbPath = Files.createTempFile("saga-daemon-startup-", ".db");
    definitionsDir = Files.createTempDirectory("saga-daemon-startup-defs-");
    Files.writeString(definitionsDir.resolve("saga.json"), DEFINITION);
  }

  @AfterEach
  void tearDown() throws Exception {
    Files.deleteIfExists(definitionsDir.resolve("saga.json"));
    Files.deleteIfExists(definitionsDir);
    Files.deleteIfExists(tempDbPath);
  }

  @Test
  void constructor_definitionReferencesUnconfiguredService_failsToStart() {
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    props.setProperty(SagaServerConfig.PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, definitionsDir.toString());
    // Deliberately no scalar.db.saga.server.service.account.base_url.

    assertThatThrownBy(() -> new SagaServer(SagaServerConfig.load(props)))
        .isInstanceOf(SagaDefinitionException.class);
  }
}
