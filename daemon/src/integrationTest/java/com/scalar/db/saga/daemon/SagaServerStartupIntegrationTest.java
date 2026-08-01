package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.exception.SagaDefinitionException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Startup coverage for configurations {@link DaemonIntegrationTestSupport} does not exercise.
 *
 * <ul>
 *   <li>A declarative definition that references a service with no configured {@code
 *       service.<name>.base_url} makes {@link SagaServer} fail to start — the engine rejects the
 *       unregistered service while registering the definition. This per-reference guard (together
 *       with fail-fast on zero definitions) is what makes a separate "zero endpoints configured"
 *       check unnecessary: any daemon that starts necessarily has every referenced service
 *       configured.
 *   <li>Every engine setting the properties file exposes, from owner id through shutdown, recovery,
 *       and retention, is accepted by the orchestrator builder it is forwarded to. Parsing is
 *       unit-tested; this covers the forwarding, which a mocked orchestrator cannot.
 * </ul>
 */
class SagaServerStartupIntegrationTest {

  // References service "account", but the test configures no service.account.base_url.
  private static final String DEFINITION =
      """
      { "name": "saga", "mode": "SAGA", "steps": [
        { "name": "s", "service": "account",
          "execution":    { "method": "POST", "path": "/x" },
          "compensation": { "method": "POST", "path": "/x" } } ] }
      """;

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
    // Deliberately no scalar.db.saga.server.service.account.base_url.
    Properties props = storeProperties();

    assertThatThrownBy(() -> new SagaServer(SagaServerConfig.load(props)))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void start_withEveryEngineSettingConfigured_startsAndServes() throws Exception {
    Properties props = storeProperties();
    // Deliberately unreachable: the saga is never started, and registration only has to resolve the
    // service name.
    props.setProperty(
        SagaServerConfig.SERVICE_KEY_PREFIX + "account" + SagaServerConfig.SERVICE_BASE_URL_SUFFIX,
        "http://127.0.0.1:1");
    props.setProperty(SagaServerConfig.OWNER_ID_KEY, "saga-daemon-it-0");
    props.setProperty(SagaServerConfig.SHUTDOWN_MODE_KEY, "WAIT_ALL_SAGAS");
    props.setProperty(SagaServerConfig.SHUTDOWN_TIMEOUT_MILLIS_KEY, "5000");
    props.setProperty(SagaServerConfig.RECOVERY_TIMEOUT_MILLIS_KEY, "90000");
    props.setProperty(SagaServerConfig.RECOVERY_INTERVAL_SECONDS_KEY, "300");
    props.setProperty(SagaServerConfig.RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY, "600");
    props.setProperty(SagaServerConfig.RECOVERY_BATCH_SIZE_KEY, "50");
    props.setProperty(SagaServerConfig.RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY, "2");
    props.setProperty(SagaServerConfig.RETENTION_PERIOD_SECONDS_KEY, "3600");
    props.setProperty(SagaServerConfig.RETENTION_CLEANUP_INTERVAL_SECONDS_KEY, "300");
    props.setProperty(SagaServerConfig.RETENTION_BATCH_SIZE_KEY, "100");
    props.setProperty(SagaServerConfig.RETENTION_MAX_CONCURRENT_PURGES_KEY, "2");

    // start() binds both transports and starts the background recovery and retention tasks the
    // settings above configure; close() drains them under the configured shutdown policy.
    try (SagaServer server = new SagaServer(SagaServerConfig.load(props)).start()) {
      assertThat(server.port()).isPositive();
      assertThat(server.grpcPort()).isPositive();
    }
  }

  /** The store and transport settings every case here needs: a SQLite file and ephemeral ports. */
  private Properties storeProperties() {
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, definitionsDir.toString());
    return props;
  }
}
