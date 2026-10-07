package com.scalar.db.saga.integration;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.saga.store.SagaSchema;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Chooses the ScalarDB store an integration test runs against.
 *
 * <p>By default every test gets a private SQLite file, so nothing needs cleaning up. Setting the
 * {@value #PROPERTY} system property to the path of a ScalarDB properties file points the same
 * tests at any other backend instead. That store is shared by every test, so {@link
 * IntegrationTestStoreExtension} empties its saga and coordinator tables before each test, and each
 * test still starts from nothing.
 *
 * <p>The tables themselves are created by the store factory the tests open, so the file carries the
 * same store keys a deployment would, such as {@code
 * scalar.db.saga.store.scalardb.creation_options.no-scaling=true} for DynamoDB Local.
 */
public final class IntegrationTestStore {

  /** System property naming a ScalarDB properties file; unset means a private SQLite file. */
  public static final String PROPERTY = "scalardb.saga.integration_test.properties";

  private static final List<String> SAGA_TABLES =
      List.of(
          SagaSchema.EVENTS_TABLE,
          SagaSchema.STATE_TABLE,
          SagaSchema.PARKED_TABLE,
          SagaSchema.DEFINITIONS_TABLE);

  private IntegrationTestStore() {}

  /**
   * Puts the store connection settings into {@code props}.
   *
   * @param props the properties the test hands to the store factory or the server
   * @param sqliteDb the SQLite file to use when no external store is configured
   */
  public static void configure(Properties props, Path sqliteDb) {
    String file = System.getProperty(PROPERTY);
    if (file == null) {
      props.setProperty("scalar.db.storage", "jdbc");
      props.setProperty(
          "scalar.db.contact_points",
          "jdbc:sqlite:" + sqliteDb.toAbsolutePath() + "?busy_timeout=10000&journal_mode=WAL");
      return;
    }
    props.putAll(load(file));
  }

  /**
   * Empties the external store's saga and coordinator tables, skipping any not created yet. Does
   * nothing when the tests run on SQLite. Called by {@link IntegrationTestStoreExtension} before
   * each test; a test that opens several stores must not see its own rows vanish between them.
   */
  public static void reset() {
    String file = System.getProperty(PROPERTY);
    if (file == null) {
      return;
    }
    try (DistributedTransactionAdmin admin =
        TransactionFactory.create(load(file)).getTransactionAdmin()) {
      if (admin.coordinatorTablesExist()) {
        admin.truncateCoordinatorTables();
      }
      for (String table : SAGA_TABLES) {
        if (admin.tableExists(SagaSchema.NAMESPACE, table)) {
          admin.truncateTable(SagaSchema.NAMESPACE, table);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("could not reset the external saga store", e);
    }
  }

  private static Properties load(String file) {
    Properties external = new Properties();
    try (InputStream in = Files.newInputStream(Paths.get(file))) {
      external.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read " + PROPERTY + "=" + file, e);
    }
    return external;
  }
}
