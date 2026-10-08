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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>The file may also carry {@value #CREATE_OPTIONS_PROPERTY}, a comma-separated {@code
 * name:value} list of ScalarDB table creation options such as {@code replication-factor:1} for a
 * single-node Cassandra or {@code no-scaling:true,no-backup:true} for DynamoDB Local. It is removed
 * before the properties reach ScalarDB.
 */
public final class IntegrationTestStore {

  /** System property naming a ScalarDB properties file; unset means a private SQLite file. */
  public static final String PROPERTY = "scalardb.saga.integration_test.properties";

  /** Key inside that file for the creation options; see the class comment. */
  public static final String CREATE_OPTIONS_PROPERTY =
      "scalardb.saga.integration_test.create_options";

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
    Properties external = new Properties();
    try (InputStream in = Files.newInputStream(Paths.get(file))) {
      external.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read " + PROPERTY + "=" + file, e);
    }
    creationOptions(external);
    props.putAll(external);
  }

  /**
   * Creates the external store's saga and coordinator tables if needed and empties them. Does
   * nothing when the tests run on SQLite. Called by {@link IntegrationTestStoreExtension} before
   * each test; a test that opens several stores must not see its own rows vanish between them.
   */
  public static void reset() {
    String file = System.getProperty(PROPERTY);
    if (file == null) {
      return;
    }
    Properties external = new Properties();
    try (InputStream in = Files.newInputStream(Paths.get(file))) {
      external.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read " + PROPERTY + "=" + file, e);
    }
    resetTables(external, creationOptions(external));
  }

  private static Map<String, String> creationOptions(Properties props) {
    Map<String, String> options = new HashMap<>();
    Object spec = props.remove(CREATE_OPTIONS_PROPERTY);
    if (spec != null) {
      for (String pair : spec.toString().split(",", -1)) {
        String[] nameAndValue = pair.split(":", 2);
        if (nameAndValue.length != 2) {
          throw new IllegalArgumentException(
              CREATE_OPTIONS_PROPERTY + " entries are name:value, got \"" + pair + "\"");
        }
        options.put(nameAndValue[0].trim(), nameAndValue[1].trim());
      }
    }
    return options;
  }

  /** Creates the saga and coordinator tables if needed, then empties them. */
  private static void resetTables(Properties props, Map<String, String> options) {
    try (DistributedTransactionAdmin admin =
        TransactionFactory.create(props).getTransactionAdmin()) {
      admin.createCoordinatorTables(true, options);
      admin.createNamespace(SagaSchema.NAMESPACE, true, options);
      admin.createTable(
          SagaSchema.NAMESPACE,
          SagaSchema.EVENTS_TABLE,
          SagaSchema.sagaEventsTable(),
          true,
          options);
      admin.createTable(
          SagaSchema.NAMESPACE, SagaSchema.STATE_TABLE, SagaSchema.sagaStateTable(), true, options);
      admin.createTable(
          SagaSchema.NAMESPACE,
          SagaSchema.PARKED_TABLE,
          SagaSchema.sagaParkedTable(),
          true,
          options);
      admin.createTable(
          SagaSchema.NAMESPACE,
          SagaSchema.DEFINITIONS_TABLE,
          SagaSchema.sagaDefinitionsTable(),
          true,
          options);
      admin.truncateCoordinatorTables();
      for (String table : SAGA_TABLES) {
        admin.truncateTable(SagaSchema.NAMESPACE, table);
      }
    } catch (Exception e) {
      throw new IllegalStateException("could not reset the external saga store", e);
    }
  }
}
