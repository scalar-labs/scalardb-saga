package com.scalar.db.saga.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.service.TransactionFactory;
import java.util.Objects;
import java.util.Properties;

/**
 * Factory that creates a {@link ScalarDbSagaStore} backed by ScalarDB.
 *
 * <p>The {@link #create(Properties)} method automatically creates the saga tables if they do not
 * already exist (idempotent). Each {@link #createStore()} call creates an independent store with
 * its own ScalarDB transaction manager; the store owns and releases the manager when {@link
 * SagaStore#close()} is called.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty("scalar.db.storage", "jdbc");
 * props.setProperty("scalar.db.contact_points", "jdbc:postgresql://...");
 *
 * DefaultSagaOrchestrator manager = DefaultSagaOrchestrator.newBuilder()
 *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
 *     .build();
 * }</pre>
 *
 * <p>Store-specific properties (all optional):
 *
 * <ul>
 *   <li>{@code scalar.db.saga.store.max_event_payload_bytes} — maximum event payload size in bytes
 *       ({@code 0} = no limit, default: {@code 0})
 *   <li>{@code scalar.db.saga.store.transaction_retry_count} — max transaction retry attempts
 *       (default: {@code 3})
 *   <li>{@code scalar.db.saga.store.num_buckets} — number of state-table bucket partitions
 *       (default: {@code 16})
 * </ul>
 */
public class ScalarDbSagaStoreFactory implements SagaStoreFactory {

  private static final String PROP_PREFIX = "scalar.db.saga.store.";

  private final TransactionFactory transactionFactory;
  private final ScalarDbSagaStoreConfig config;
  private final ObjectMapper objectMapper;

  private ScalarDbSagaStoreFactory(
      TransactionFactory transactionFactory,
      ScalarDbSagaStoreConfig config,
      ObjectMapper objectMapper) {
    this.transactionFactory = transactionFactory;
    this.config = config;
    this.objectMapper = objectMapper;
  }

  /**
   * Creates a factory from properties. ScalarDB connection properties (e.g., {@code
   * scalar.db.storage}, {@code scalar.db.contact_points}) and optional saga store properties (see
   * class Javadoc) are read from the same {@link Properties} object.
   *
   * <p>This method automatically creates the saga tables if they do not already exist (idempotent).
   *
   * @param properties ScalarDB connection and saga store properties
   * @return a new factory instance
   * @throws SagaPersistenceException if schema creation fails
   */
  public static ScalarDbSagaStoreFactory create(Properties properties) {
    Objects.requireNonNull(properties, "properties must not be null");

    ScalarDbSagaStoreConfig config = parseConfig(properties);

    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    createSchema(transactionFactory);
    // Defense in depth against polymorphic-deserialization gadgets (off by default in Jackson 2.x).
    ObjectMapper objectMapper = new ObjectMapper().deactivateDefaultTyping();
    return new ScalarDbSagaStoreFactory(transactionFactory, config, objectMapper);
  }

  @Override
  public SagaStore createStore() {
    SagaSchema schema = new SagaSchema(config.getNumBuckets());
    return new ScalarDbSagaStore(
        transactionFactory.getTransactionManager(), objectMapper, schema, config);
  }

  private static void createSchema(TransactionFactory transactionFactory) {
    try (var admin = transactionFactory.getTransactionAdmin()) {
      admin.createCoordinatorTables(true);
      SagaSchema.createAll(admin);
    } catch (Exception e) {
      throw SagaPersistenceException.storeUnavailable(e);
    }
  }

  private static ScalarDbSagaStoreConfig parseConfig(Properties properties) {
    rejectRemovedKeys(properties);
    ScalarDbSagaStoreConfig.Builder builder = ScalarDbSagaStoreConfig.builder();
    String maxPayload = properties.getProperty(PROP_PREFIX + "max_event_payload_bytes");
    if (maxPayload != null) {
      builder.maxEventPayloadBytes(parseIntProperty("max_event_payload_bytes", maxPayload));
    }
    String retryCount = properties.getProperty(PROP_PREFIX + "transaction_retry_count");
    if (retryCount != null) {
      builder.transactionRetryCount(parseIntProperty("transaction_retry_count", retryCount));
    }
    String numBuckets = properties.getProperty(PROP_PREFIX + "num_buckets");
    if (numBuckets != null) {
      builder.numBuckets(parseIntProperty("num_buckets", numBuckets));
    }
    return builder.build();
  }

  /**
   * Fails on a removed key rather than ignoring it. Nothing else validates this namespace, so a
   * store key that is merely dropped from the parser reads as unset: the deployment starts cleanly
   * and runs on a default the operator believes they overrode.
   */
  private static void rejectRemovedKeys(Properties properties) {
    String scanLimit = PROP_PREFIX + "recovery_scan_limit";
    if (properties.getProperty(scanLimit) != null) {
      throw new IllegalArgumentException(
          "'"
              + scanLimit
              + "' has been removed. It is now an internal page size with no operator meaning."
              + " Delete the key; to spread recovery over more or fewer passes, size"
              + " scalar.db.saga.server.recovery.max_recoveries_per_sweep instead.");
    }
  }

  private static int parseIntProperty(String key, String value) {
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid integer value for property '" + PROP_PREFIX + key + "': " + value, e);
    }
  }
}
