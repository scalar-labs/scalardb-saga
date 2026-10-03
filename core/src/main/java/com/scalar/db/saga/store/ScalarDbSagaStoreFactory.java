package com.scalar.db.saga.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.service.TransactionFactory;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger logger = LoggerFactory.getLogger(ScalarDbSagaStoreFactory.class);

  private static final String PROP_PREFIX = "scalar.db.saga.store.";

  /**
   * Boot-time schema creation: ten attempts, pausing from about half a second and doubling to a
   * 15-second ceiling, roughly a minute of pauses in all. See {@link #createSchema} for why.
   */
  static final RetryPolicy SCHEMA_CREATE_RETRY =
      RetryPolicy.newBuilder()
          .maxAttempts(10)
          .initialIntervalMillis(500)
          .backoffMultiplier(2.0)
          .maxIntervalMillis(15_000)
          .build();

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
    createSchema(transactionFactory::getTransactionAdmin, SCHEMA_CREATE_RETRY);
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

  /**
   * Creates the coordinator and saga schema, retrying when a sibling replica gets there first.
   *
   * <p>ScalarDB's {@code ifNotExists} admin calls check and then create as two separate steps, so
   * replicas booting together can both pass the check, and the slower one fails with "already
   * exists" from ScalarDB, or with the storage's own duplicate-DDL error one level down. Every step
   * here is idempotent, so a rerun once the winner has finished short-circuits cleanly. Only that
   * family of failures is expected, but nothing distinguishes it reliably from the storage's
   * wrapped errors, so every failure is retried; the bound keeps a store that is really down from
   * hanging the boot. A retried attempt is the expected outcome of every multi-replica first boot,
   * so it logs its cause at INFO with the stack at DEBUG; the last failure is the cause of the
   * thrown exception, which the daemon reports at ERROR, so an outage stays visible either way.
   *
   * <p>The pauses back off with jitter so replicas that failed together do not retry together:
   * losers that wake in the same instant race each other on the tables the winner has not reached
   * yet, and one that grabs a table fails the winner's pass as well. {@link #SCHEMA_CREATE_RETRY}
   * is sized for backends whose table creation blocks until the table is ready, DynamoDB above all,
   * where a six-table pass keeps the winner busy for the better part of a minute.
   *
   * <p>Package-private, taking the admin as a supplier and the policy as a parameter, so a test can
   * drive it with a mocked admin and no pause: ScalarDB's {@code TransactionFactory} is final and
   * needs a live database.
   *
   * @param admins produces a fresh admin per attempt; each is closed after use
   * @param retry how many attempts to make, and how long to pause between them
   * @throws SagaPersistenceException if every attempt fails, or a pause is interrupted
   */
  static void createSchema(Supplier<DistributedTransactionAdmin> admins, RetryPolicy retry) {
    long interval = retry.getInitialIntervalMillis();
    for (int attempt = 1; ; attempt++) {
      Exception failure;
      try (DistributedTransactionAdmin admin = admins.get()) {
        admin.createCoordinatorTables(true);
        SagaSchema.createAll(admin);
        return;
      } catch (Exception e) {
        failure = e;
      }
      if (attempt >= retry.getMaxAttempts()) {
        throw SagaPersistenceException.storeUnavailable(failure);
      }
      logger.info(
          "Creating the saga schema failed (attempt {} of {}); retrying within {} ms: {}",
          attempt,
          retry.getMaxAttempts(),
          interval,
          failure.toString());
      logger.debug("Schema creation failure detail", failure);
      try {
        interval = retry.sleepWithBackoff(interval);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw SagaPersistenceException.operationAborted(e);
      }
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
              + "' has been removed; it is now an internal page size. Delete the key. To pace"
              + " recovery, size RecoveryConfig.maxRecoveriesPerSweep instead (daemon key:"
              + " scalar.db.saga.server.recovery.max_recoveries_per_sweep).");
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
