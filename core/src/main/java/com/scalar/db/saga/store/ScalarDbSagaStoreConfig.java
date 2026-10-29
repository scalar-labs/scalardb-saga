package com.scalar.db.saga.store;

/** Configuration options for {@link ScalarDbSagaStore}. */
class ScalarDbSagaStoreConfig {

  private static final int DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 0;
  private static final int DEFAULT_TRANSACTION_MAX_ATTEMPTS = 3;
  // Deliberately not operator-configurable. This is a page size, and no operator-visible signal
  // says it needs changing or in which direction. If a storage backend ever needs a different
  // value, adding a property back is compatible; removing one is not.
  //
  // Raising this is not free: it raises the minimum safe recovery budget with it, because a page
  // holds one status after another and a sweep stops submitting once its budget runs out. See
  // getRecoveryScanLimit below. Within-bucket paging removes the coupling; until then, leave it.
  private static final int DEFAULT_RECOVERY_SCAN_LIMIT = 100;

  private final int maxEventPayloadBytes;
  private final int transactionMaxAttempts;
  private final int recoveryScanLimit;
  private final int numBuckets;

  private ScalarDbSagaStoreConfig(Builder builder) {
    this.maxEventPayloadBytes = builder.maxEventPayloadBytes;
    this.transactionMaxAttempts = builder.transactionMaxAttempts;
    this.recoveryScanLimit = builder.recoveryScanLimit;
    this.numBuckets = builder.numBuckets;
  }

  /**
   * Returns the maximum event payload size in bytes. A value of {@code 0} means no limit.
   *
   * @return the maximum payload size
   */
  int getMaxEventPayloadBytes() {
    return maxEventPayloadBytes;
  }

  /**
   * Returns how many times a transaction is attempted, including the first, before it fails on a
   * retryable failure (a conflict, an unknown transaction status, or another transaction failure).
   *
   * @return the transaction attempt count
   */
  int getTransactionMaxAttempts() {
    return transactionMaxAttempts;
  }

  /**
   * Returns the maximum number of rows returned per status scan in {@code findRecoverable}. Any
   * sagas beyond this limit are picked up on the next recovery cycle.
   *
   * <p>{@link com.scalar.db.saga.engine.RecoveryConfig#maxRecoveriesPerSweep()} bounds this from
   * above: it must not exceed {@code maxRecoveriesPerSweep / numRecoverableStatuses}. A page holds
   * every recoverable status one after another, and the sweep stops submitting the moment its
   * budget runs out, so a smaller budget truncates the page and the truncation always falls on the
   * trailing status. With RUNNING first and COMPENSATING second, a budget at or below this limit
   * never recovers compensating sagas at all; above it but below a full page they are served, but
   * throttled behind running ones.
   *
   * @return the recovery scan limit
   */
  int getRecoveryScanLimit() {
    return recoveryScanLimit;
  }

  /**
   * Returns the number of bucket partitions for the {@code saga_sweep} table. Must remain constant
   * once data has been written.
   *
   * @return the number of buckets
   */
  int getNumBuckets() {
    return numBuckets;
  }

  /**
   * Creates a new builder with default values.
   *
   * @return a new builder
   */
  static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ScalarDbSagaStoreConfig}. */
  static class Builder {
    private int maxEventPayloadBytes = DEFAULT_MAX_EVENT_PAYLOAD_BYTES;
    private int transactionMaxAttempts = DEFAULT_TRANSACTION_MAX_ATTEMPTS;
    private int recoveryScanLimit = DEFAULT_RECOVERY_SCAN_LIMIT;
    private int numBuckets = SagaSchema.DEFAULT_NUM_BUCKETS;

    private Builder() {}

    /**
     * Sets the maximum event payload size in bytes. A value of {@code 0} means no limit.
     *
     * @param maxEventPayloadBytes the maximum payload size
     * @return this builder
     */
    Builder maxEventPayloadBytes(int maxEventPayloadBytes) {
      if (maxEventPayloadBytes < 0) {
        throw new IllegalArgumentException("maxEventPayloadBytes must be >= 0");
      }
      this.maxEventPayloadBytes = maxEventPayloadBytes;
      return this;
    }

    /**
     * Sets how many times a transaction is attempted, including the first, before it fails.
     *
     * @param transactionMaxAttempts the attempt count (must be &gt;= 1)
     * @return this builder
     */
    Builder transactionMaxAttempts(int transactionMaxAttempts) {
      if (transactionMaxAttempts < 1) {
        throw new IllegalArgumentException("transactionMaxAttempts must be >= 1");
      }
      this.transactionMaxAttempts = transactionMaxAttempts;
      return this;
    }

    /**
     * Sets the maximum number of rows returned per status scan in {@code findRecoverable}. Any
     * sagas beyond this limit are picked up on the next recovery cycle.
     *
     * <p>Not settable from configuration properties — see the field comment on the default. See
     * {@link ScalarDbSagaStoreConfig#getRecoveryScanLimit()} for how it interacts with {@link
     * com.scalar.db.saga.engine.RecoveryConfig#maxRecoveriesPerSweep()}.
     *
     * @param recoveryScanLimit the scan limit (must be &gt;= 1)
     * @return this builder
     */
    Builder recoveryScanLimit(int recoveryScanLimit) {
      if (recoveryScanLimit < 1) {
        throw new IllegalArgumentException("recoveryScanLimit must be >= 1");
      }
      this.recoveryScanLimit = recoveryScanLimit;
      return this;
    }

    /**
     * Sets the number of bucket partitions for the {@code saga_sweep} table. Must remain constant
     * once data has been written. Defaults to {@link SagaSchema#DEFAULT_NUM_BUCKETS}.
     *
     * @param numBuckets the number of buckets (must be &gt; 0)
     * @return this builder
     */
    Builder numBuckets(int numBuckets) {
      if (numBuckets <= 0) {
        throw new IllegalArgumentException("numBuckets must be > 0, got " + numBuckets);
      }
      this.numBuckets = numBuckets;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return the configuration
     */
    ScalarDbSagaStoreConfig build() {
      return new ScalarDbSagaStoreConfig(this);
    }
  }
}
