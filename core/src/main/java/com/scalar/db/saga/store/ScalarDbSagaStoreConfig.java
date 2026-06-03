package com.scalar.db.saga.store;

/** Configuration options for {@link ScalarDbSagaStore}. */
class ScalarDbSagaStoreConfig {

  private static final int DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 0;
  private static final int DEFAULT_TRANSACTION_RETRY_COUNT = 3;
  private static final int DEFAULT_RECOVERY_SCAN_LIMIT = 100;

  private final int maxEventPayloadBytes;
  private final int transactionRetryCount;
  private final int recoveryScanLimit;
  private final int numBuckets;

  private ScalarDbSagaStoreConfig(Builder builder) {
    this.maxEventPayloadBytes = builder.maxEventPayloadBytes;
    this.transactionRetryCount = builder.transactionRetryCount;
    this.recoveryScanLimit = builder.recoveryScanLimit;
    this.numBuckets = builder.numBuckets;
  }

  /**
   * Returns the maximum event payload size in bytes. A value of {@code 0} means no limit.
   *
   * @return the maximum payload size
   */
  public int getMaxEventPayloadBytes() {
    return maxEventPayloadBytes;
  }

  /**
   * Returns the maximum number of transaction retry attempts for retryable failures (conflict
   * exceptions and unknown transaction status).
   *
   * @return the transaction retry count
   */
  public int getTransactionRetryCount() {
    return transactionRetryCount;
  }

  /**
   * Returns the maximum number of rows returned per status scan in {@code findRecoverable}. Any
   * sagas beyond this limit are picked up on the next recovery cycle.
   *
   * <p>This per-bucket cap works together with {@link
   * com.scalar.db.saga.api.RecoveryConfig#batchSize()} (total cap per pass) to ensure fair
   * distribution across buckets. Keep this value smaller than {@code batchSize /
   * numRecoverableStatuses} so that a single hot bucket cannot consume the entire batch budget.
   *
   * @return the recovery scan limit
   */
  public int getRecoveryScanLimit() {
    return recoveryScanLimit;
  }

  /**
   * Returns the number of bucket partitions for the {@code saga_state} table. Must remain constant
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
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ScalarDbSagaStoreConfig}. */
  static class Builder {
    private int maxEventPayloadBytes = DEFAULT_MAX_EVENT_PAYLOAD_BYTES;
    private int transactionRetryCount = DEFAULT_TRANSACTION_RETRY_COUNT;
    private int recoveryScanLimit = DEFAULT_RECOVERY_SCAN_LIMIT;
    private int numBuckets = SagaSchema.DEFAULT_NUM_BUCKETS;

    private Builder() {}

    /**
     * Sets the maximum event payload size in bytes. A value of {@code 0} means no limit.
     *
     * @param maxEventPayloadBytes the maximum payload size
     * @return this builder
     */
    public Builder maxEventPayloadBytes(int maxEventPayloadBytes) {
      if (maxEventPayloadBytes < 0) {
        throw new IllegalArgumentException("maxEventPayloadBytes must be >= 0");
      }
      this.maxEventPayloadBytes = maxEventPayloadBytes;
      return this;
    }

    /**
     * Sets the maximum number of transaction retry attempts for retryable failures.
     *
     * @param transactionRetryCount the retry count (must be &gt;= 1)
     * @return this builder
     */
    public Builder transactionRetryCount(int transactionRetryCount) {
      if (transactionRetryCount < 1) {
        throw new IllegalArgumentException("transactionRetryCount must be >= 1");
      }
      this.transactionRetryCount = transactionRetryCount;
      return this;
    }

    /**
     * Sets the maximum number of rows returned per status scan in {@code findRecoverable}. Any
     * sagas beyond this limit are picked up on the next recovery cycle.
     *
     * <p>See {@link ScalarDbSagaStoreConfig#getRecoveryScanLimit()} for how this interacts with
     * {@link com.scalar.db.saga.api.RecoveryConfig#batchSize()}.
     *
     * @param recoveryScanLimit the scan limit (must be &gt;= 1)
     * @return this builder
     */
    public Builder recoveryScanLimit(int recoveryScanLimit) {
      if (recoveryScanLimit < 1) {
        throw new IllegalArgumentException("recoveryScanLimit must be >= 1");
      }
      this.recoveryScanLimit = recoveryScanLimit;
      return this;
    }

    /**
     * Sets the number of bucket partitions for the {@code saga_state} table. Must remain constant
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
