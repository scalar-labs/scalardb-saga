package com.scalar.db.saga.store;

import java.time.Duration;
import java.util.Objects;

/** Configuration options for {@link ScalarDbSagaStore}. */
public class ScalarDbSagaStoreConfig {

  private static final int DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 0;
  private static final int DEFAULT_CACHE_MAX_SIZE = 1000;
  private static final Duration DEFAULT_CACHE_EXPIRE_AFTER_WRITE = Duration.ofMinutes(5);
  private static final int DEFAULT_TRANSACTION_RETRY_COUNT = 3;
  private static final int DEFAULT_RECOVERY_SCAN_LIMIT = 1000;

  private final int maxEventPayloadBytes;
  private final int cacheMaxSize;
  private final Duration cacheExpireAfterWrite;
  private final int transactionRetryCount;
  private final int recoveryScanLimit;

  private ScalarDbSagaStoreConfig(Builder builder) {
    this.maxEventPayloadBytes = builder.maxEventPayloadBytes;
    this.cacheMaxSize = builder.cacheMaxSize;
    this.cacheExpireAfterWrite = builder.cacheExpireAfterWrite;
    this.transactionRetryCount = builder.transactionRetryCount;
    this.recoveryScanLimit = builder.recoveryScanLimit;
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
   * Returns the maximum number of cached state snapshots.
   *
   * @return the cache maximum size
   */
  public int getCacheMaxSize() {
    return cacheMaxSize;
  }

  /**
   * Returns the cache entry TTL.
   *
   * @return the cache expiration duration
   */
  public Duration getCacheExpireAfterWrite() {
    return cacheExpireAfterWrite;
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
   * @return the recovery scan limit
   */
  public int getRecoveryScanLimit() {
    return recoveryScanLimit;
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
  public static class Builder {
    private int maxEventPayloadBytes = DEFAULT_MAX_EVENT_PAYLOAD_BYTES;
    private int cacheMaxSize = DEFAULT_CACHE_MAX_SIZE;
    private Duration cacheExpireAfterWrite = DEFAULT_CACHE_EXPIRE_AFTER_WRITE;
    private int transactionRetryCount = DEFAULT_TRANSACTION_RETRY_COUNT;
    private int recoveryScanLimit = DEFAULT_RECOVERY_SCAN_LIMIT;

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
     * Sets the maximum number of cached state snapshots.
     *
     * @param cacheMaxSize the cache maximum size
     * @return this builder
     */
    public Builder cacheMaxSize(int cacheMaxSize) {
      if (cacheMaxSize < 0) {
        throw new IllegalArgumentException("cacheMaxSize must be >= 0");
      }
      this.cacheMaxSize = cacheMaxSize;
      return this;
    }

    /**
     * Sets the cache entry TTL.
     *
     * @param cacheExpireAfterWrite the cache expiration duration
     * @return this builder
     */
    public Builder cacheExpireAfterWrite(Duration cacheExpireAfterWrite) {
      Objects.requireNonNull(cacheExpireAfterWrite, "cacheExpireAfterWrite must not be null");
      if (cacheExpireAfterWrite.isZero() || cacheExpireAfterWrite.isNegative()) {
        throw new IllegalArgumentException("cacheExpireAfterWrite must be positive");
      }
      this.cacheExpireAfterWrite = cacheExpireAfterWrite;
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
     * Builds the configuration.
     *
     * @return the configuration
     */
    public ScalarDbSagaStoreConfig build() {
      return new ScalarDbSagaStoreConfig(this);
    }
  }
}
