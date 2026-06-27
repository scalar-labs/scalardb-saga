package com.scalar.db.saga.store;

/**
 * Factory for creating {@link SagaStore} instances.
 *
 * <p>The created store owns the lifecycle of underlying resources (e.g., connection pools,
 * transaction managers) and releases them when {@link SagaStore#close()} is called.
 *
 * <p>This is a {@linkplain FunctionalInterface functional interface} — for testing, a lambda
 * returning a mock store can be used directly:
 *
 * <pre>{@code
 * DefaultSagaOrchestrator manager = DefaultSagaOrchestrator.newBuilder()
 *     .storeFactory(() -> mockStore)
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface SagaStoreFactory {

  /**
   * Creates a new {@link SagaStore} instance.
   *
   * @return the store instance
   */
  SagaStore createStore();
}
