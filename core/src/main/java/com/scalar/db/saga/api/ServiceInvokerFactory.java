package com.scalar.db.saga.api;

/**
 * Factory for creating {@link ServiceInvoker} instances.
 *
 * <p>The {@link SagaManager} built from a builder invokes this factory once, then owns the
 * resulting invoker and {@linkplain ServiceInvoker#close() closes} it (releasing resources such as
 * HTTP clients) when {@link SagaManager#close()} is called — mirroring how {@link SagaStoreFactory}
 * works for the store.
 *
 * <p><b>Create a new invoker inside the factory.</b> Do not return a shared or pre-built instance
 * you intend to keep using or register with another manager — the manager closes whatever the
 * factory returns, so a shared instance would be closed out from under you.
 *
 * <p>This is a {@linkplain FunctionalInterface functional interface}:
 *
 * <pre>{@code
 * SagaManager.newBuilder()
 *     .storeFactory(...)
 *     .serviceInvokerFactory("account", () -> HttpServiceInvoker.newBuilder("http://acct:8080")
 *         .operation("debit").execution(exec).compensation(comp).add()
 *         .build())
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface ServiceInvokerFactory {

  /**
   * Creates a new {@link ServiceInvoker} instance.
   *
   * @return the invoker instance
   */
  ServiceInvoker createServiceInvoker();
}
