package com.scalar.db.saga.transport;

/**
 * Resolves a declarative step's service name to the {@link TransportAdapter} currently registered
 * for it. This is the late-binding seam: a declarative step holds its service NAME and resolves it
 * once per phase call ({@code execute}/{@code compensate}, {@code reserve}/{@code confirm}/{@code
 * cancel}), so a configuration swap changes what the next resolution sees while a call already in
 * flight finishes against the adapter it resolved.
 *
 * <p>Defined in this package (not {@code engine}) so the engine depends on transport, never the
 * reverse.
 */
@FunctionalInterface
public interface TransportResolver {

  /**
   * Returns the adapter currently registered under {@code service}.
   *
   * @throws TransportException if no endpoint is currently registered under {@code service} —
   *     retryable and known-not-committed: the miss is proven pre-send, and when it comes from a
   *     replica whose configuration lags a removal or rename, a later resolution succeeds once the
   *     configuration propagates
   */
  TransportAdapter resolve(String service) throws TransportException;
}
