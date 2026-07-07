package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.CallSpec;

/**
 * Executes one direction of a declaratively-defined service step (Layer 2b): given a {@link
 * CallSpec} and the saga context, it resolves the {@code ${key}} request/addressing templates,
 * performs the remote call, and returns the response fields extracted by the spec's {@code output}
 * mapping.
 *
 * <p>This is the transport-pluggable seam: the built-in {@code HttpTransportAdapter} handles {@link
 * CallSpec.Transport#HTTP}; a gRPC adapter is added in Task 2.1b. An adapter handles exactly the
 * transport(s) it was registered for and rejects any other {@link CallSpec#transport()}.
 */
public interface TransportAdapter {

  /**
   * Resolves and performs {@code spec} against the registered service endpoint, propagating the
   * saga correlation context ({@code X-Saga-Id} from {@link SagaContext#getSagaId()}, {@code
   * X-Saga-Step} from {@code stepName}).
   *
   * @param spec the call to perform
   * @param context the saga context for {@code ${key}} resolution and the saga id
   * @param stepName the step name (for the correlation header / dedup key)
   * @return the call outcome: {@link StepResult#of} carrying the extracted output fields (per
   *     {@code spec}'s output mapping; empty if none), or {@link StepResult#pending()} when an
   *     async step's participant accepted the work and will complete it later via a callback
   * @throws TransportException on a non-retryable definition/contract error or a (possibly
   *     retryable) transport failure
   */
  StepResult call(CallSpec spec, SagaContext context, String stepName) throws TransportException;
}
