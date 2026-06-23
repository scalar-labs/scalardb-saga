package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.CallSpec;
import com.scalar.db.saga.api.SagaContext;
import java.util.Map;

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
   * @return the extracted output fields (per {@code spec}'s output mapping), ready to merge into
   *     the context; empty if the spec has no output mapping
   * @throws TransportException on a non-retryable definition/contract error or a (possibly
   *     retryable) transport failure
   */
  Map<String, Object> call(CallSpec spec, SagaContext context, String stepName)
      throws TransportException;
}
