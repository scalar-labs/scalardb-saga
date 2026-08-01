package com.scalar.db.saga.server.grpc;

import com.scalar.db.saga.server.security.SagaOperation;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Maps a gRPC method onto the {@link SagaOperation} that governs it — the gRPC transport's half of
 * the operation mapping, mirroring the per-route tags the REST transport declares. The policy
 * itself (required role, rate-limit treatment) lives on {@link SagaOperation}; only the mapping is
 * per transport.
 *
 * <p>Keyed on the <b>bare</b> method name rather than the full {@code package.Service/Method} path.
 * A method absent from the map is refused by its interceptor rather than defaulted to a role: an
 * unmapped method means an RPC was added without a policy, and gRPC's own dispatch means it can
 * only be reached if it was registered. {@code TransportPolicyParityTest} enumerates the generated
 * service descriptors and fails the build on any method missing here, so the runtime refusal is a
 * backstop rather than the first line of defence.
 */
final class GrpcOperations {

  private static final Map<String, SagaOperation> BY_BARE_METHOD_NAME =
      Map.ofEntries(
          Map.entry("StartSaga", SagaOperation.START_SAGA),
          Map.entry("GetSaga", SagaOperation.GET_SAGA),
          Map.entry("AwaitSaga", SagaOperation.AWAIT_SAGA),
          Map.entry("GetSagaDetail", SagaOperation.GET_SAGA_DETAIL),
          Map.entry("ListSagas", SagaOperation.LIST_SAGAS),
          Map.entry("RecoverSaga", SagaOperation.RECOVER_SAGA),
          Map.entry("ForceComplete", SagaOperation.FORCE_COMPLETE),
          Map.entry("ResetEscalated", SagaOperation.RESET_ESCALATED),
          Map.entry("ResetEscalatedBulk", SagaOperation.RESET_ESCALATED));

  private GrpcOperations() {}

  /**
   * Returns the operation governing {@code bareMethodName}, or {@code null} if the method has no
   * mapped policy (including a {@code null} name). Callers must refuse a {@code null} result rather
   * than defaulting it to a role.
   */
  static @Nullable SagaOperation fromBareMethodName(@Nullable String bareMethodName) {
    return bareMethodName == null ? null : BY_BARE_METHOD_NAME.get(bareMethodName);
  }

  /**
   * Returns the bare method names carrying a policy. Used by the parity test's completeness check.
   */
  static Iterable<String> mappedMethodNames() {
    return BY_BARE_METHOD_NAME.keySet();
  }
}
