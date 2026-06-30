package com.scalar.db.saga.daemon.grpc;

import com.google.protobuf.Timestamp;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;

/**
 * Converts api value types to their generated {@code rpc} wire types. These conversions live only
 * here, at the server adapter boundary — the generated {@code com.scalar.db.saga.rpc} types never
 * leak into the engine or api.
 */
final class ProtoMappers {

  private ProtoMappers() {}

  /**
   * Maps an api snapshot to the wire snapshot. {@code ownerId} is deliberately dropped — it is a
   * server-internal recovery-coordination field, not surfaced over the remote API (parity with the
   * REST {@code SagaSnapshotResponse}).
   */
  static com.scalar.db.saga.rpc.SagaSnapshot toProto(SagaStateSnapshot snapshot) {
    return com.scalar.db.saga.rpc.SagaSnapshot.newBuilder()
        .setSagaId(snapshot.getSagaId())
        .setName(snapshot.getSagaName())
        .setStatus(toProtoStatus(snapshot.getStatus()))
        .setDefinitionVersion(snapshot.getDefinitionVersion())
        .setCreatedAt(toTimestamp(snapshot.getCreatedAt()))
        .setUpdatedAt(toTimestamp(snapshot.getUpdatedAt()))
        .build();
  }

  /**
   * Maps the api status to the wire status <b>by name</b> ({@code RUNNING} → {@code
   * SAGA_STATUS_RUNNING}). The numeric codes deliberately differ (the api {@code RUNNING=0}
   * collides with proto3's reserved zero {@code _UNSPECIFIED}), so name mapping is the contract.
   * {@link com.scalar.db.saga.rpc.SagaStatus#valueOf} throws if an api status has no wire
   * counterpart — failing loudly rather than silently degrading to {@code UNSPECIFIED}.
   */
  static com.scalar.db.saga.rpc.SagaStatus toProtoStatus(SagaStatus status) {
    return com.scalar.db.saga.rpc.SagaStatus.valueOf("SAGA_STATUS_" + status.name());
  }

  private static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
