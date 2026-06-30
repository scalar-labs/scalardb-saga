package com.scalar.db.saga.grpc;

import com.google.protobuf.Timestamp;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.rpc.SagaSnapshot;
import java.time.Instant;

/**
 * Converts the generated {@code rpc} wire types back to api value types — the inverse of the
 * daemon's server-side mapper. The generated {@code com.scalar.db.saga.rpc} types are imported only
 * here and in {@link GrpcSagaOrchestratorClient}, never past this client boundary.
 */
final class ClientProtoMappers {

  private static final String STATUS_PREFIX = "SAGA_STATUS_";

  private ClientProtoMappers() {}

  /**
   * Maps a wire snapshot to an api snapshot. {@code ownerId} is set to the empty string: it is a
   * server-internal recovery-coordination field that the remote API deliberately does not surface
   * (parity with the REST DTO), so a remote snapshot never carries a real owner.
   */
  static SagaStateSnapshot toApi(SagaSnapshot snapshot) {
    return new SagaStateSnapshot(
        snapshot.getSagaId(),
        snapshot.getName(),
        toApiStatus(snapshot.getStatus()),
        "",
        snapshot.getDefinitionVersion(),
        toInstant(snapshot.getCreatedAt()),
        toInstant(snapshot.getUpdatedAt()));
  }

  /**
   * Maps the wire status to the api status <b>by name</b> ({@code SAGA_STATUS_RUNNING} → {@code
   * RUNNING}) — the inverse of the server's by-name mapping. Rejects {@code
   * SAGA_STATUS_UNSPECIFIED} and the proto3 {@code UNRECOGNIZED} sentinel loudly rather than
   * degrading silently.
   */
  static SagaStatus toApiStatus(com.scalar.db.saga.rpc.SagaStatus status) {
    String name = status.name();
    if (!name.startsWith(STATUS_PREFIX) || name.equals(STATUS_PREFIX + "UNSPECIFIED")) {
      throw new SagaRuntimeException("Unexpected wire saga status: " + name);
    }
    try {
      return SagaStatus.valueOf(name.substring(STATUS_PREFIX.length()));
    } catch (IllegalArgumentException e) {
      throw new SagaRuntimeException("Unexpected wire saga status: " + name, e);
    }
  }

  private static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
