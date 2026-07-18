package com.scalar.db.saga.grpc;

import com.google.protobuf.Timestamp;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.rpc.SagaSnapshot;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

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
  static SagaStateSnapshot fromProto(SagaSnapshot snapshot) {
    return new SagaStateSnapshot(
        snapshot.getSagaId(),
        snapshot.getName(),
        fromProtoStatus(snapshot.getStatus()),
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
  static SagaStatus fromProtoStatus(com.scalar.db.saga.rpc.SagaStatus status) {
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

  /** Maps a wire detail (snapshot + timeline) back to an api detail. */
  static SagaDetail fromProto(com.scalar.db.saga.rpc.SagaDetail detail) {
    List<TimelineEvent> timeline = new ArrayList<>(detail.getTimelineCount());
    for (com.scalar.db.saga.rpc.TimelineEvent event : detail.getTimelineList()) {
      timeline.add(fromProto(event));
    }
    return new SagaDetail(fromProto(detail.getSaga()), timeline);
  }

  /**
   * Maps one wire timeline event back to an api event. A proto3 {@code optional} that is unset maps
   * to a {@code null} api field, the inverse of the server mapper's set-only-when-present.
   */
  static TimelineEvent fromProto(com.scalar.db.saga.rpc.TimelineEvent event) {
    @Nullable Integer stepIndex = event.hasStepIndex() ? event.getStepIndex() : null;
    @Nullable String stepName = event.hasStepName() ? event.getStepName() : null;
    @Nullable SagaStatus resultingStatus =
        event.hasResultingStatus() ? fromProtoStatus(event.getResultingStatus()) : null;
    @Nullable String detail = event.hasDetail() ? event.getDetail() : null;
    @Nullable String operator = event.hasOperator() ? event.getOperator() : null;
    return new TimelineEvent(
        toInstant(event.getTimestamp()),
        event.getType(),
        stepIndex,
        stepName,
        resultingStatus,
        detail,
        operator);
  }

  private static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
