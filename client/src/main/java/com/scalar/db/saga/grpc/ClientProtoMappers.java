package com.scalar.db.saga.grpc;

import com.google.protobuf.Timestamp;
import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.rpc.ListSagasRequest;
import com.scalar.db.saga.rpc.ListSagasResponse;
import com.scalar.db.saga.rpc.ResetEscalatedBulkRequest;
import com.scalar.db.saga.rpc.SagaSnapshot;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Converts between api value types and the generated {@code rpc} wire types — the client-side
 * mirror of the daemon's server-side mapper. Requests carry api types out to the wire ({@code
 * toXxx}); responses carry wire types back to api types ({@code fromProto}). The generated {@code
 * com.scalar.db.saga.rpc} types are imported only here and in the client classes, never past this
 * boundary.
 */
final class ClientProtoMappers {

  private static final String STATUS_PREFIX = "SAGA_STATUS_";
  private static final String SKIP_REASON_PREFIX = "SKIP_REASON_";

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
      throw new SagaRuntimeException(
          SagaErrorCode.UNRECOGNIZED_SERVER_ERROR, ErrorMetadata.of("server_value", name));
    }
    try {
      return SagaStatus.valueOf(name.substring(STATUS_PREFIX.length()));
    } catch (IllegalArgumentException e) {
      throw new SagaRuntimeException(
          SagaErrorCode.UNRECOGNIZED_SERVER_ERROR, ErrorMetadata.of("server_value", name), e);
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

  /**
   * Marshals a query to a {@code ListSagas} request. A null query field is left unset on the wire.
   */
  static ListSagasRequest toListSagasRequest(SagaQuery query) {
    ListSagasRequest.Builder builder = ListSagasRequest.newBuilder();
    SagaStatus status = query.getStatus();
    if (status != null) {
      builder.setStatus(toProtoStatus(status));
    }
    Instant updatedAfter = query.getUpdatedAfter();
    if (updatedAfter != null) {
      builder.setUpdatedAfter(toTimestamp(updatedAfter));
    }
    Instant updatedBefore = query.getUpdatedBefore();
    if (updatedBefore != null) {
      builder.setUpdatedBefore(toTimestamp(updatedBefore));
    }
    builder.setPageSize(query.getPageSize());
    String pageToken = query.getPageToken();
    if (pageToken != null) {
      builder.setPageToken(pageToken);
    }
    return builder.build();
  }

  /**
   * Marshals a sweep window plus the operator reason to a bulk-reset request. The query's status
   * filter is not sent — the sweep is defined as escalated sagas, which the server pins.
   */
  static ResetEscalatedBulkRequest toResetEscalatedBulkRequest(SagaQuery query, String reason) {
    ResetEscalatedBulkRequest.Builder builder =
        ResetEscalatedBulkRequest.newBuilder().setReason(reason);
    Instant updatedAfter = query.getUpdatedAfter();
    if (updatedAfter != null) {
      builder.setUpdatedAfter(toTimestamp(updatedAfter));
    }
    Instant updatedBefore = query.getUpdatedBefore();
    if (updatedBefore != null) {
      builder.setUpdatedBefore(toTimestamp(updatedBefore));
    }
    builder.setPageSize(query.getPageSize());
    String pageToken = query.getPageToken();
    if (pageToken != null) {
      builder.setPageToken(pageToken);
    }
    return builder.build();
  }

  /**
   * Maps the api status to the wire status by name (inverse of {@link #fromProtoStatus}). A missing
   * wire counterpart is api/proto version skew — an internal bug, since {@link SagaStatus} is a
   * Java enum the caller cannot fabricate — so it throws {@link IllegalStateException} rather than
   * degrading silently.
   */
  static com.scalar.db.saga.rpc.SagaStatus toProtoStatus(SagaStatus status) {
    try {
      return com.scalar.db.saga.rpc.SagaStatus.valueOf(STATUS_PREFIX + status.name());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("No wire SagaStatus for api status " + status.name(), e);
    }
  }

  /** Maps a wire list response back to an api page of snapshots. */
  static SagaPage<SagaStateSnapshot> fromProto(ListSagasResponse response) {
    List<SagaStateSnapshot> items = new ArrayList<>(response.getSagasCount());
    for (SagaSnapshot snapshot : response.getSagasList()) {
      items.add(fromProto(snapshot));
    }
    @Nullable String nextPageToken =
        response.hasNextPageToken() ? response.getNextPageToken() : null;
    return new SagaPage<>(items, nextPageToken);
  }

  /** Maps a wire bulk-reset result (count + itemized skips + token) back to the api result. */
  static ResetResult fromProto(com.scalar.db.saga.rpc.ResetResult result) {
    List<ResetResult.SkippedSaga> skipped = new ArrayList<>(result.getSkippedCount());
    for (com.scalar.db.saga.rpc.SkippedSaga wire : result.getSkippedList()) {
      @Nullable String detail = wire.hasDetail() ? wire.getDetail() : null;
      skipped.add(
          new ResetResult.SkippedSaga(
              wire.getSagaId(), fromProtoSkipReason(wire.getReason()), detail));
    }
    @Nullable String nextPageToken = result.hasNextPageToken() ? result.getNextPageToken() : null;
    return new ResetResult(result.getResetCount(), skipped, nextPageToken);
  }

  /**
   * Maps the wire skip reason to the api reason by name, the inverse of the server's mapping.
   * Rejects the proto3-reserved {@code SKIP_REASON_UNSPECIFIED} and {@code UNRECOGNIZED} loudly.
   */
  static ResetResult.SkipReason fromProtoSkipReason(com.scalar.db.saga.rpc.SkipReason reason) {
    String name = reason.name();
    if (!name.startsWith(SKIP_REASON_PREFIX) || name.equals(SKIP_REASON_PREFIX + "UNSPECIFIED")) {
      throw new SagaRuntimeException(
          SagaErrorCode.UNRECOGNIZED_SERVER_ERROR, ErrorMetadata.of("server_value", name));
    }
    try {
      return ResetResult.SkipReason.valueOf(name.substring(SKIP_REASON_PREFIX.length()));
    } catch (IllegalArgumentException e) {
      throw new SagaRuntimeException(
          SagaErrorCode.UNRECOGNIZED_SERVER_ERROR, ErrorMetadata.of("server_value", name), e);
    }
  }

  private static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  private static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
