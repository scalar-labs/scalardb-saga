package com.scalar.db.saga.daemon.grpc;

import com.google.protobuf.Timestamp;
import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.rpc.ListSagasRequest;
import com.scalar.db.saga.rpc.ListSagasResponse;
import com.scalar.db.saga.rpc.ResetEscalatedBulkRequest;
import java.time.DateTimeException;
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
    try {
      return com.scalar.db.saga.rpc.SagaStatus.valueOf("SAGA_STATUS_" + status.name());
    } catch (IllegalArgumentException e) {
      // No wire counterpart for a server-internal status is api/proto version skew, a server fault.
      // Throw IllegalStateException so the error mapper reports INTERNAL, not the client-facing
      // INVALID_ARGUMENT (this is a response-path conversion, never client input).
      throw new IllegalStateException("No wire SagaStatus for api status " + status.name(), e);
    }
  }

  /** Maps an api detail (snapshot + redacted timeline) to the wire detail. */
  static com.scalar.db.saga.rpc.SagaDetail toProto(SagaDetail detail) {
    com.scalar.db.saga.rpc.SagaDetail.Builder builder =
        com.scalar.db.saga.rpc.SagaDetail.newBuilder().setSaga(toProto(detail.getSnapshot()));
    for (TimelineEvent event : detail.getTimeline()) {
      builder.addTimeline(toProto(event));
    }
    return builder.build();
  }

  /**
   * Maps one api timeline event to the wire event. The nullable fields are set only when present,
   * so a {@code null} api field round-trips as an unset proto3 {@code optional} rather than a
   * defaulted value.
   */
  static com.scalar.db.saga.rpc.TimelineEvent toProto(TimelineEvent event) {
    com.scalar.db.saga.rpc.TimelineEvent.Builder builder =
        com.scalar.db.saga.rpc.TimelineEvent.newBuilder()
            .setTimestamp(toTimestamp(event.getTimestamp()))
            .setType(event.getType());
    Integer stepIndex = event.getStepIndex();
    if (stepIndex != null) {
      builder.setStepIndex(stepIndex);
    }
    String stepName = event.getStepName();
    if (stepName != null) {
      builder.setStepName(stepName);
    }
    SagaStatus resultingStatus = event.getResultingStatus();
    if (resultingStatus != null) {
      builder.setResultingStatus(toProtoStatus(resultingStatus));
    }
    String detail = event.getDetail();
    if (detail != null) {
      builder.setDetail(detail);
    }
    String operator = event.getOperator();
    if (operator != null) {
      builder.setOperator(operator);
    }
    return builder.build();
  }

  /**
   * Builds the api {@link SagaQuery} a {@code ListSagas} request selects. An out-of-range page
   * size, an out-of-range {@code updatedAt} timestamp, or an empty {@code updatedAt} window
   * surfaces as {@link IllegalArgumentException} (mapped to {@code INVALID_ARGUMENT}).
   */
  static SagaQuery toSagaQuery(ListSagasRequest request) {
    SagaQuery.Builder builder = SagaQuery.newBuilder();
    if (request.hasStatus()) {
      builder.status(fromProtoStatus(request.getStatus()));
    }
    if (request.hasUpdatedAfter()) {
      builder.updatedAfter(toInstant(request.getUpdatedAfter()));
    }
    if (request.hasUpdatedBefore()) {
      builder.updatedBefore(toInstant(request.getUpdatedBefore()));
    }
    if (request.hasPageSize()) {
      builder.pageSize(request.getPageSize());
    }
    if (request.hasPageToken()) {
      builder.pageToken(request.getPageToken());
    }
    return builder.build();
  }

  /**
   * Builds the api {@link SagaQuery} a bulk-reset sweep selects. The status filter is not accepted
   * — the sweep is defined as escalated sagas, which the engine pins — so only the window and
   * paging are mapped. An out-of-range page size or {@code updatedAt} timestamp, or an empty
   * window, surfaces as {@link IllegalArgumentException} (mapped to {@code INVALID_ARGUMENT}).
   */
  static SagaQuery toSagaQuery(ResetEscalatedBulkRequest request) {
    SagaQuery.Builder builder = SagaQuery.newBuilder();
    if (request.hasUpdatedAfter()) {
      builder.updatedAfter(toInstant(request.getUpdatedAfter()));
    }
    if (request.hasUpdatedBefore()) {
      builder.updatedBefore(toInstant(request.getUpdatedBefore()));
    }
    if (request.hasPageSize()) {
      builder.pageSize(request.getPageSize());
    }
    if (request.hasPageToken()) {
      builder.pageToken(request.getPageToken());
    }
    return builder.build();
  }

  /** Maps a page of snapshots to the wire list response. */
  static ListSagasResponse toProto(SagaPage<SagaStateSnapshot> page) {
    ListSagasResponse.Builder builder = ListSagasResponse.newBuilder();
    for (SagaStateSnapshot snapshot : page.getItems()) {
      builder.addSagas(toProto(snapshot));
    }
    if (page.getNextPageToken() != null) {
      builder.setNextPageToken(page.getNextPageToken());
    }
    return builder.build();
  }

  /** Maps a bulk-reset result (count + itemized skips + continuation token) to the wire result. */
  static com.scalar.db.saga.rpc.ResetResult toProto(ResetResult result) {
    com.scalar.db.saga.rpc.ResetResult.Builder builder =
        com.scalar.db.saga.rpc.ResetResult.newBuilder().setResetCount(result.getResetCount());
    for (ResetResult.SkippedSaga skipped : result.getSkipped()) {
      com.scalar.db.saga.rpc.SkippedSaga.Builder skippedBuilder =
          com.scalar.db.saga.rpc.SkippedSaga.newBuilder()
              .setSagaId(skipped.getSagaId())
              .setReason(toProtoSkipReason(skipped.getReason()));
      if (skipped.getDetail() != null) {
        skippedBuilder.setDetail(skipped.getDetail());
      }
      builder.addSkipped(skippedBuilder);
    }
    if (result.getNextPageToken() != null) {
      builder.setNextPageToken(result.getNextPageToken());
    }
    return builder.build();
  }

  /** Maps the wire status to the api status by name (inverse of {@link #toProtoStatus}). */
  static SagaStatus fromProtoStatus(com.scalar.db.saga.rpc.SagaStatus status) {
    String name = status.name();
    String prefix = "SAGA_STATUS_";
    if (!name.startsWith(prefix) || name.equals(prefix + "UNSPECIFIED")) {
      throw new IllegalArgumentException("unrecognized saga status filter");
    }
    return SagaStatus.valueOf(name.substring(prefix.length()));
  }

  static com.scalar.db.saga.rpc.SkipReason toProtoSkipReason(ResetResult.SkipReason reason) {
    try {
      return com.scalar.db.saga.rpc.SkipReason.valueOf("SKIP_REASON_" + reason.name());
    } catch (IllegalArgumentException e) {
      // As with toProtoStatus: a missing wire counterpart is api/proto version skew, a server
      // fault,
      // so INTERNAL rather than INVALID_ARGUMENT.
      throw new IllegalStateException("No wire SkipReason for api reason " + reason.name(), e);
    }
  }

  private static Instant toInstant(Timestamp timestamp) {
    try {
      return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    } catch (DateTimeException | ArithmeticException e) {
      // A client-supplied seconds value outside Instant's range (or one that overflows the nano
      // carry) is bad input, not a server fault; surface it as IllegalArgumentException so the
      // error mapper reports INVALID_ARGUMENT rather than INTERNAL.
      throw new IllegalArgumentException("timestamp out of range", e);
    }
  }

  private static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
