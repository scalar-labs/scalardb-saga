package com.scalar.db.saga.server.api;

import com.scalar.db.saga.api.SagaDetail;
import java.util.List;

/**
 * REST response view of a {@link SagaDetail}: the saga's current state snapshot plus its timeline
 * (metadata and error/reason only — never raw step payloads). {@code truncated} is true when the
 * timeline holds only the newest events because the saga's history exceeded the server's configured
 * bound; the full history remains in the store.
 */
public record SagaDetailResponse(
    SagaSnapshotResponse saga, List<TimelineEventResponse> timeline, boolean truncated) {

  /** Defensively copies {@code timeline} so the response is immutable. */
  public SagaDetailResponse {
    timeline = List.copyOf(timeline);
  }

  /**
   * Builds a response from a {@link SagaDetail}.
   *
   * @param detail the saga detail
   * @return the response view
   */
  public static SagaDetailResponse from(SagaDetail detail) {
    List<TimelineEventResponse> timeline =
        detail.getTimeline().stream().map(TimelineEventResponse::from).toList();
    return new SagaDetailResponse(
        SagaSnapshotResponse.from(detail.getSnapshot()), timeline, detail.isTruncated());
  }
}
