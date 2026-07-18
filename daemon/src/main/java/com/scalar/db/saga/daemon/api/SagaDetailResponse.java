package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.TimelineEvent;
import java.util.List;

/**
 * REST response view of a {@link SagaDetail}: the saga's current state snapshot plus its timeline
 * (metadata and error/reason only — never raw step payloads).
 */
public record SagaDetailResponse(SagaSnapshotResponse saga, List<TimelineEventResponse> timeline) {

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
        detail.getTimeline().stream().map(SagaDetailResponse::event).toList();
    return new SagaDetailResponse(SagaSnapshotResponse.from(detail.getSnapshot()), timeline);
  }

  private static TimelineEventResponse event(TimelineEvent event) {
    return TimelineEventResponse.from(event);
  }
}
