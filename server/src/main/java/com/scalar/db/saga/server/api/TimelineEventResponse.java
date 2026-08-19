package com.scalar.db.saga.server.api;

import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import org.jspecify.annotations.Nullable;

/**
 * REST response view of one {@link TimelineEvent}. Carries metadata plus the failure error or
 * intervention reason only — never a raw step input/output payload, exactly as the core value type
 * already redacts. Timestamps are ISO-8601 strings to keep the JSON independent of the server's
 * date handling; the resulting status is its enum name; the nullable fields are omitted by the JSON
 * mapper when null.
 */
public record TimelineEventResponse(
    String timestamp,
    String type,
    @Nullable Integer stepIndex,
    @Nullable String stepName,
    @Nullable String resultingStatus,
    @Nullable String detail,
    @Nullable String operator) {

  /**
   * Builds a response from a {@link TimelineEvent}.
   *
   * @param event the timeline event
   * @return the response view
   */
  public static TimelineEventResponse from(TimelineEvent event) {
    SagaStatus resultingStatus = event.getResultingStatus();
    return new TimelineEventResponse(
        event.getTimestamp().toString(),
        event.getType(),
        event.getStepIndex(),
        event.getStepName(),
        resultingStatus == null ? null : resultingStatus.name(),
        event.getDetail(),
        event.getOperator());
  }
}
