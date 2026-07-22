package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.ResetResult.SkippedSaga;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * REST response view of a bulk {@link ResetResult}: how many sagas were reset, which were skipped
 * and why, and an opaque {@code nextPageToken} when the sweep did not reach the end of the matching
 * set (absent otherwise).
 */
public record ResetResultResponse(
    int resetCount, List<SkippedSagaResponse> skipped, @Nullable String nextPageToken) {

  /** Defensively copies {@code skipped} so the response is immutable. */
  public ResetResultResponse {
    skipped = List.copyOf(skipped);
  }

  /** One skipped saga: its id, a machine-readable reason, and an optional human-readable detail. */
  public record SkippedSagaResponse(String sagaId, String reason, @Nullable String detail) {}

  /**
   * Builds a response from a {@link ResetResult}.
   *
   * @param result the bulk reset result
   * @return the response view
   */
  public static ResetResultResponse from(ResetResult result) {
    List<SkippedSagaResponse> skipped =
        result.getSkipped().stream().map(ResetResultResponse::skipped).toList();
    return new ResetResultResponse(result.getResetCount(), skipped, result.getNextPageToken());
  }

  private static SkippedSagaResponse skipped(SkippedSaga skipped) {
    return new SkippedSagaResponse(
        skipped.getSagaId(), skipped.getReason().name(), skipped.getDetail());
  }
}
