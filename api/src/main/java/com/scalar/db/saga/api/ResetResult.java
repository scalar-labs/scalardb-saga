package com.scalar.db.saga.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Outcome of a bulk {@link SagaAdminService#resetEscalated(SagaQuery, String)} over one page of
 * escalated sagas.
 *
 * <p>{@link #getResetCount()} rows were un-escalated and handed to the recovery loop to drive.
 * {@link #getSkipped()} lists the rows that matched the scan but were <b>not</b> actioned, each
 * with a {@link SkipReason} so the operator can follow up on exactly the right sagas — a lost
 * optimistic-concurrency race (likely transient) versus an unresolvable definition (needs an
 * operational fix). Pagination mirrors {@link SagaPage}: drive the sweep by {@link
 * #getNextPageToken()} until it is {@code null}, not by the counts.
 */
@Immutable
public final class ResetResult {

  /** Why a matched saga was skipped rather than reset. */
  public enum SkipReason {
    /**
     * Lost an optimistic-concurrency race to a concurrent writer; likely resolves on a later sweep.
     */
    CONCURRENT_MODIFICATION,
    /** The saga's definition version could not be resolved; register/redeploy it, then retry. */
    DEFINITION_NOT_FOUND,
    /**
     * The saga's stored event stream could not be read back, and a retry would fail identically —
     * its data is damaged, or was written by a newer version this one cannot decode. Needs manual
     * inspection; the rest of the sweep is unaffected.
     */
    CORRUPT_EVENT_STREAM
  }

  /** A saga that matched the sweep but was not reset, with the reason. */
  @Immutable
  public static final class SkippedSaga {

    private final String sagaId;
    private final SkipReason reason;

    public SkippedSaga(String sagaId, SkipReason reason) {
      this.sagaId = Objects.requireNonNull(sagaId, "sagaId must not be null");
      this.reason = Objects.requireNonNull(reason, "reason must not be null");
    }

    public String getSagaId() {
      return sagaId;
    }

    public SkipReason getReason() {
      return reason;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (!(o instanceof SkippedSaga)) return false;
      SkippedSaga that = (SkippedSaga) o;
      return sagaId.equals(that.sagaId) && reason == that.reason;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sagaId, reason);
    }

    @Override
    public String toString() {
      return "SkippedSaga{sagaId='" + sagaId + "', reason=" + reason + '}';
    }
  }

  private final int resetCount;
  private final List<SkippedSaga> skipped;
  private final @Nullable String nextPageToken;

  public ResetResult(int resetCount, List<SkippedSaga> skipped, @Nullable String nextPageToken) {
    if (resetCount < 0) {
      throw new IllegalArgumentException("resetCount must be >= 0, got " + resetCount);
    }
    Objects.requireNonNull(skipped, "skipped must not be null");
    this.resetCount = resetCount;
    this.skipped = Collections.unmodifiableList(new ArrayList<>(skipped));
    this.nextPageToken = nextPageToken;
  }

  /** The number of sagas un-escalated and handed to the recovery loop in this page. */
  public int getResetCount() {
    return resetCount;
  }

  /**
   * The matched sagas that were skipped (lost CAS race or unresolvable definition), unmodifiable.
   */
  public List<SkippedSaga> getSkipped() {
    return skipped;
  }

  /** Convenience for {@code getSkipped().size()}. */
  public int getSkippedCount() {
    return skipped.size();
  }

  /** The token to continue the sweep, or {@code null} if this was the last page. */
  public @Nullable String getNextPageToken() {
    return nextPageToken;
  }

  /** Returns {@code true} if there are more pages of escalated sagas to sweep. */
  public boolean hasMore() {
    return nextPageToken != null;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof ResetResult)) return false;
    ResetResult that = (ResetResult) o;
    return resetCount == that.resetCount
        && skipped.equals(that.skipped)
        && Objects.equals(nextPageToken, that.nextPageToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resetCount, skipped, nextPageToken);
  }

  @Override
  public String toString() {
    return "ResetResult{resetCount="
        + resetCount
        + ", skipped="
        + skipped
        + ", nextPageToken='"
        + nextPageToken
        + "'}";
  }
}
