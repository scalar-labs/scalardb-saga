package com.scalar.db.saga.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * A single saga's current state plus its flat event timeline — the detail view behind {@link
 * SagaOrchestrator#getSagaDetail(String)}.
 *
 * <p>When the saga's history exceeds the orchestrator's configured timeline bound, the timeline
 * holds only the newest events and {@link #isTruncated()} is true; the full history remains in the
 * store.
 */
@Immutable
public final class SagaDetail {

  private final SagaStateSnapshot snapshot;
  private final List<TimelineEvent> timeline;
  private final boolean truncated;

  /**
   * Creates an untruncated detail. Equivalent to {@code SagaDetail(snapshot, timeline, false)}.
   *
   * @param snapshot the saga's current state
   * @param timeline the saga's events in sequence order (defensively copied)
   */
  public SagaDetail(SagaStateSnapshot snapshot, List<TimelineEvent> timeline) {
    this(snapshot, timeline, false);
  }

  /**
   * @param snapshot the saga's current state
   * @param timeline the saga's events in sequence order (defensively copied)
   * @param truncated whether the timeline was cut to the newest events because the saga's history
   *     exceeded the configured bound
   */
  public SagaDetail(SagaStateSnapshot snapshot, List<TimelineEvent> timeline, boolean truncated) {
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot must not be null");
    Objects.requireNonNull(timeline, "timeline must not be null");
    this.timeline = Collections.unmodifiableList(new ArrayList<>(timeline));
    this.truncated = truncated;
  }

  /** The saga's current state snapshot. */
  public SagaStateSnapshot getSnapshot() {
    return snapshot;
  }

  /** The saga's events in sequence order (unmodifiable). */
  public List<TimelineEvent> getTimeline() {
    return timeline;
  }

  /**
   * Whether the timeline holds only the newest events because the saga's history exceeded the
   * configured bound. The full history remains in the store.
   */
  public boolean isTruncated() {
    return truncated;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaDetail)) return false;
    SagaDetail that = (SagaDetail) o;
    return snapshot.equals(that.snapshot)
        && timeline.equals(that.timeline)
        && truncated == that.truncated;
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshot, timeline, truncated);
  }

  @Override
  public String toString() {
    return "SagaDetail{snapshot="
        + snapshot
        + ", timeline="
        + timeline
        + ", truncated="
        + truncated
        + '}';
  }
}
