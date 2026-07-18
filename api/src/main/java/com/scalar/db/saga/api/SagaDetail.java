package com.scalar.db.saga.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * A single saga's current state plus its full, flat event timeline — the detail view behind {@link
 * SagaOrchestrator#getSagaDetail(String)}.
 */
@Immutable
public final class SagaDetail {

  private final SagaStateSnapshot snapshot;
  private final List<TimelineEvent> timeline;

  /**
   * @param snapshot the saga's current state
   * @param timeline the saga's events in sequence order (defensively copied)
   */
  public SagaDetail(SagaStateSnapshot snapshot, List<TimelineEvent> timeline) {
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot must not be null");
    Objects.requireNonNull(timeline, "timeline must not be null");
    this.timeline = Collections.unmodifiableList(new ArrayList<>(timeline));
  }

  /** The saga's current state snapshot. */
  public SagaStateSnapshot getSnapshot() {
    return snapshot;
  }

  /** The saga's events in sequence order (unmodifiable). */
  public List<TimelineEvent> getTimeline() {
    return timeline;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaDetail)) return false;
    SagaDetail that = (SagaDetail) o;
    return snapshot.equals(that.snapshot) && timeline.equals(that.timeline);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshot, timeline);
  }

  @Override
  public String toString() {
    return "SagaDetail{snapshot=" + snapshot + ", timeline=" + timeline + '}';
  }
}
