package com.scalar.db.saga.store;

import com.scalar.db.saga.api.SagaStateSnapshot;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * A saga's current state snapshot together with its full event stream, read atomically in one
 * transaction. Pairing the two in a single read guarantees the snapshot's status is coherent with
 * the events (a concurrent status transition cannot pair a stale snapshot with a newer timeline).
 */
@Immutable
public record SagaStateAndEvents(SagaStateSnapshot snapshot, List<SagaEvent> events) {

  public SagaStateAndEvents {
    Objects.requireNonNull(snapshot, "snapshot must not be null");
    Objects.requireNonNull(events, "events must not be null");
    events = Collections.unmodifiableList(new ArrayList<>(events));
  }
}
