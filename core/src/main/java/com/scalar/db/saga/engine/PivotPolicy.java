package com.scalar.db.saga.engine;

import com.scalar.db.saga.store.StatusEvent;
import org.jspecify.annotations.Nullable;

/**
 * Determines pivot boundary behavior for the unified execution loop.
 *
 * @param index last compensatable step index ({@code -1} means all steps are retriable)
 * @param crossingEvent event to emit when crossing the pivot (e.g., {@link
 *     StatusEvent#confirming()} for TCC), or {@code null} for Saga modes
 */
record PivotPolicy(int index, @Nullable StatusEvent crossingEvent) {}
