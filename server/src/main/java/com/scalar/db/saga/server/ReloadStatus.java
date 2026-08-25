package com.scalar.db.saga.server;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * One immutable snapshot of the configuration-reload state, updated by {@link ConfigReconciler}
 * after every pass. Observability is logs-only in v1, so nothing serves this over the wire yet —
 * but the pass maintains it fully so a later admin status endpoint can expose it additively,
 * unchanged.
 *
 * <p>Hashes cover <b>raw file bytes only</b>, never resolved values: a hash over resolved content
 * would let anyone who can read the status confirm guesses about secret values. The applied hashes
 * are what an operator greps across replicas to tell a lagging replica from a rejecting one; the
 * rejection carries the candidate's hash and reason so the rejecting replica names what it refused.
 *
 * @param appliedServicesSha256 hash of the applied service files (name-keyed, raw bytes)
 * @param appliedDefinitionsSha256 hash of the applied definition files (name-keyed, raw bytes)
 * @param appliedAt when the applied state last changed; a pass that finds nothing to change
 *     verifies the applied state without advancing this
 * @param rejection the most recent rejection since the last successful apply, or {@code null}
 */
record ReloadStatus(
    String appliedServicesSha256,
    String appliedDefinitionsSha256,
    Instant appliedAt,
    @Nullable Rejection rejection) {

  /**
   * The Envoy-{@code error_state}-style rejection record: what was refused, why, and when.
   *
   * @param candidateSha256 hash of the rejected candidate set's raw file bytes (services and
   *     definitions combined)
   * @param reason the aggregated rejection reason (never carries resolved values)
   * @param rejectedAt when the rejection happened
   */
  record Rejection(String candidateSha256, String reason, Instant rejectedAt) {}

  ReloadStatus withRejection(Rejection rejection) {
    return new ReloadStatus(appliedServicesSha256, appliedDefinitionsSha256, appliedAt, rejection);
  }
}
