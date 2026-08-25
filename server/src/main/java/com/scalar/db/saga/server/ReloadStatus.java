package com.scalar.db.saga.server;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * One immutable snapshot of the configuration-reload state, replaced by {@link ConfigReconciler} as
 * a pass proceeds — including part-way through one, so a pass that applies services and then fails
 * still reports the services it applied. Observability is logs-only in v1, so nothing serves this
 * over the wire yet — but the pass maintains it fully so a later admin status endpoint can expose
 * it additively, unchanged.
 *
 * <p>Every component is {@code null} before the first pass concludes, which is the honest rendering
 * of "nothing applied yet" for an endpoint that will serialize this. A sentinel would read as a
 * value.
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
 * @param lastPassAt when a pass last concluded, whether it applied or was rejected. It is the only
 *     component a healthy no-change pass advances, so it is what separates a replica that is up to
 *     date from one whose pass thread is wedged — a hung mount parks the single pass thread in a
 *     filesystem call that does not respond to interrupt, and every other field then describes a
 *     state nothing is still verifying
 * @param rejection the most recent rejection since the last successful apply, or {@code null}
 */
record ReloadStatus(
    @Nullable String appliedServicesSha256,
    @Nullable String appliedDefinitionsSha256,
    @Nullable Instant appliedAt,
    @Nullable Instant lastPassAt,
    @Nullable Rejection rejection) {

  /** The state before any pass has concluded. */
  static ReloadStatus initial() {
    return new ReloadStatus(null, null, null, null, null);
  }

  /**
   * The Envoy-{@code error_state}-style rejection record: what was refused, why, and when.
   *
   * @param candidateSha256 hash of the rejected candidate set's raw file bytes (services and
   *     definitions combined)
   * @param reason the aggregated rejection reason (never carries resolved values)
   * @param rejectedAt when the rejection happened
   * @param operatorActionRequired whether clearing this rejection needs the mounted files to
   *     change. A pass aggregates problems, so one rejection can carry several: this is true when
   *     any of them is one retrying cannot fix, which is the question an alert asks — will this
   *     replica heal on its own, or is it waiting for a human? The reason names which problem is
   *     which, but nothing should have to match on that prose to find out.
   */
  record Rejection(
      String candidateSha256, String reason, Instant rejectedAt, boolean operatorActionRequired) {}

  ReloadStatus withRejection(Rejection rejection, Instant lastPassAt) {
    return new ReloadStatus(
        appliedServicesSha256, appliedDefinitionsSha256, appliedAt, lastPassAt, rejection);
  }
}
