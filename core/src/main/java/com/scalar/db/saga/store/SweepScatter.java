package com.scalar.db.saga.store;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Deterministic per-replica scatter for the periodic sweeps (recovery, parked timeout, retention).
 *
 * <p>Multiple replicas sweep the same bucket-partitioned tables on similar schedules. Deriving each
 * replica's bucket order and schedule offset from a hash of its {@code ownerId} spreads the
 * replicas across buckets and across time without any coordination: distinct owner IDs (already
 * required by the claim protocol) yield distinct sweep permutations with high probability, and a
 * collision merely degrades the colliding pair to in-phase behavior, which the claim protocol
 * absorbs.
 *
 * <p>Everything here is a pure function of the inputs — no {@code Random} (deterministic behavior,
 * reproducible tests, and no SpotBugs {@code PREDICTABLE_RANDOM} findings).
 *
 * <p><b>The avalanche mix is load-bearing.</b> The per-bucket sort key must depend on the bucket
 * number through a full avalanche mix. A key monotonic in the bucket number (for example {@code
 * seed + bucket}) would sort every replica's buckets into the same ascending order, silently
 * putting all replicas back in lockstep. {@code SweepScatterTest} guards this with fixed-seed order
 * assertions.
 *
 * <p>This type is {@code public} solely for cross-package access within the module (the engine
 * package's recovery and retention managers derive their schedule offsets here); it is not part of
 * the public API.
 */
public final class SweepScatter {

  /** Decorrelates {@link #offsetSeconds} from {@link #permutation} for the same owner. */
  private static final int OFFSET_SALT = 0x7F4A7C15;

  /** Spreads consecutive bucket numbers across the hash space before mixing. */
  private static final int BUCKET_SPREAD = 0x9E3779B9;

  private SweepScatter() {}

  /**
   * The scatter seed for a replica, an avalanche-mixed hash of its owner ID. The seed, not the
   * owner ID, is what sweep cursors carry.
   */
  public static int seed(String ownerId) {
    return mix(ownerId.hashCode());
  }

  /**
   * The bucket sweep order for a seed: a pseudo-random permutation of {@code [0, numBuckets)},
   * stable for a given {@code (seed, numBuckets)} pair.
   */
  public static int[] permutation(int seed, int numBuckets) {
    Integer[] buckets = new Integer[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      buckets[i] = i;
    }
    Arrays.sort(
        buckets,
        Comparator.comparingInt((Integer bucket) -> mix(seed ^ (bucket * BUCKET_SPREAD)))
            .thenComparing(bucket -> bucket));
    int[] order = new int[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      order[i] = buckets[i];
    }
    return order;
  }

  /**
   * A deterministic schedule offset in {@code [0, intervalSeconds)} for de-phasing a replica's
   * periodic sweep within the shared interval. The {@code purpose} (e.g., {@code "recovery"} vs
   * {@code "retention"}) is mixed in so a replica's different sweeps get independent offsets:
   * without it, two sweeps configured with equal intervals would fire at the same instant for the
   * life of the process — exactly the coincident load the offset exists to prevent.
   */
  public static long offsetSeconds(String ownerId, String purpose, long intervalSeconds) {
    return Math.floorMod(
        (long) mix(seed(ownerId) ^ OFFSET_SALT ^ purpose.hashCode()), intervalSeconds);
  }

  /** Murmur3's 32-bit finalizer: full avalanche, every input bit affects every output bit. */
  private static int mix(int h) {
    h ^= h >>> 16;
    h *= 0x85EBCA6B;
    h ^= h >>> 13;
    h *= 0xC2B2AE35;
    h ^= h >>> 16;
    return h;
  }
}
