package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SweepScatterTest {

  // Fixed sample owner IDs; the assertions on them are deterministic, not statistical.
  private static final List<String> SAMPLE_OWNER_IDS =
      List.of(
          "replica-a",
          "replica-b",
          "replica-c",
          "saga-server-0",
          "saga-server-1",
          "e9b1c2d3-4f56-7890-abcd-ef0123456789");

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 16, 17, 64})
  void permutation_anyBucketCountGiven_coversAllBucketsExactlyOnce(int numBuckets) {
    for (String ownerId : SAMPLE_OWNER_IDS) {
      // Act
      int[] order = SweepScatter.permutation(SweepScatter.seed(ownerId), numBuckets);

      // Assert
      assertThat(order).hasSize(numBuckets);
      Set<Integer> seen = new HashSet<>();
      for (int bucket : order) {
        assertThat(bucket).isBetween(0, numBuckets - 1);
        assertThat(seen.add(bucket)).isTrue();
      }
    }
  }

  @Test
  void permutation_sameSeedGiven_returnsSameOrder() {
    // Arrange
    int seed = SweepScatter.seed("replica-a");

    // Act
    int[] first = SweepScatter.permutation(seed, 16);
    int[] second = SweepScatter.permutation(seed, 16);

    // Assert
    assertThat(first).isEqualTo(second);
  }

  @Test
  void permutation_distinctOwnerIdsGiven_returnsPairwiseDistinctOrders() {
    // Act
    List<int[]> orders =
        SAMPLE_OWNER_IDS.stream()
            .map(ownerId -> SweepScatter.permutation(SweepScatter.seed(ownerId), 16))
            .toList();

    // Assert
    for (int i = 0; i < orders.size(); i++) {
      for (int j = i + 1; j < orders.size(); j++) {
        assertThat(orders.get(i)).isNotEqualTo(orders.get(j));
      }
    }
  }

  /**
   * Guards the monotonic-sort-key trap: a key monotonic in the bucket number would sort every
   * replica into the identity (ascending) order, silently reinstating in-phase lockstep sweeps.
   */
  @Test
  void permutation_sampleOwnerIdsGiven_neverTheAscendingIdentityOrder() {
    // Arrange
    int[] identity = new int[16];
    for (int i = 0; i < 16; i++) {
      identity[i] = i;
    }

    for (String ownerId : SAMPLE_OWNER_IDS) {
      // Act
      int[] order = SweepScatter.permutation(SweepScatter.seed(ownerId), 16);

      // Assert
      assertThat(order).isNotEqualTo(identity);
    }
  }

  @Test
  void seed_sameOwnerIdGiven_returnsSameSeed() {
    // Act
    int first = SweepScatter.seed("replica-a");
    int second = SweepScatter.seed("replica-a");

    // Assert
    assertThat(first).isEqualTo(second);
  }

  @Test
  void seed_distinctOwnerIdsGiven_returnsDistinctSeeds() {
    // Act
    Set<Integer> seeds = new HashSet<>();
    for (String ownerId : SAMPLE_OWNER_IDS) {
      seeds.add(SweepScatter.seed(ownerId));
    }

    // Assert
    assertThat(seeds).hasSize(SAMPLE_OWNER_IDS.size());
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 30, 60, 3600})
  void offsetSeconds_anyIntervalGiven_staysWithinInterval(long intervalSeconds) {
    for (String ownerId : SAMPLE_OWNER_IDS) {
      // Act
      long offset = SweepScatter.offsetSeconds(ownerId, "recovery", intervalSeconds);

      // Assert
      assertThat(offset).isBetween(0L, intervalSeconds - 1);
    }
  }

  @Test
  void offsetSeconds_sameOwnerIdGiven_returnsSameOffset() {
    // Act
    long first = SweepScatter.offsetSeconds("replica-a", "recovery", 3600);
    long second = SweepScatter.offsetSeconds("replica-a", "recovery", 3600);

    // Assert
    assertThat(first).isEqualTo(second);
  }

  @Test
  void offsetSeconds_distinctOwnerIdsGiven_doNotAllCollide() {
    // Act
    Set<Long> offsets = new HashSet<>();
    for (String ownerId : SAMPLE_OWNER_IDS) {
      offsets.add(SweepScatter.offsetSeconds(ownerId, "recovery", 3600));
    }

    // Assert: pairwise distinctness is not guaranteed by hashing, but the fixed samples must not
    // all land on one offset; that would indicate a broken mix.
    assertThat(offsets.size()).isGreaterThan(1);
  }

  @Test
  void offsetSeconds_distinctPurposesGiven_offsetsDoNotSystematicallyCollide() {
    // Arrange — a replica's different sweeps (recovery vs retention) with EQUAL intervals must
    // not all share an offset, or the purpose salt is inert and both sweeps fire simultaneously
    // for the life of the process.
    int differing = 0;
    for (String ownerId : SAMPLE_OWNER_IDS) {
      // Act
      long recovery = SweepScatter.offsetSeconds(ownerId, "recovery", 3600);
      long retention = SweepScatter.offsetSeconds(ownerId, "retention", 3600);
      if (recovery != retention) {
        differing++;
      }
    }

    // Assert — per-pair equality is not guaranteed by hashing, but all six samples colliding
    // would mean the purpose is not mixed in at all
    assertThat(differing).isGreaterThan(0);
  }

  @Test
  void offsetSeconds_decorrelatedFromPermutationSeed() {
    // Arrange: two owners whose permutations differ must not need to differ in offset, and vice
    // versa; assert only that offset is not simply the seed reduced modulo the interval, which
    // would couple the two derivations.
    String ownerId = "replica-a";

    // Act
    long offset = SweepScatter.offsetSeconds(ownerId, "recovery", 3600);
    long naive = Math.floorMod((long) SweepScatter.seed(ownerId), 3600L);

    // Assert
    assertThat(offset).isNotEqualTo(naive);
  }

  @Test
  void permutation_distinctSeedsGiven_ordersDivergeBeyondTheStart() {
    // Arrange: two replicas must not share a rotated or shifted copy of one global order; compare
    // full sequences from each starting point.
    int[] a = SweepScatter.permutation(SweepScatter.seed("replica-a"), 16);
    int[] b = SweepScatter.permutation(SweepScatter.seed("replica-b"), 16);

    // Act: check whether b is any rotation of a.
    boolean anyRotationMatches = false;
    for (int shift = 0; shift < 16; shift++) {
      boolean matches = true;
      for (int i = 0; i < 16; i++) {
        if (a[(i + shift) % 16] != b[i]) {
          matches = false;
          break;
        }
      }
      if (matches) {
        anyRotationMatches = true;
        break;
      }
    }

    // Assert
    assertThat(anyRotationMatches).isFalse();
  }
}
