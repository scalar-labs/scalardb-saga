package com.scalar.db.saga.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Context;
import io.grpc.Deadline;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the deadline arithmetic both request-thread-bounding paths share.
 *
 * <p>Tested directly rather than through a served call. The property that matters most — a deadline
 * at or under the slack floors rather than reaching zero — can only be provoked end to end by a
 * genuinely tiny wall-clock deadline, which under build load expires before the server can answer
 * and fails the call instead of the assertion. The arithmetic owes nothing to a round trip, so the
 * round trip is not what should verify it.
 *
 * <p>The deadlines here are built on a <b>frozen ticker</b>, so {@code timeRemaining} returns the
 * same value however long the JVM is descheduled between constructing one and reading it. That is
 * what lets these assert exact milliseconds instead of ranges.
 */
class GrpcDeadlinesTest {

  /**
   * The slack {@code GrpcDeadlines} subtracts, restated here rather than read from production.
   * Widening that constant's visibility for a test's benefit is a production change made for the
   * test, so the duplication is the cheaper of the two; a drift between them fails the two cases
   * below that name it, which is the point.
   */
  private static final long SLACK_MILLIS = 100L;

  /** A stopped clock. Every {@code timeRemaining} reads the same instant the deadline was built. */
  private static final Deadline.Ticker FROZEN =
      new Deadline.Ticker() {
        @Override
        public long nanoTime() {
          return 0L;
        }
      };

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  void tightenToCallDeadline_noDeadlineOnTheContext_returnsTheBoundUnchanged() {
    // Act / Assert — an embedded caller, or a client that set none: nothing to tighten against.
    assertThat(GrpcDeadlines.tightenToCallDeadline(30_000L, 1L)).isEqualTo(30_000L);
  }

  @Test
  void tightenToCallDeadline_deadlineFurtherAwayThanTheBound_returnsTheBoundUnchanged() {
    // Act / Assert — 60s remaining leaves 59.9s after slack, so the caller's own bound is still the
    // binding constraint and the deadline changes nothing.
    assertThat(withDeadline(60_000L, () -> GrpcDeadlines.tightenToCallDeadline(30_000L, 1L)))
        .isEqualTo(30_000L);
  }

  @Test
  void tightenToCallDeadline_deadlineNearerThanTheBound_returnsRemainingLessSlack() {
    // Act / Assert — the deadline binds, and the answer is exactly what is left after slack. The
    // slack is what makes the server answer before gRPC cancels the call out from under it.
    assertThat(withDeadline(5_000L, () -> GrpcDeadlines.tightenToCallDeadline(30_000L, 1L)))
        .isEqualTo(5_000L - SLACK_MILLIS);
  }

  @Test
  void tightenToCallDeadline_deadlineEqualToTheSlack_floorsInsteadOfReachingZero() {
    // Act / Assert — the case this class exists to pin. Remaining minus slack is exactly 0 here,
    // and 0 means "unbounded, drive on the calling thread" downstream of the admin path, which is
    // the opposite of what a caller in a hurry asked for. The floor is what stops that.
    assertThat(withDeadline(SLACK_MILLIS, () -> GrpcDeadlines.tightenToCallDeadline(30_000L, 1L)))
        .isEqualTo(1L);
  }

  @Test
  void tightenToCallDeadline_deadlineAlreadyInsideTheSlack_floorsRatherThanGoingNegative() {
    // Act / Assert — remaining minus slack is negative. Without the floor the bound would be a
    // negative number rather than merely zero.
    assertThat(withDeadline(10L, () -> GrpcDeadlines.tightenToCallDeadline(30_000L, 1L)))
        .isEqualTo(1L);
  }

  @Test
  void tightenToCallDeadline_withTheSyncWaitFloorOfZero_floorsToZeroNotOne() {
    // Act / Assert — the two call sites pass different floors because 0 means opposite things
    // downstream: "return immediately" on the sync-wait path, "unbounded" on the admin drive. Same
    // input, different floor, so the floor is genuinely the caller's to choose.
    assertThat(withDeadline(SLACK_MILLIS, () -> GrpcDeadlines.tightenToCallDeadline(30_000L, 0L)))
        .isEqualTo(0L);
  }

  /** Runs {@code arithmetic} under a context whose deadline is {@code remainingMillis} away. */
  private long withDeadline(long remainingMillis, LongSupplier arithmetic) {
    Context.CancellableContext context =
        Context.current()
            .withDeadline(
                Deadline.after(remainingMillis, TimeUnit.MILLISECONDS, FROZEN), scheduler);
    try {
      return context.call(arithmetic::getAsLong);
    } catch (Exception e) {
      throw new IllegalStateException("deadline arithmetic threw", e);
    } finally {
      context.close();
    }
  }
}
