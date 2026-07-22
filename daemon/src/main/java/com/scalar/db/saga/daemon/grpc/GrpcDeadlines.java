package com.scalar.db.saga.daemon.grpc;

import io.grpc.Context;
import io.grpc.Deadline;
import java.util.concurrent.TimeUnit;

/**
 * Shared gRPC deadline arithmetic for the request-thread-bounding paths ({@link SagaServiceImpl}'s
 * sync wait and {@link AdminServiceImpl}'s admin drive). Owns the single slack both subtract from
 * the remaining call deadline, so the two cannot drift.
 */
final class GrpcDeadlines {

  /**
   * Slack subtracted from the remaining call deadline so the server returns its snapshot before
   * gRPC cancels the call (which would otherwise leave the caller with nothing).
   */
  private static final long DEADLINE_SLACK_MILLIS = 100L;

  private GrpcDeadlines() {}

  /**
   * Returns {@code bound} tightened by the caller's remaining gRPC call deadline (minus slack) when
   * one is set, floored at {@code floorMillis}. The floor differs by call site because {@code 0}
   * means opposite things downstream: {@code 0} where it means "return immediately" (the sync-wait
   * path), {@code 1} where {@code 0} would instead flip the admin drive to "unbounded, on the
   * calling thread".
   */
  static long tightenToCallDeadline(long bound, long floorMillis) {
    Deadline deadline = Context.current().getDeadline();
    if (deadline != null) {
      long remaining =
          Math.max(
              floorMillis, deadline.timeRemaining(TimeUnit.MILLISECONDS) - DEADLINE_SLACK_MILLIS);
      bound = Math.min(bound, remaining);
    }
    return bound;
  }
}
