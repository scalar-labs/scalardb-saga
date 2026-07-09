package com.scalar.db.saga.api;

import java.util.HashMap;
import java.util.Map;

/** Lifecycle status of a saga instance. */
public enum SagaStatus {

  /** Executing forward steps (Saga) or Try/Confirm phase (TCC). */
  RUNNING(0),

  /** All steps succeeded (and confirmed, in TCC mode). */
  COMPLETED(1),

  /** Executing compensation steps (Saga) or Cancel phase (TCC). */
  COMPENSATING(2),

  /** All compensations/cancellations completed. */
  COMPENSATED(3),

  /** Stuck beyond grace period, needs manual intervention. */
  ESCALATED(4),

  /** Parked on an async step, awaiting an external callback (daemon mode). */
  WAITING(5);

  private static final Map<Integer, SagaStatus> BY_STATUS_CODE = new HashMap<>();

  static {
    for (SagaStatus status : values()) {
      BY_STATUS_CODE.put(status.statusCode, status);
    }
  }

  private final int statusCode;

  SagaStatus(int statusCode) {
    this.statusCode = statusCode;
  }

  /**
   * Returns the stable status code for database storage.
   *
   * @return the status code
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * Returns the {@code SagaStatus} for the given status code.
   *
   * @param statusCode the status code (as stored in the database)
   * @return the corresponding status
   * @throws IllegalArgumentException if the status code is unknown
   */
  public static SagaStatus fromStatusCode(int statusCode) {
    SagaStatus status = BY_STATUS_CODE.get(statusCode);
    if (status == null) {
      throw new IllegalArgumentException("Invalid SagaStatus code: " + statusCode);
    }
    return status;
  }

  /** Returns {@code true} if this status is terminal (COMPLETED, COMPENSATED, or ESCALATED). */
  public boolean isTerminal() {
    return this == COMPLETED || this == COMPENSATED || this == ESCALATED;
  }

  /**
   * Returns {@code true} if sagas in this status are eligible for the recovery staleness scan:
   * resuming forward execution (RUNNING) or compensation (COMPENSATING). A parked (WAITING) saga is
   * excluded — it is timed out via the dedicated {@code saga_parked} deadline index, not by {@code
   * updated_at} staleness.
   */
  public boolean isRecoverable() {
    return this == RUNNING || this == COMPENSATING;
  }

  /**
   * Returns {@code true} if sagas in this status may be automatically purged after the retention
   * period. ESCALATED sagas are excluded — they require manual admin resolution before cleanup.
   */
  public boolean isPurgeable() {
    return this == COMPLETED || this == COMPENSATED;
  }
}
