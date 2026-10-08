package com.scalar.db.saga.api;

import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Immutable read-only view of a saga instance.
 *
 * <p>{@link #withTransition} creates a new snapshot with updated status and timestamp.
 *
 * <p><b>What belongs here.</b> Only what a caller of the public API can act on. Server-internal
 * bookkeeping stays in the store's own rows: a field that coordinates replicas or otherwise serves
 * the engine rather than the caller is not a component of this class. The component list freezes at
 * the first release, so a later addition means a second constructor kept for good and a quiet
 * change to what {@link #equals} means.
 */
@Immutable
public final class SagaStateSnapshot {

  private final String sagaId;
  private final String sagaName;
  private final SagaStatus status;
  private final String definitionVersion;
  private final Instant createdAt;
  private final Instant updatedAt;

  /**
   * Creates a snapshot of a saga instance.
   *
   * @param sagaId the saga instance id
   * @param sagaName the name of the saga definition the instance runs
   * @param status the instance's current lifecycle status
   * @param definitionVersion the version of the saga definition the instance runs
   * @param createdAt when the instance was created
   * @param updatedAt when the instance's state last changed
   */
  public SagaStateSnapshot(
      String sagaId,
      String sagaName,
      SagaStatus status,
      String definitionVersion,
      Instant createdAt,
      Instant updatedAt) {
    this.sagaId = Objects.requireNonNull(sagaId, "sagaId must not be null");
    this.sagaName = Objects.requireNonNull(sagaName, "sagaName must not be null");
    this.status = Objects.requireNonNull(status, "status must not be null");
    this.definitionVersion =
        Objects.requireNonNull(definitionVersion, "definitionVersion must not be null");
    this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
    this.updatedAt = Objects.requireNonNull(updatedAt, "updatedAt must not be null");
  }

  /**
   * Creates a new snapshot with updated status and timestamp.
   *
   * @param newStatus the status the saga transitioned to
   * @param newUpdatedAt when the transition happened
   * @return a snapshot equal to this one except for its status and update time
   */
  public SagaStateSnapshot withTransition(SagaStatus newStatus, Instant newUpdatedAt) {
    Objects.requireNonNull(newStatus, "newStatus must not be null");
    Objects.requireNonNull(newUpdatedAt, "newUpdatedAt must not be null");
    return new SagaStateSnapshot(
        sagaId, sagaName, newStatus, definitionVersion, createdAt, newUpdatedAt);
  }

  /**
   * Returns the saga instance id.
   *
   * @return the saga instance id
   */
  public String getSagaId() {
    return sagaId;
  }

  /**
   * Returns the name of the saga definition this instance runs.
   *
   * @return the saga definition name
   */
  public String getSagaName() {
    return sagaName;
  }

  /**
   * Returns the instance's current lifecycle status.
   *
   * @return the current status
   */
  public SagaStatus getStatus() {
    return status;
  }

  /**
   * Returns the version of the saga definition this instance runs.
   *
   * @return the saga definition version
   */
  public String getDefinitionVersion() {
    return definitionVersion;
  }

  /**
   * Returns when the instance was created.
   *
   * @return the creation time
   */
  public Instant getCreatedAt() {
    return createdAt;
  }

  /**
   * Returns when the instance's state last changed.
   *
   * @return the time of the last state change
   */
  public Instant getUpdatedAt() {
    return updatedAt;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaStateSnapshot)) return false;
    SagaStateSnapshot that = (SagaStateSnapshot) o;
    return sagaId.equals(that.sagaId)
        && sagaName.equals(that.sagaName)
        && status == that.status
        && definitionVersion.equals(that.definitionVersion)
        && createdAt.equals(that.createdAt)
        && updatedAt.equals(that.updatedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sagaId, sagaName, status, definitionVersion, createdAt, updatedAt);
  }

  @Override
  public String toString() {
    return "SagaStateSnapshot{"
        + "sagaId='"
        + sagaId
        + "', sagaName='"
        + sagaName
        + "', status="
        + status
        + ", definitionVersion='"
        + definitionVersion
        + "', createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }
}
