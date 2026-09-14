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

  /** Creates a new snapshot with updated status and timestamp. */
  public SagaStateSnapshot withTransition(SagaStatus newStatus, Instant newUpdatedAt) {
    Objects.requireNonNull(newStatus, "newStatus must not be null");
    Objects.requireNonNull(newUpdatedAt, "newUpdatedAt must not be null");
    return new SagaStateSnapshot(
        sagaId, sagaName, newStatus, definitionVersion, createdAt, newUpdatedAt);
  }

  public String getSagaId() {
    return sagaId;
  }

  public String getSagaName() {
    return sagaName;
  }

  public SagaStatus getStatus() {
    return status;
  }

  public String getDefinitionVersion() {
    return definitionVersion;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

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
