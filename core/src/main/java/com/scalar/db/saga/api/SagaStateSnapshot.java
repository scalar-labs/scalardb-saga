package com.scalar.db.saga.api;

import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Immutable read-only view of a saga instance, constructed from a {@code saga_state} row.
 *
 * <p>{@link #withTransition} creates a new snapshot with updated status and timestamp but does
 * <b>not</b> increment the version. Version is only incremented by {@code SagaStore} on transaction
 * writes, ensuring safe optimistic concurrency control across recovery boundaries.
 */
@Immutable
public final class SagaStateSnapshot {

  private final String sagaId;
  private final String sagaName;
  private final SagaStatus status;
  private final String ownerId;
  private final int version;
  private final String definitionVersion;
  private final Instant createdAt;
  private final Instant updatedAt;

  public SagaStateSnapshot(
      String sagaId,
      String sagaName,
      SagaStatus status,
      String ownerId,
      int version,
      String definitionVersion,
      Instant createdAt,
      Instant updatedAt) {
    this.sagaId = Objects.requireNonNull(sagaId, "sagaId must not be null");
    this.sagaName = Objects.requireNonNull(sagaName, "sagaName must not be null");
    this.status = Objects.requireNonNull(status, "status must not be null");
    this.ownerId = Objects.requireNonNull(ownerId, "ownerId must not be null");
    this.version = version;
    this.definitionVersion =
        Objects.requireNonNull(definitionVersion, "definitionVersion must not be null");
    this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
    this.updatedAt = Objects.requireNonNull(updatedAt, "updatedAt must not be null");
  }

  /**
   * Creates a new snapshot with updated status and timestamp.
   *
   * <p>The version is <b>not</b> incremented — version changes are managed by the store layer.
   */
  public SagaStateSnapshot withTransition(SagaStatus newStatus, Instant newUpdatedAt) {
    Objects.requireNonNull(newStatus, "newStatus must not be null");
    Objects.requireNonNull(newUpdatedAt, "newUpdatedAt must not be null");
    return new SagaStateSnapshot(
        sagaId, sagaName, newStatus, ownerId, version, definitionVersion, createdAt, newUpdatedAt);
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

  public String getOwnerId() {
    return ownerId;
  }

  public int getVersion() {
    return version;
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
    if (!(o instanceof SagaStateSnapshot that)) return false;
    return version == that.version
        && sagaId.equals(that.sagaId)
        && sagaName.equals(that.sagaName)
        && status == that.status
        && ownerId.equals(that.ownerId)
        && definitionVersion.equals(that.definitionVersion)
        && createdAt.equals(that.createdAt)
        && updatedAt.equals(that.updatedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        sagaId, sagaName, status, ownerId, version, definitionVersion, createdAt, updatedAt);
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
        + ", ownerId='"
        + ownerId
        + "', version="
        + version
        + ", definitionVersion='"
        + definitionVersion
        + "', createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }
}
