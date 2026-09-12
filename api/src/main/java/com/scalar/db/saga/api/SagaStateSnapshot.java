package com.scalar.db.saga.api;

import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Immutable read-only view of a saga instance, constructed from a {@code saga_state} row.
 *
 * <p>{@link #withTransition} creates a new snapshot with updated status and timestamp.
 *
 * <p><b>What belongs here.</b> This class ships in the Java-8 {@code api} module, so its component
 * list freezes at the first release. After that a component can be added only as a second
 * constructor standing beside the first, kept for good, and each addition quietly changes what
 * {@link #equals} means. It therefore carries only what a caller of the public API can act on.
 *
 * <p><b>Server-internal bookkeeping stops at the store; it does not ride this type.</b> Decided
 * 2026-09-12. A field that coordinates replicas, routes a notification, or otherwise serves the
 * engine rather than the caller is read by the engine from its own rows. The test is whether a
 * remote SDK caller could act on the value. If not, it is not a component of this class.
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
