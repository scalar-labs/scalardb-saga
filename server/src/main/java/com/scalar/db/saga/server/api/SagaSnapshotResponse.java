package com.scalar.db.saga.server.api;

import com.scalar.db.saga.api.SagaStateSnapshot;

/**
 * REST response view of a saga's current state. Timestamps are ISO-8601 strings to keep the JSON
 * representation independent of the server's JSON date handling.
 *
 * @param sagaId the saga instance id
 * @param sagaName the name of the saga definition the instance runs
 * @param status the {@code SagaStatus} name, such as {@code "RUNNING"}
 * @param definitionVersion the version of the saga definition the instance runs
 * @param createdAt when the instance was created, as an ISO-8601 instant
 * @param updatedAt when the instance's state last changed, as an ISO-8601 instant
 */
public record SagaSnapshotResponse(
    String sagaId,
    String sagaName,
    String status,
    String definitionVersion,
    String createdAt,
    String updatedAt) {

  /**
   * Builds a response from a {@link SagaStateSnapshot}.
   *
   * @param snapshot the saga state
   * @return the response view
   */
  public static SagaSnapshotResponse from(SagaStateSnapshot snapshot) {
    return new SagaSnapshotResponse(
        snapshot.getSagaId(),
        snapshot.getSagaName(),
        snapshot.getStatus().name(),
        snapshot.getDefinitionVersion(),
        snapshot.getCreatedAt().toString(),
        snapshot.getUpdatedAt().toString());
  }
}
