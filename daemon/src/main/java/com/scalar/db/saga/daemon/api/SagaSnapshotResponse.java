package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaStateSnapshot;

/**
 * REST response view of a saga's current state. Timestamps are ISO-8601 strings to keep the JSON
 * representation independent of the server's JSON date handling.
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
