package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaStateSnapshot;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * REST response view of a page of sagas: the snapshots plus an opaque {@code nextPageToken} to
 * fetch the following page (absent when this is the last page). The token is passed back verbatim
 * as the {@code pageToken} query parameter; a client treats it as opaque.
 */
public record SagaListResponse(List<SagaSnapshotResponse> sagas, @Nullable String nextPageToken) {

  /** Defensively copies {@code sagas} so the response is immutable. */
  public SagaListResponse {
    sagas = List.copyOf(sagas);
  }

  /**
   * Builds a response from a page of snapshots.
   *
   * @param page the page of saga state snapshots
   * @return the response view
   */
  public static SagaListResponse from(SagaPage<SagaStateSnapshot> page) {
    List<SagaSnapshotResponse> sagas =
        page.getItems().stream().map(SagaSnapshotResponse::from).toList();
    return new SagaListResponse(sagas, page.getNextPageToken());
  }
}
