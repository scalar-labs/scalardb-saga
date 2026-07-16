package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test for {@link SagaStore#listStateSnapshots} against a real (SQLite-backed)
 * ScalarDB. Proves the pagination reassembles the full set with no gaps or duplicates across bucket
 * and status boundaries — the property that mock-based unit tests cannot exercise.
 */
class ScalarDbSagaStoreListIntegrationTest {

  @TempDir Path tempDir;

  private Path dbPath;
  private ScalarDbSagaStoreFactory factory;
  private SagaStore store;

  @BeforeEach
  void setUp() {
    dbPath = tempDir.resolve("saga-list-it.db");
    Properties props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + dbPath.toAbsolutePath() + "?busy_timeout=10000");
    // Multiple buckets so pagination must sweep across bucket boundaries.
    props.setProperty("scalar.db.saga.store.num_buckets", "4");
    factory = ScalarDbSagaStoreFactory.create(props); // creates the schema
    store = factory.createStore();
  }

  @AfterEach
  void tearDown() throws Exception {
    store.close();
    Files.deleteIfExists(dbPath);
  }

  @Test
  void listStateSnapshots_acrossBuckets_reassemblesFullSetWithoutGapsOrDuplicates() {
    // Arrange — seed 25 RUNNING sagas spread across the 4 buckets.
    List<String> seeded = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      String id = String.format("saga-%02d", i);
      store.createSaga(id, "order-saga", "engine-1", Map.of(), "v1");
      seeded.add(id);
    }

    // Act — page through with a small page size, following the opaque token.
    List<String> collected = collectAll(SagaStatus.RUNNING, 10);

    // Assert — every seeded saga appears exactly once.
    assertThat(collected)
        .hasSize(25)
        .containsExactlyInAnyOrderElementsOf(seeded)
        .doesNotHaveDuplicates();
  }

  @Test
  void listStateSnapshots_statusFilter_returnsOnlyMatchingStatus() {
    // Arrange — 5 RUNNING; transition 2 of them to COMPLETED.
    for (int i = 0; i < 5; i++) {
      store.createSaga("s-" + i, "order-saga", "engine-1", Map.of(), "v1");
    }
    complete("s-1");
    complete("s-3");

    // Act & Assert
    assertThat(collected(SagaStatus.COMPLETED, 100)).containsExactlyInAnyOrder("s-1", "s-3");
    assertThat(collected(SagaStatus.RUNNING, 100)).containsExactlyInAnyOrder("s-0", "s-2", "s-4");
  }

  @Test
  void listStateSnapshots_noStatusFilter_returnsAllStatuses() {
    // Arrange
    for (int i = 0; i < 4; i++) {
      store.createSaga("m-" + i, "order-saga", "engine-1", Map.of(), "v1");
    }
    complete("m-0");

    // Act — no status filter sweeps every status slice.
    List<String> all = paginate(SagaQuery::newBuilder, 2);

    // Assert
    assertThat(all).containsExactlyInAnyOrder("m-0", "m-1", "m-2", "m-3");
  }

  @Test
  void listStateSnapshots_emptyStore_returnsEmptyPageWithNullToken() {
    // Act
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(SagaQuery.newBuilder().status(SagaStatus.RUNNING).build());

    // Assert
    assertThat(page.getItems()).isEmpty();
    assertThat(page.hasMore()).isFalse();
  }

  @Test
  void listStateSnapshots_updatedBeforeWindow_boundsResults() {
    // Arrange
    for (int i = 0; i < 3; i++) {
      store.createSaga("w-" + i, "order-saga", "engine-1", Map.of(), "v1");
    }

    // Act & Assert — everything is updated at ~now.
    Instant future = Instant.now().plus(1, ChronoUnit.DAYS);
    assertThat(collectedBefore(SagaStatus.RUNNING, future, 100)).hasSize(3);
    assertThat(collectedBefore(SagaStatus.RUNNING, Instant.EPOCH, 100)).isEmpty();
  }

  @Test
  void listStateSnapshots_updatedAfterWindow_boundsResults() {
    // Arrange
    for (int i = 0; i < 3; i++) {
      store.createSaga("a-" + i, "order-saga", "engine-1", Map.of(), "v1");
    }

    // Act & Assert — everything is updated at ~now.
    Instant future = Instant.now().plus(1, ChronoUnit.DAYS);
    assertThat(collectedAfter(SagaStatus.RUNNING, Instant.EPOCH, 100)).hasSize(3);
    assertThat(collectedAfter(SagaStatus.RUNNING, future, 100)).isEmpty();
  }

  @Test
  void listStateSnapshots_malformedTokenGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                store.listStateSnapshots(
                    SagaQuery.newBuilder().pageToken("!!!not-base64!!!").build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- helpers ---

  private void complete(String sagaId) {
    SagaStateSnapshot current = store.getStateSnapshot(sagaId).orElseThrow();
    store.recordStatusEvent(current, store.getEventCount(sagaId), StatusEvent.completed());
  }

  private List<String> collected(SagaStatus status, int pageSize) {
    return collect(SagaQuery.newBuilder().status(status).pageSize(pageSize).build());
  }

  private List<String> collectedBefore(SagaStatus status, Instant updatedBefore, int pageSize) {
    return collect(
        SagaQuery.newBuilder()
            .status(status)
            .updatedBefore(updatedBefore)
            .pageSize(pageSize)
            .build());
  }

  private List<String> collectedAfter(SagaStatus status, Instant updatedAfter, int pageSize) {
    return collect(
        SagaQuery.newBuilder()
            .status(status)
            .updatedAfter(updatedAfter)
            .pageSize(pageSize)
            .build());
  }

  /** Fetches a single page (no pagination), returning the saga IDs. */
  private List<String> collect(SagaQuery query) {
    List<String> ids = new ArrayList<>();
    store.listStateSnapshots(query).getItems().forEach(s -> ids.add(s.getSagaId()));
    return ids;
  }

  /** Pages through the entire status-filtered result set following the opaque token. */
  private List<String> collectAll(SagaStatus status, int pageSize) {
    return paginate(() -> SagaQuery.newBuilder().status(status), pageSize);
  }

  /**
   * Pages through the entire result set for a fresh query built by {@code base} each iteration,
   * following the opaque token, returning all saga IDs.
   */
  private List<String> paginate(Supplier<SagaQuery.Builder> base, int pageSize) {
    List<String> ids = new ArrayList<>();
    String token = null;
    do {
      // Page size is approximate under the timestamp-cohort design (a page never splits a cohort),
      // so we assert only exact reassembly, not the per-page count.
      SagaPage<SagaStateSnapshot> page =
          store.listStateSnapshots(base.get().pageSize(pageSize).pageToken(token).build());
      page.getItems().forEach(s -> ids.add(s.getSagaId()));
      token = page.getNextPageToken();
    } while (token != null);
    return ids;
  }
}
