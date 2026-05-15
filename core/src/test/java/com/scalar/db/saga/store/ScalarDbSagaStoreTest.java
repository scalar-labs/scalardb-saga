package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.store.SagaStore.Recoverables;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ScalarDbSagaStoreTest {

  @Mock private DistributedTransactionManager txManager;
  @Mock private DistributedTransaction tx;

  private ObjectMapper objectMapper;
  private SagaSchema schema;
  private SagaDefinitionSerializer definitionSerializer;
  private ScalarDbSagaStore store;

  @BeforeEach
  void setUp() throws TransactionException {
    objectMapper = new ObjectMapper();
    schema = new SagaSchema(4);
    definitionSerializer = new SagaDefinitionSerializer(objectMapper);
    store =
        new ScalarDbSagaStore(
            txManager, objectMapper, schema, ScalarDbSagaStoreConfig.builder().build());
    lenient().when(txManager.begin()).thenReturn(tx);
  }

  // ---------------------------------------------------------------------------
  // createSaga
  // ---------------------------------------------------------------------------

  @Test
  void createSaga_nullSagaIdGiven_generatesIdAndReturnsSnapshot() throws Exception {
    // Act
    SagaStateSnapshot result =
        store.createSaga(null, "order-saga", "engine-1", Map.of("amount", 100), "v1");

    // Assert
    assertThat(result.getSagaId()).isNotNull().isNotEmpty();
    assertThat(result.getSagaName()).isEqualTo("order-saga");
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(result.getOwnerId()).isEqualTo("engine-1");
    assertThat(result.getDefinitionVersion()).isEqualTo("v1");
    // event insert + state insert
    verify(tx, times(2)).insert(any(Insert.class));
    verify(tx).commit();
  }

  @Test
  void createSaga_validSagaIdGiven_usesProvidedId() throws Exception {
    // Act
    SagaStateSnapshot result =
        store.createSaga("my-saga-123", "order-saga", "engine-1", Map.of(), "v1");

    // Assert
    assertThat(result.getSagaId()).isEqualTo("my-saga-123");
    verify(tx).commit();
  }

  @Test
  void createSaga_invalidSagaIdGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(
            () -> store.createSaga("invalid id!", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void createSaga_commitConflictWithExistingSaga_throwsSagaAlreadyExistsException()
      throws Exception {
    // Arrange — commit conflict is not retried; lookup finds the existing saga
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    DistributedTransaction txLookup = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(txLookup);
    Result stateResult = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(txLookup.get(any(Get.class))).thenReturn(Optional.of(stateResult));

    // Act & Assert
    assertThatThrownBy(() -> store.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(SagaAlreadyExistsException.class);
  }

  @Test
  void createSaga_commitConflictNoExistingSaga_throwsSagaPersistenceException() throws Exception {
    // Arrange — commit conflict is not retried; lookup finds no saga
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    DistributedTransaction txLookup = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(txLookup);
    when(txLookup.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(() -> store.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void createSaga_payloadWithinLimit_succeeds() throws Exception {
    // Arrange
    ScalarDbSagaStore limitedStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().maxEventPayloadBytes(1024).build());

    // Act
    SagaStateSnapshot result =
        limitedStore.createSaga(null, "order", "engine-1", Map.of("k", "v"), "v1");

    // Assert
    assertThat(result.getSagaId()).isNotNull();
    verify(tx).commit();
  }

  @Test
  void createSaga_payloadExceedsLimit_throwsIllegalArgumentException() {
    // Arrange — 5-byte limit is too small for any valid payload
    ScalarDbSagaStore limitedStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().maxEventPayloadBytes(5).build());

    // Act & Assert
    assertThatThrownBy(
            () -> limitedStore.createSaga(null, "order", "engine-1", Map.of("k", "v"), "v1"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // registerDefinition / getDefinition
  // ---------------------------------------------------------------------------

  @Test
  void registerDefinition_firstRegistration_insertsDefinition() throws Exception {
    // Arrange
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    store.registerDefinition(def);

    // Assert
    verify(tx).get(any(Get.class));
    verify(tx).insert(any(Insert.class));
    verify(tx).commit();
  }

  @Test
  void registerDefinition_sameContentAlreadyExists_skipsWrite() throws Exception {
    // Arrange
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    String json = definitionSerializer.serialize(def);
    Result existingRow = mock(Result.class);
    when(existingRow.getText("definition_json")).thenReturn(json);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));

    // Act
    store.registerDefinition(def);

    // Assert
    verify(tx).get(any(Get.class));
    verify(tx, never()).insert(any(Insert.class));
    verify(tx).commit();
  }

  @Test
  void registerDefinition_differentContentSameVersion_throwsSagaDefinitionException()
      throws Exception {
    // Arrange
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    SagaDefinition differentDef =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("credit", "com.example.CreditStep")
            .add()
            .build();
    String differentJson = definitionSerializer.serialize(differentDef);
    Result existingRow = mock(Result.class);
    when(existingRow.getText("definition_json")).thenReturn(differentJson);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));

    // Act & Assert
    assertThatThrownBy(() -> store.registerDefinition(def))
        .isInstanceOf(SagaDefinitionException.class);
    verify(tx, never()).insert(any(Insert.class));
    verify(tx, never()).commit();
  }

  @Test
  void registerDefinition_unknownStatusAndVerifierFindsMatchingDefinition_succeeds()
      throws Exception {
    // Arrange — commit throws UTSE, verifier finds the same definition
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier uses a new transaction to re-read the definition
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2);
    String json = definitionSerializer.serialize(def);
    Result defResult = mock(Result.class);
    when(defResult.getText("definition_json")).thenReturn(json);
    when(tx2.get(any(Get.class))).thenReturn(Optional.of(defResult));

    // Act
    store.registerDefinition(def);

    // Assert — verifier confirmed committed (no exception)
    verify(tx2).get(any(Get.class));
  }

  @Test
  void
      registerDefinition_unknownStatusAndVerifierFindsDifferentDefinition_throwsSagaDefinitionException()
          throws Exception {
    // Arrange — commit throws UTSE, verifier finds a different definition.
    // Verifier returns empty (our insert didn't commit), retry's primary action
    // detects the conflict and throws SagaDefinitionException.
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // tx2: verifier's getDefinition — finds different content
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    // tx3: retry's primary action — finds existing different definition
    DistributedTransaction tx3 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2).thenReturn(tx3);
    SagaDefinition differentDef =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("credit", "com.example.CreditStep")
            .add()
            .build();
    String differentJson = definitionSerializer.serialize(differentDef);
    Result defResult = mock(Result.class);
    when(defResult.getText("definition_json")).thenReturn(differentJson);
    when(tx2.get(any(Get.class))).thenReturn(Optional.of(defResult));
    when(tx3.get(any(Get.class))).thenReturn(Optional.of(defResult));

    // Act & Assert
    assertThatThrownBy(() -> store.registerDefinition(def))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void getDefinition_existingDefinition_returnsDefinition() throws Exception {
    // Arrange
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    String json = definitionSerializer.serialize(def);
    Result result = mock(Result.class);
    when(result.getText("definition_json")).thenReturn(json);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<SagaDefinition> found = store.getDefinition("order-saga", "v1");

    // Assert
    assertThat(found).isPresent();
    assertThat(found.get().getName()).isEqualTo("order-saga");
    assertThat(found.get().getVersion()).isEqualTo("v1");
    assertThat(found.get().getSteps()).hasSize(1);
    assertThat(found.get().getSteps().get(0).getName()).isEqualTo("debit");
  }

  @Test
  void getDefinition_nonExistingDefinition_returnsEmpty() throws Exception {
    // Arrange
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    Optional<SagaDefinition> found = store.getDefinition("unknown", "v1");

    // Assert
    assertThat(found).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // recordStepEvent
  // ---------------------------------------------------------------------------

  @Test
  void recordStepEvent_validEventGiven_insertsAndCommits() throws Exception {
    // Arrange
    StepEvent event = StepEvent.completed(0, "debit", "{\"ok\":true}");

    // Act
    store.recordStepEvent("saga-1", 1, event);

    // Assert
    verify(tx).insert(any(Insert.class));
    verify(tx).commit();
  }

  @Test
  void recordStepEvent_commitConflict_throwsSagaPersistenceException() throws Exception {
    // Arrange — insert succeeds (OCC buffers locally), but commit fails
    doThrow(mock(CommitConflictException.class)).when(tx).commit();

    // Act & Assert
    assertThatThrownBy(() -> store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // recordStatusEvent
  // ---------------------------------------------------------------------------

  @Test
  void recordStatusEvent_validTransition_deletesOldInsertsNewAndReturnsUpdatedSnapshot()
      throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    StatusEvent event = StatusEvent.completed();
    Result existingRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));

    // Act
    SagaStateSnapshot result = store.recordStatusEvent(current, 5, event);

    // Assert
    assertThat(result.getSagaId()).isEqualTo("saga-1");
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(result.getUpdatedAt()).isAfterOrEqualTo(now);
    verify(tx).get(any(Get.class));
    // event insert + state insert
    verify(tx, times(2)).insert(any(Insert.class));
    verify(tx).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void recordStatusEvent_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(() -> store.recordStatusEvent(current, 1, StatusEvent.completed()))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void recordStatusEvent_commitConflict_throwsSagaPersistenceException() throws Exception {
    // Arrange — insert succeeds (OCC buffers locally), but commit fails
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    Result existingRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));
    doThrow(mock(CommitConflictException.class)).when(tx).commit();

    // Act & Assert
    assertThatThrownBy(() -> store.recordStatusEvent(current, 1, StatusEvent.completed()))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // getEvents
  // ---------------------------------------------------------------------------

  @SuppressWarnings("NullAway")
  @Test
  void getEvents_eventsExist_returnsSagaEvents() throws Exception {
    // Arrange
    Result r1 = mockEventResult("SAGA_STARTED", -1, null, "{\"input\":1}");
    Result r2 = mockEventResult("STEP_COMPLETED", 0, "debit", null);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r1, r2));

    // Act
    List<SagaEvent> events = store.getEvents("saga-1");

    // Assert
    assertThat(events).hasSize(2);

    assertThat(events.get(0)).isInstanceOf(StatusEvent.class);
    StatusEvent statusEvent = (StatusEvent) events.get(0);
    assertThat(statusEvent.getEventType()).isEqualTo(EventType.SAGA_STARTED);
    assertThat(statusEvent.getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(statusEvent.getPayload()).isEqualTo("{\"input\":1}");
    assertThat(statusEvent.getTimestamp()).isNotNull();

    assertThat(events.get(1)).isInstanceOf(StepEvent.class);
    StepEvent stepEvent = (StepEvent) events.get(1);
    assertThat(stepEvent.getEventType()).isEqualTo(EventType.STEP_COMPLETED);
    assertThat(stepEvent.getStepIndex()).isEqualTo(0);
    assertThat(stepEvent.getStepName()).isEqualTo("debit");
    assertThat(stepEvent.getPayload()).isNull();
    assertThat(stepEvent.getTimestamp()).isNotNull();
  }

  @Test
  void getEvents_noEvents_returnsEmptyList() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    List<SagaEvent> events = store.getEvents("saga-1");

    // Assert
    assertThat(events).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // getEventCount
  // ---------------------------------------------------------------------------

  @Test
  void getEventCount_eventsExist_returnsCount() throws Exception {
    // Arrange
    Result r1 = mock(Result.class);
    Result r2 = mock(Result.class);
    Result r3 = mock(Result.class);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r1, r2, r3));

    // Act
    int count = store.getEventCount("saga-1");

    // Assert
    assertThat(count).isEqualTo(3);
  }

  @Test
  void getEventCount_storageFailureGiven_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.getEventCount("saga-1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // getStateSnapshot
  // ---------------------------------------------------------------------------

  @Test
  void getStateSnapshot_existsInDb_returnsSnapshot() throws Exception {
    // Arrange
    Result result = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<SagaStateSnapshot> snapshot = store.getStateSnapshot("saga-1");

    // Assert
    assertThat(snapshot).isPresent();
    SagaStateSnapshot s = snapshot.get();
    assertThat(s.getSagaId()).isEqualTo("saga-1");
    assertThat(s.getSagaName()).isEqualTo("order-saga");
    assertThat(s.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(s.getOwnerId()).isEqualTo("engine-1");
    assertThat(s.getDefinitionVersion()).isEqualTo("v1");
    assertThat(s.getCreatedAt()).isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
    assertThat(s.getUpdatedAt()).isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
  }

  @Test
  void getStateSnapshot_notFound_returnsEmpty() throws Exception {
    // Arrange
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    Optional<SagaStateSnapshot> snapshot = store.getStateSnapshot("unknown");

    // Assert
    assertThat(snapshot).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // findRecoverable
  // ---------------------------------------------------------------------------

  @Test
  void findRecoverable_firstCall_scansFirstBucketAndReturnsCursor() throws Exception {
    // Arrange — findRecoverable scans 3 statuses per bucket
    Result r = mockStateResult("saga-stale", SagaStatus.RUNNING);
    when(tx.scan(any(Scan.class)))
        .thenReturn(List.of(r))
        .thenReturn(List.of())
        .thenReturn(List.of());

    // Act
    Recoverables result = store.findRecoverable(60_000, null);

    // Assert
    assertThat(result.sagas()).hasSize(1);
    assertThat(result.sagas().get(0).getSagaId()).isEqualTo("saga-stale");
    assertThat(result.hasMore()).isTrue();
    assertThat(result.nextCursor()).isNotNull();
  }

  @Test
  void findRecoverable_chainedCalls_exhaustAllBuckets() throws Exception {
    // Arrange — schema has 4 buckets; each bucket scan does 3 status queries
    // All return empty results
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act — chain 4 calls (buckets 0-3), each returning cursor for next bucket
    Recoverables r0 = store.findRecoverable(60_000, null);
    assertThat(r0.hasMore()).isTrue();

    Recoverables r1 = store.findRecoverable(60_000, r0.nextCursor());
    assertThat(r1.hasMore()).isTrue();

    Recoverables r2 = store.findRecoverable(60_000, r1.nextCursor());
    assertThat(r2.hasMore()).isTrue();

    Recoverables r3 = store.findRecoverable(60_000, r2.nextCursor());

    // Assert — last bucket returns null cursor
    assertThat(r3.hasMore()).isFalse();
    assertThat(r3.nextCursor()).isNull();
  }

  @Test
  void findRecoverable_nullCursorGiven_startsFromBucketZero() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    Recoverables result = store.findRecoverable(60_000, null);

    // Assert
    assertThat(result.sagas()).isEmpty();
    assertThat(result.hasMore()).isTrue(); // bucket 1..3 remain
  }

  @Test
  void findRecoverable_bothStatuses_returnsResultsFromAllScans() throws Exception {
    // Arrange — each status scan returns one result
    Result running = mockStateResult("saga-running", SagaStatus.RUNNING);
    Result compensating = mockStateResult("saga-compensating", SagaStatus.COMPENSATING);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(running)).thenReturn(List.of(compensating));

    // Act
    Recoverables result = store.findRecoverable(60_000, null);

    // Assert — both statuses collected
    assertThat(result.sagas()).hasSize(2);
    assertThat(result.sagas())
        .extracting(SagaStateSnapshot::getSagaId)
        .containsExactly("saga-running", "saga-compensating");
    verify(tx, times(2)).scan(any(Scan.class));
  }

  @Test
  void findRecoverable_storageFailureGiven_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.findRecoverable(60_000, null))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // claimForRecovery
  // ---------------------------------------------------------------------------

  @Test
  void claimForRecovery_validSagaGiven_returnsClaimedSnapshot() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "old-owner", "v1", now, now);
    Result currentRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(currentRow));

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(saga, "new-owner");

    // Assert
    assertThat(claimed).isPresent();
    assertThat(claimed.get().getOwnerId()).isEqualTo("new-owner");
    verify(tx).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void claimForRecovery_rowNotFound_returnsEmpty() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "old-owner", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(saga, "new-owner");

    // Assert
    assertThat(claimed).isEmpty();
  }

  @Test
  void claimForRecovery_commitConflictThenRowGone_returnsEmpty() throws Exception {
    // Arrange — commit conflict on first attempt; retry finds row gone (other replica claimed)
    Instant now = Instant.now();
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "old-owner", "v1", now, now);

    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx, tx2);

    // First attempt: row exists but commit fails
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    doThrow(mock(CommitConflictException.class)).when(tx).commit();

    // Retry: row is gone (other replica claimed it)
    when(tx2.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(saga, "new-owner");

    // Assert — returns empty because row was claimed by another replica
    assertThat(claimed).isEmpty();
  }

  @Test
  void claimForRecovery_storageFailureGiven_throwsSagaPersistenceException() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "old-owner", "v1", now, now);
    when(tx.get(any(Get.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.claimForRecovery(saga, "new-owner"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // markForRecovery
  // ---------------------------------------------------------------------------

  @Test
  void markForRecovery_existingSaga_deletesAndReinserts() throws Exception {
    // Arrange
    Result r = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(r));

    // Act
    store.markForRecovery("saga-1");

    // Assert
    verify(tx).delete(any(Delete.class));
    verify(tx).insert(any(Insert.class));
    verify(tx).commit();
  }

  @Test
  void markForRecovery_noSaga_commitsWithoutModification() throws Exception {
    // Arrange
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    store.markForRecovery("saga-1");

    // Assert
    verify(tx, never()).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void markForRecovery_transactionFails_doesNotThrow() throws Exception {
    // Arrange — markForRecovery is best-effort
    when(tx.get(any(Get.class))).thenThrow(new RuntimeException("db error"));

    // Act — should not throw
    store.markForRecovery("saga-1");
  }

  // ---------------------------------------------------------------------------
  // deleteSaga
  // ---------------------------------------------------------------------------

  @Test
  void deleteSaga_completedSaga_deletesStateAndEvents() throws Exception {
    // Arrange
    Result stateRow = mockStateResult("saga-1", SagaStatus.COMPLETED);
    Result eventRow1 = mockEventRow(0);
    Result eventRow2 = mockEventRow(1);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(stateRow));
    when(tx.scan(any(Scan.class))).thenReturn(List.of(eventRow1, eventRow2));

    // Act
    store.deleteSaga("saga-1");

    // Assert
    verify(tx, times(3)).delete(any(Delete.class)); // 1 state row + 2 event rows
    verify(tx).commit();
  }

  @Test
  void deleteSaga_compensatedSaga_deletesStateAndEvents() throws Exception {
    // Arrange
    Result stateRow = mockStateResult("saga-1", SagaStatus.COMPENSATED);
    Result eventRow1 = mockEventRow(0);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(stateRow));
    when(tx.scan(any(Scan.class))).thenReturn(List.of(eventRow1));

    // Act
    store.deleteSaga("saga-1");

    // Assert
    verify(tx, times(2)).delete(any(Delete.class)); // 1 state row + 1 event row
    verify(tx).commit();
  }

  @Test
  void deleteSaga_escalatedSaga_deletesStateAndEvents() throws Exception {
    // Arrange
    Result stateRow = mockStateResult("saga-1", SagaStatus.ESCALATED);
    Result eventRow1 = mockEventRow(0);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(stateRow));
    when(tx.scan(any(Scan.class))).thenReturn(List.of(eventRow1));

    // Act
    store.deleteSaga("saga-1");

    // Assert
    verify(tx, times(2)).delete(any(Delete.class)); // 1 state row + 1 event row
    verify(tx).commit();
  }

  @Test
  void deleteSaga_runningSaga_throwsIllegalStateException() throws Exception {
    // Arrange
    Result stateRow = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(stateRow));

    // Act & Assert
    assertThatThrownBy(() -> store.deleteSaga("saga-1")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void deleteSaga_compensatingSaga_throwsIllegalStateException() throws Exception {
    // Arrange
    Result stateRow = mockStateResult("saga-1", SagaStatus.COMPENSATING);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(stateRow));

    // Act & Assert
    assertThatThrownBy(() -> store.deleteSaga("saga-1")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void deleteSaga_transactionFails_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.get(any(Get.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.deleteSaga("saga-1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // Definition serialization round-trip
  // ---------------------------------------------------------------------------

  @SuppressWarnings("NullAway")
  @Test
  void registerAndGetDefinition_withRetryPolicy_roundTripsCorrectly() throws Exception {
    // Arrange — use SAGA mode (TCC doesn't allow pivot=true)
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga", SagaMode.SAGA)
            .version("v2")
            .timeoutMillis(30_000)
            .defaultRetryPolicy(
                RetryPolicy.newBuilder()
                    .maxAttempts(3)
                    .initialIntervalMillis(100)
                    .backoffMultiplier(2.0)
                    .maxIntervalMillis(5000)
                    .build())
            .step("debit", "com.example.DebitStep")
            .retryPolicy(RetryPolicy.newBuilder().maxAttempts(5).initialIntervalMillis(200).build())
            .add()
            .step("notify", "com.example.NotifyStep")
            .timeoutMillis(10_000)
            .add()
            .build();

    // Mock GET for retrieval
    String json = definitionSerializer.serialize(def);
    Result result = mock(Result.class);
    when(result.getText("definition_json")).thenReturn(json);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<SagaDefinition> found = store.getDefinition("order-saga", "v2");

    // Assert
    assertThat(found).isPresent();
    SagaDefinition roundTripped = found.get();
    assertThat(roundTripped.getName()).isEqualTo("order-saga");
    assertThat(roundTripped.getVersion()).isEqualTo("v2");
    assertThat(roundTripped.getMode()).isEqualTo(SagaMode.SAGA);
    assertThat(roundTripped.getTimeoutMillis()).isEqualTo(30_000);
    RetryPolicy defaultRetryPolicy = roundTripped.getDefaultRetryPolicy();
    assertThat(defaultRetryPolicy).isNotNull();
    assertThat(defaultRetryPolicy.getMaxAttempts()).isEqualTo(3);
    assertThat(roundTripped.getSteps()).hasSize(2);
    assertThat(roundTripped.getSteps().get(0).getName()).isEqualTo("debit");
    RetryPolicy stepRetryPolicy = roundTripped.getSteps().get(0).getRetryPolicy();
    assertThat(stepRetryPolicy).isNotNull();
    assertThat(stepRetryPolicy.getMaxAttempts()).isEqualTo(5);
    assertThat(roundTripped.getSteps().get(1).getName()).isEqualTo("notify");
    assertThat(roundTripped.getSteps().get(1).getTimeoutMillis()).isEqualTo(10_000);
  }

  // ---------------------------------------------------------------------------
  // Event type deserialization coverage
  // ---------------------------------------------------------------------------

  @SuppressWarnings("NullAway")
  @Test
  void getEvents_allSagaLevelEventTypes_deserializesCorrectly() throws Exception {
    // Arrange
    Result r1 = mockEventResult("SAGA_STARTED", -1, null, "{\"input\":1}");
    Result r2 = mockEventResult("SAGA_COMPENSATING", -1, null, null);
    Result r3 = mockEventResult("SAGA_COMPLETED", -1, null, null);
    Result r4 = mockEventResult("SAGA_COMPENSATED", -1, null, null);
    Result r5 = mockEventResult("SAGA_ESCALATED", -1, null, "timeout");
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r1, r2, r3, r4, r5));

    // Act
    List<SagaEvent> events = store.getEvents("saga-1");

    // Assert
    assertThat(events).hasSize(5);
    assertThat(events.get(0).getEventType()).isEqualTo(EventType.SAGA_STARTED);
    assertThat(events.get(1).getEventType()).isEqualTo(EventType.SAGA_COMPENSATING);
    assertThat(events.get(2).getEventType()).isEqualTo(EventType.SAGA_COMPLETED);
    assertThat(events.get(3).getEventType()).isEqualTo(EventType.SAGA_COMPENSATED);
    assertThat(events.get(4).getEventType()).isEqualTo(EventType.SAGA_ESCALATED);
  }

  @SuppressWarnings("NullAway")
  @Test
  void getEvents_allStepLevelEventTypes_deserializesCorrectly() throws Exception {
    // Arrange
    Result r1 = mockEventResult("STEP_COMPLETED", 0, "debit", "{\"ok\":true}");
    Result r2 = mockEventResult("STEP_FAILED", 1, "credit", "{\"error\":\"timeout\"}");
    Result r3 = mockEventResult("STEP_COMPENSATED", 0, "debit", null);
    Result r4 = mockEventResult("STEP_COMPENSATION_FAILED", 1, "credit", "{\"error\":\"retry\"}");
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r1, r2, r3, r4));

    // Act
    List<SagaEvent> events = store.getEvents("saga-1");

    // Assert
    assertThat(events).hasSize(4);
    assertThat(events.get(0).getEventType()).isEqualTo(EventType.STEP_COMPLETED);
    assertThat(events.get(1).getEventType()).isEqualTo(EventType.STEP_FAILED);
    assertThat(events.get(2).getEventType()).isEqualTo(EventType.STEP_COMPENSATED);
    assertThat(events.get(3).getEventType()).isEqualTo(EventType.STEP_COMPENSATION_FAILED);
  }

  @SuppressWarnings("NullAway")
  @Test
  void getEvents_unknownSagaEventType_throwsSagaPersistenceException() throws Exception {
    // Arrange
    Result r = mockEventResult("UNKNOWN_TYPE", -1, null, null);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r));

    // Act & Assert
    assertThatThrownBy(() -> store.getEvents("saga-1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void getEvents_unknownStepEventType_throwsSagaPersistenceException() throws Exception {
    // Arrange
    Result r = mockEventResult("UNKNOWN_STEP_TYPE", 0, "step", null);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r));

    // Act & Assert
    assertThatThrownBy(() -> store.getEvents("saga-1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // findByStatusOlderThan
  // ---------------------------------------------------------------------------

  @Test
  void findByStatusOlderThan_matchingSagasExist_returnsSnapshots() throws Exception {
    // Arrange
    Instant threshold = Instant.parse("2026-01-08T00:00:00Z");
    Result r1 = mockStateResult("saga-1", SagaStatus.COMPLETED);
    Result r2 = mockStateResult("saga-2", SagaStatus.COMPLETED);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r1, r2));

    // Act
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(0, SagaStatus.COMPLETED, threshold, 100);

    // Assert
    assertThat(result).hasSize(2);
    verify(tx).commit();
  }

  @Test
  void findByStatusOlderThan_noMatchingSagas_returnsEmptyList() throws Exception {
    // Arrange
    Instant threshold = Instant.parse("2026-01-08T00:00:00Z");
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(0, SagaStatus.COMPENSATED, threshold, 100);

    // Assert
    assertThat(result).isEmpty();
    verify(tx).commit();
  }

  @Test
  void findByStatusOlderThan_transactionFails_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(
            () -> store.findByStatusOlderThan(0, SagaStatus.COMPLETED, Instant.now(), 100))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // runInTransaction — retry and exception handling
  // ---------------------------------------------------------------------------

  @Test
  void runInTransaction_crudConflictOnFirstAttempt_retriesAndSucceeds() throws Exception {
    // Arrange — first tx conflicts on insert, second tx succeeds
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2);
    doThrow(mock(CrudConflictException.class)).when(tx).insert(any(Insert.class));

    // Act
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "debit", null));

    // Assert — second transaction succeeds
    verify(tx2).insert(any(Insert.class));
    verify(tx2).commit();
  }

  @Test
  void runInTransaction_commitConflictOnFirstAttempt_retriesAndSucceeds() throws Exception {
    // Arrange — first tx commit conflicts, second tx succeeds
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2);
    doThrow(mock(CommitConflictException.class)).when(tx).commit();

    // Act
    store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "debit", null));

    // Assert — second transaction succeeds
    verify(tx2).insert(any(Insert.class));
    verify(tx2).commit();
  }

  @Test
  void runInTransaction_allRetriesExhausted_throwsSagaPersistenceException() throws Exception {
    // Arrange — configure store with 2 retries, all conflict
    ScalarDbSagaStore retryStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().transactionRetryCount(2).build());
    doThrow(mock(CrudConflictException.class)).when(tx).insert(any(Insert.class));

    // Act & Assert
    assertThatThrownBy(
            () -> retryStore.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void runInTransaction_unknownStatusWithVerifierConfirmsCommitted_returnsResult()
      throws Exception {
    // Arrange — commit throws UTSE, but verifier confirms saga was created
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // loadFromDb (used as verifier) will use a new transaction
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2);
    Result stateResult = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(tx2.get(any(Get.class))).thenReturn(Optional.of(stateResult));

    // Act
    SagaStateSnapshot result = store.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1");

    // Assert — verifier confirmed committed, returns the result
    assertThat(result.getSagaId()).isEqualTo("saga-1");
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void runInTransaction_unknownStatusWithVerifierConfirmsNotCommitted_retries() throws Exception {
    // Arrange — first tx: UTSE, verifier confirms not committed. Second tx: succeeds.
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    DistributedTransaction tx3 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2).thenReturn(tx3);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier (loadFromDb via tx2): saga not found → not committed
    when(tx2.get(any(Get.class))).thenReturn(Optional.empty());
    // Retry (tx3): succeeds

    // Act
    SagaStateSnapshot result = store.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1");

    // Assert — retried after verifier confirmed not committed
    assertThat(result.getSagaId()).isEqualTo("saga-1");
    verify(tx3).commit();
  }

  @Test
  void runInTransaction_unknownStatusWithVerifierRetriesExhausted_throwsSagaPersistenceException()
      throws Exception {
    // Arrange — UTSE on commit, all verifier retries fail
    ScalarDbSagaStore retryStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().transactionRetryCount(2).build());
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier transactions all fail
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    DistributedTransaction tx3 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2).thenReturn(tx3);
    when(tx2.get(any(Get.class))).thenThrow(mock(CrudException.class));
    when(tx3.get(any(Get.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(
            () -> retryStore.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(SagaPersistenceException.class)
        .hasMessageContaining("commit status unknown and verification failed");
  }

  @Test
  void runInTransaction_unknownStatusWithVerifierThrowsRuntimeException_propagatesImmediately()
      throws Exception {
    // Arrange — UTSE on commit, verifier throws a non-SagaPersistenceException RuntimeException.
    // Should propagate immediately without retrying.
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    IllegalStateException verifierError = new IllegalStateException("unexpected state");
    ScalarDbSagaStore spyStore =
        new ScalarDbSagaStore(
            txManager, objectMapper, schema, ScalarDbSagaStoreConfig.builder().build());

    // Act & Assert
    assertThatThrownBy(
            () ->
                spyStore.runInTransaction(
                    tx -> Boolean.TRUE,
                    () -> {
                      throw verifierError;
                    },
                    "test operation"))
        .isSameAs(verifierError);
    // Only one transaction attempt — no retries after RuntimeException from verifier
    verify(txManager, times(1)).begin();
  }

  @Test
  void runInTransaction_unknownStatusReadOnly_retriesWholeTransaction() throws Exception {
    // Arrange — first tx: UTSE on commit (read-only, null verifier). Second tx: succeeds.
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    when(tx2.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    List<SagaEvent> events = store.getEvents("saga-1");

    // Assert — retried the whole transaction
    assertThat(events).isEmpty();
    verify(tx2).commit();
  }

  @Test
  void runInTransaction_crudException_retriesPerScalarDbGuidance() throws Exception {
    // Arrange — CrudException (TransactionException) is retried per ScalarDB API guide
    doThrow(mock(CrudException.class)).when(tx).insert(any(Insert.class));

    // Act & Assert
    assertThatThrownBy(() -> store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .isInstanceOf(SagaPersistenceException.class);
    // Should retry all attempts (default 3)
    verify(txManager, times(3)).begin();
  }

  // ---------------------------------------------------------------------------
  // ScalarDbSagaStoreConfig
  // ---------------------------------------------------------------------------

  @Test
  void build_withDefaults_hasDefaultMaxEventPayloadBytes() {
    // Act
    ScalarDbSagaStoreConfig config = ScalarDbSagaStoreConfig.builder().build();

    // Assert
    assertThat(config.getMaxEventPayloadBytes()).isEqualTo(0);
  }

  @Test
  void maxEventPayloadBytes_positiveValueGiven_setsValue() {
    // Act
    ScalarDbSagaStoreConfig config =
        ScalarDbSagaStoreConfig.builder().maxEventPayloadBytes(1024).build();

    // Assert
    assertThat(config.getMaxEventPayloadBytes()).isEqualTo(1024);
  }

  @Test
  void maxEventPayloadBytes_negativeValueGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreConfig.builder().maxEventPayloadBytes(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withDefaults_hasDefaultTransactionRetryCount() {
    // Act
    ScalarDbSagaStoreConfig config = ScalarDbSagaStoreConfig.builder().build();

    // Assert
    assertThat(config.getTransactionRetryCount()).isEqualTo(3);
  }

  @Test
  void transactionRetryCount_positiveValueGiven_setsValue() {
    // Act
    ScalarDbSagaStoreConfig config =
        ScalarDbSagaStoreConfig.builder().transactionRetryCount(5).build();

    // Assert
    assertThat(config.getTransactionRetryCount()).isEqualTo(5);
  }

  @Test
  void transactionRetryCount_zeroGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreConfig.builder().transactionRetryCount(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withDefaults_hasDefaultRecoveryScanLimit() {
    // Act
    ScalarDbSagaStoreConfig config = ScalarDbSagaStoreConfig.builder().build();

    // Assert
    assertThat(config.getRecoveryScanLimit()).isEqualTo(1000);
  }

  @Test
  void recoveryScanLimit_positiveValueGiven_setsValue() {
    // Act
    ScalarDbSagaStoreConfig config =
        ScalarDbSagaStoreConfig.builder().recoveryScanLimit(500).build();

    // Assert
    assertThat(config.getRecoveryScanLimit()).isEqualTo(500);
  }

  @Test
  void recoveryScanLimit_zeroGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> ScalarDbSagaStoreConfig.builder().recoveryScanLimit(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private Result mockEventResult(String eventType, int stepIndex, String stepName, String payload) {
    Result r = mock(Result.class);
    when(r.getText("event_type")).thenReturn(eventType);
    when(r.getInt("step_index")).thenReturn(stepIndex);
    when(r.isNull("step_name")).thenReturn(stepName == null);
    if (stepName != null) {
      when(r.getText("step_name")).thenReturn(stepName);
    }
    when(r.isNull("payload")).thenReturn(payload == null);
    if (payload != null) {
      when(r.getText("payload")).thenReturn(payload);
    }
    when(r.getTimestampTZ("created_at")).thenReturn(Instant.now());
    return r;
  }

  /** Creates a mock event row with only the sequence field (for deleteSaga). */
  private Result mockEventRow(int sequence) {
    Result r = mock(Result.class);
    when(r.getInt("sequence")).thenReturn(sequence);
    return r;
  }

  /**
   * Creates a mock state result with all fields using lenient stubs. Not all callers use all fields
   * (e.g., findRecoverable doesn't need bucket, deleteSaga doesn't need saga_name), so lenient
   * stubs prevent UnnecessaryStubbingException.
   */
  private Result mockStateResult(String sagaId, SagaStatus status) {
    Result r = mock(Result.class);
    lenient().when(r.getText("saga_id")).thenReturn(sagaId);
    lenient().when(r.getText("saga_name")).thenReturn("order-saga");
    lenient().when(r.getInt("status")).thenReturn(status.getStatusCode());
    lenient().when(r.getText("owner_id")).thenReturn("engine-1");
    lenient().when(r.getText("definition_version")).thenReturn("v1");
    lenient().when(r.getInt("bucket")).thenReturn(0);
    lenient()
        .when(r.getTimestampTZ("created_at"))
        .thenReturn(Instant.parse("2026-01-01T00:00:00Z"));
    lenient()
        .when(r.getTimestampTZ("updated_at"))
        .thenReturn(Instant.parse("2026-01-01T00:00:00Z"));
    return r;
  }
}
