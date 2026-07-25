package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
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
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.SagaMode;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.store.SagaStore.OverdueParked;
import com.scalar.db.saga.store.SagaStore.Recoverables;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

@ExtendWith(MockitoExtension.class)
class ScalarDbSagaStoreTest {

  /**
   * Predictable append id injected into every {@link ScalarDbSagaStore} the tests construct. Tests
   * that want to represent "a different writer's row at the same sequence" stub the verifier's
   * {@code append_id} to a different literal (see {@link #OTHER_APPEND_ID}).
   */
  private static final String OWN_APPEND_ID = "test-append-id-own";

  private static final String OTHER_APPEND_ID = "test-append-id-other-writer";

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
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().build(),
            () -> OWN_APPEND_ID);
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
    when(txLookup.scan(any(Scan.class))).thenReturn(List.of(stateResult));

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
    when(txLookup.scan(any(Scan.class))).thenReturn(List.of());

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
        SagaDefinition.newBuilder("order-saga")
            .saga()
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
        SagaDefinition.newBuilder("order-saga")
            .saga()
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
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    SagaDefinition differentDef =
        SagaDefinition.newBuilder("order-saga")
            .saga()
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
        SagaDefinition.newBuilder("order-saga")
            .saga()
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
        SagaDefinition.newBuilder("order-saga")
            .saga()
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
        SagaDefinition.newBuilder("order-saga")
            .saga()
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
        SagaDefinition.newBuilder("order-saga")
            .saga()
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

  @Test
  void getDefinition_multipleVersionsExist_returnsLatestByRegisteredAt() throws Exception {
    // Arrange — two versions, v1 registered earlier, v2 registered later
    SagaDefinition defV2 =
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("v2")
            .step("debit", "com.example.DebitStep")
            .add()
            .step("credit", "com.example.CreditStep")
            .add()
            .build();
    Result resultV1 = mock(Result.class);
    when(resultV1.getTimestampTZ("registered_at"))
        .thenReturn(Instant.parse("2026-01-01T00:00:00Z"));
    Result resultV2 = mock(Result.class);
    when(resultV2.getText("definition_json")).thenReturn(definitionSerializer.serialize(defV2));
    when(resultV2.getTimestampTZ("registered_at"))
        .thenReturn(Instant.parse("2026-01-02T00:00:00Z"));
    when(tx.scan(any(Scan.class))).thenReturn(List.of(resultV1, resultV2));

    // Act
    Optional<SagaDefinition> found = store.getDefinition("order-saga");

    // Assert — returns v2 (latest by registered_at)
    assertThat(found).isPresent();
    assertThat(found.get().getVersion()).isEqualTo("v2");
    assertThat(found.get().getSteps()).hasSize(2);
  }

  @Test
  void getDefinition_noVersionsExist_returnsEmpty() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    Optional<SagaDefinition> found = store.getDefinition("unknown");

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
  void recordStepEvent_commitConflictExhausted_throwsSagaConcurrentModification() throws Exception {
    // Arrange — insert succeeds (OCC buffers locally), but every commit throws CommitConflict.
    // The append family reclassifies an exhausted commit-conflict as a 409 (another writer took the
    // sequence), not a 503 store failure.
    doThrow(mock(CommitConflictException.class)).when(tx).commit();

    // Act & Assert
    assertThatThrownBy(() -> store.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      recordStepEvent_unknownStatusAndVerifierFindsAnotherWritersEvent_throwsSagaConcurrentModification()
          throws Exception {
    // Arrange — the append's commit is ambiguous (unknown status) and a DIFFERENT writer's event
    // sits at the same sequence. That is a proven collision, not an unresolved commit: the verifier
    // throws SagaConcurrentModificationException (409) at once rather than reporting a retryable
    // failure. Even with a single attempt the op surfaces the conflict, since retrying would only
    // reuse the same taken sequence.
    ScalarDbSagaStore singleAttemptStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().transactionRetryCount(1).build(),
            () -> OWN_APPEND_ID);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStepEvent(
                    "saga-1", 3, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void recordStepEvent_unknownStatusAndVerifierFindsNoEvent_throwsRetryablePersistenceException()
      throws Exception {
    // Arrange — the append's commit is ambiguous (unknown status) and NO event is present at the
    // sequence, so our write did not land. This is the retry case, not a proven collision: the
    // verifier reports "not committed", the single attempt exhausts, and the op surfaces a 503,
    // not a 409 conflict.
    ScalarDbSagaStore singleAttemptStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().transactionRetryCount(1).build(),
            () -> OWN_APPEND_ID);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStepEvent(
                    "saga-1", 3, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void recordStepEvent_unknownStatusAndVerifierFindsOwnEvent_succeeds() throws Exception {
    // Arrange — commit is ambiguous but our own STEP_COMPLETED (identified by append_id) did
    // persist at the sequence, so the verifier confirms the commit and the append completes.
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));

    // Act & Assert
    assertThatCode(() -> store.recordStepEvent("saga-1", 3, StepEvent.completed(1, "charge", null)))
        .doesNotThrowAnyException();
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

    // Act — a different replica records the transition; owner_id is re-stamped to it
    SagaStateSnapshot result = store.recordStatusEvent(current, 5, event, "engine-2");

    // Assert
    assertThat(result.getSagaId()).isEqualTo("saga-1");
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(result.getOwnerId()).isEqualTo("engine-2");
    assertThat(result.getUpdatedAt()).isAfterOrEqualTo(now);
    verify(tx).get(any(Get.class));
    // event insert + state insert
    verify(tx, times(2)).insert(any(Insert.class));
    verify(tx).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void recordStatusEvent_epochUpdatedAtGiven_stampsStateRowForImmediateRecovery() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.ESCALATED, "engine-1", "v1", now, now);
    StatusEvent event = StatusEvent.reset(SagaStatus.COMPENSATING, "op", "sweep");
    Result existingRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));

    // Act — pass EPOCH so the transition and the recovery mark co-commit in one transaction
    SagaStateSnapshot result =
        store.recordStatusEvent(current, 5, event, "engine-2", Instant.EPOCH);

    // Assert — the transition applies, and the state row's recovery-scan key is EPOCH (immediate
    // pickup by the sweeper), all within a single transaction: event insert + state insert + commit
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(result.getOwnerId()).isEqualTo("engine-2");
    assertThat(result.getUpdatedAt()).isEqualTo(Instant.EPOCH);
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
    assertThatThrownBy(
            () -> store.recordStatusEvent(current, 1, StatusEvent.completed(), "engine-1"))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void recordStatusEvent_commitConflictExhausted_throwsSagaConcurrentModification()
      throws Exception {
    // Arrange — insert succeeds (OCC buffers locally), but every commit throws CommitConflict.
    // Reclassified as 409 by the append-family path, not a retryable 503, which would only send
    // the caller back to retry the same taken sequence.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    Result existingRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));
    doThrow(mock(CommitConflictException.class)).when(tx).commit();

    // Act & Assert
    assertThatThrownBy(
            () -> store.recordStatusEvent(current, 1, StatusEvent.completed(), "engine-1"))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      recordStatusEvent_unknownStatusAndVerifierFindsAnotherWritersEvent_throwsSagaConcurrentModificationException()
          throws Exception {
    // Arrange — the transition's commit is ambiguous and a DIFFERENT writer won the CK and wrote a
    // status event at the same sequence. The append-id-aware verifier must NOT mistake it for ours:
    // it reads back another writer's append_id, a proven collision, so it throws
    // SagaConcurrentModificationException at once rather than reporting a retryable failure.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    // Attempt 1: the optimistic CK check passes, then commit is ambiguous.
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier: the persisted event at the sequence has another writer's append_id.
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () -> store.recordStatusEvent(current, 5, StatusEvent.compensating(), "engine-1"))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void recordStatusEvent_unknownStatusAndVerifierFindsOwnEvent_returnsTransitionedSnapshot()
      throws Exception {
    // Arrange — commit is ambiguous but our own SAGA_COMPENSATING (identified by append_id) did
    // persist at the sequence, so the verifier confirms the commit and returns the snapshot.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction loadTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(loadTx);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    Result compensatingState = mockStateResult("saga-1", SagaStatus.COMPENSATING);
    when(loadTx.scan(any(Scan.class))).thenReturn(List.of(compensatingState));

    // Act
    SagaStateSnapshot result =
        store.recordStatusEvent(current, 5, StatusEvent.compensating(), "engine-2");

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
  }

  // ---------------------------------------------------------------------------
  // park / resumeParkedStep
  // ---------------------------------------------------------------------------

  @Test
  void park_boundedDeadlineGiven_transitionsToWaitingAndInsertsParkedRow() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));

    // Act
    SagaStateSnapshot result =
        store.park(current, 3, StepEvent.pending(1, "charge"), now.plusSeconds(600));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.WAITING);
    verify(tx).get(any(Get.class));
    // STEP_PENDING event insert + state insert + parked-row insert
    verify(tx, times(3)).insert(any(Insert.class));
    verify(tx).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void park_nullDeadlineGiven_writesNoParkedRow() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));

    // Act
    SagaStateSnapshot result = store.park(current, 3, StepEvent.pending(1, "charge"), null);

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.WAITING);
    // event insert + state insert only — no parked row
    verify(tx, times(2)).insert(any(Insert.class));
    verify(tx).commit();
  }

  @Test
  void park_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () -> store.park(current, 1, StepEvent.pending(1, "charge"), now.plusSeconds(60)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void resumeParkedStep_parkedRowExists_transitionsToRunningAndDeletesParkedRow() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    Result parkedRow = mock(Result.class);
    when(parkedRow.getTimestampTZ("parked_deadline")).thenReturn(now.plusSeconds(600));
    when(tx.scan(any(Scan.class))).thenReturn(List.of(parkedRow));

    // Act
    SagaStateSnapshot result =
        store.resumeParkedStep(
            current, 4, StepEvent.completed(1, "charge", "{\"paymentId\":\"p1\"}"));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
    // STEP_COMPLETED event insert + state insert
    verify(tx, times(2)).insert(any(Insert.class));
    // state delete + parked-row delete
    verify(tx, times(2)).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void resumeParkedStep_unboundedPark_transitionsToRunningWithoutDeletingRow() throws Exception {
    // Arrange — an unbounded park (callbackTimeoutMillis=0 and no saga-level timeout) wrote no
    // saga_parked row, so the resume finds nothing to delete
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    SagaStateSnapshot result =
        store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
    verify(tx, times(2)).insert(any(Insert.class));
    // only the state delete — no parked-row delete
    verify(tx).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void resumeParkedStep_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 1, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      resumeParkedStep_unknownStatusAndVerifierFindsAnotherWritersEvent_throwsSagaConcurrentModificationException()
          throws Exception {
    // Arrange — the callback's commit returns an unknown status, and a concurrent timeout sweep won
    // the WAITING CK and wrote its own event at the same sequence. The verifier must NOT mistake
    // that other writer's row for ours: append_id differs, a proven collision, so it throws
    // SagaConcurrentModificationException at once rather than reporting a retryable failure.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    // Attempt 1: the optimistic WAITING-CK check passes, then commit is ambiguous.
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier: the persisted event at the sequence has another writer's append_id.
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void resumeParkedStep_unknownStatusAndVerifierFindsOwnEvent_returnsRunning() throws Exception {
    // Arrange — commit is ambiguous (unknown status) but our own STEP_COMPLETED (identified by
    // append_id) did persist at the sequence, so the verifier confirms the commit and returns the
    // RUNNING snapshot.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction loadTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(loadTx);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier finds our own event by append_id, then loadStateSnapshot re-reads the state.
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    Result runningState = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(loadTx.scan(any(Scan.class))).thenReturn(List.of(runningState));

    // Act
    SagaStateSnapshot result =
        store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  // ---------------------------------------------------------------------------
  // failParkedStep
  // ---------------------------------------------------------------------------

  @Test
  void failParkedStep_toCompensating_transitionsAndDeletesParkedRow() throws Exception {
    // Arrange — pre-pivot timeout: WAITING -> COMPENSATING, STEP_FAILED, delete the parked row
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    Result parkedRow = mock(Result.class);
    when(parkedRow.getTimestampTZ("parked_deadline")).thenReturn(now.plusSeconds(600));
    when(tx.scan(any(Scan.class))).thenReturn(List.of(parkedRow));

    // Act
    SagaStateSnapshot result =
        store.failParkedStep(
            current, 4, StepEvent.failed(1, "charge", null), SagaStatus.COMPENSATING);

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    // STEP_FAILED event insert + state insert
    verify(tx, times(2)).insert(any(Insert.class));
    // state delete + parked-row delete
    verify(tx, times(2)).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void failParkedStep_toEscalatedUnboundedPark_transitionsWithoutDeletingRow() throws Exception {
    // Arrange — post-pivot timeout of an unbounded park: WAITING -> ESCALATED, no parked row
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    SagaStateSnapshot result =
        store.failParkedStep(current, 4, StepEvent.failed(1, "charge", null), SagaStatus.ESCALATED);

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.ESCALATED);
    verify(tx, times(2)).insert(any(Insert.class));
    // only the state delete — no parked-row delete
    verify(tx).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void failParkedStep_invalidTargetStatusGiven_throwsIllegalArgumentException() {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);

    // Act & Assert — only COMPENSATING / ESCALATED are valid targets
    assertThatThrownBy(
            () ->
                store.failParkedStep(
                    current, 4, StepEvent.failed(1, "charge", null), SagaStatus.RUNNING))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void failParkedStep_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange — a concurrent callback won the WAITING CK
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () ->
                store.failParkedStep(
                    current, 1, StepEvent.failed(1, "charge", null), SagaStatus.COMPENSATING))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      failParkedStep_unknownStatusAndVerifierFindsAnotherWritersEvent_throwsSagaConcurrentModificationException()
          throws Exception {
    // Symmetric to the resume case: the sweep's commit is ambiguous and a concurrent callback won
    // the WAITING CK, writing its own event at the sequence. The verifier must not accept that
    // other writer's row (different append_id) as its own — a proven collision, so it throws
    // SagaConcurrentModificationException at once.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () ->
                store.failParkedStep(
                    current, 4, StepEvent.failed(1, "charge", null), SagaStatus.COMPENSATING))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  // ---------------------------------------------------------------------------
  // redriveParkedStep
  // ---------------------------------------------------------------------------

  @Test
  void redriveParkedStep_parkedRowExists_transitionsToRunningAndDeletesParkedRow()
      throws Exception {
    // Arrange — un-park a timed-out step to re-drive it: WAITING -> RUNNING, STEP_REISSUING, clear
    // the parked row.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    Result parkedRow = mock(Result.class);
    when(parkedRow.getTimestampTZ("parked_deadline")).thenReturn(now.plusSeconds(600));
    when(tx.scan(any(Scan.class))).thenReturn(List.of(parkedRow));

    // Act
    SagaStateSnapshot result =
        store.redriveParkedStep(current, 4, StepEvent.reissuing(1, "charge"));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
    // STEP_REISSUING event insert + state insert
    verify(tx, times(2)).insert(any(Insert.class));
    // state delete + parked-row delete
    verify(tx, times(2)).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void redriveParkedStep_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange — a concurrent callback / timeout won the WAITING CK
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(() -> store.redriveParkedStep(current, 1, StepEvent.reissuing(1, "charge")))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      redriveParkedStep_unknownStatusAndVerifierFindsAnotherWritersEvent_throwsSagaConcurrentModificationException()
          throws Exception {
    // Arrange — the re-drive's commit returns an unknown status, and a concurrent callback won the
    // WAITING CK and wrote its own event at the same sequence. The verifier must NOT mistake that
    // other writer's row (different append_id) for our STEP_REISSUING: a proven collision, so it
    // throws SagaConcurrentModificationException at once rather than reporting a retryable failure.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    // Attempt 1: the optimistic WAITING-CK check passes, then commit is ambiguous.
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier: the persisted event at the sequence has another writer's append_id.
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(() -> store.redriveParkedStep(current, 4, StepEvent.reissuing(1, "charge")))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void redriveParkedStep_unknownStatusAndVerifierFindsOwnEvent_returnsRunning() throws Exception {
    // Arrange — commit is ambiguous (unknown status) but our own STEP_REISSUING (identified by
    // append_id) did persist at the sequence, so the verifier confirms the commit and returns the
    // RUNNING snapshot.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction loadTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(loadTx);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier finds our own event by append_id, then loadStateSnapshot re-reads the state.
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    Result runningState = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(loadTx.scan(any(Scan.class))).thenReturn(List.of(runningState));

    // Act
    SagaStateSnapshot result =
        store.redriveParkedStep(current, 4, StepEvent.reissuing(1, "charge"));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  // ---------------------------------------------------------------------------
  // append_id invariants — safety net for the per-append verifier fix
  // ---------------------------------------------------------------------------

  @Test
  void recordStepEvent_writesAppendIdOnTheEventInsert() throws Exception {
    // Act
    store.recordStepEvent("saga-1", 3, StepEvent.completed(1, "charge", null));

    // Assert — the persisted event insert carries our injected append_id. Without this test, a
    // refactor that drops the .textValue call from buildEventInsert would leave every verifier-
    // stubbing test green while every real write silently lost the id (verifier would always
    // read NULL and report "not us").
    assertEventInsertCarriesAppendId(tx, OWN_APPEND_ID);
  }

  @Test
  void recordStatusEvent_writesAppendIdOnTheEventInsert() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));

    // Act
    store.recordStatusEvent(current, 5, StatusEvent.compensating(), "engine-1");

    // Assert — of the two inserts (event + state), the event insert carries our append_id.
    assertEventInsertCarriesAppendId(tx, OWN_APPEND_ID);
  }

  @Test
  void park_writesAppendIdOnTheEventInsert() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));

    // Act
    store.park(current, 3, StepEvent.pending(1, "charge"), null);

    // Assert
    assertEventInsertCarriesAppendId(tx, OWN_APPEND_ID);
  }

  @Test
  void resumeParkedStep_writesAppendIdOnTheEventInsert() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "engine-1", "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null));

    // Assert
    assertEventInsertCarriesAppendId(tx, OWN_APPEND_ID);
  }

  @Test
  void recordStepEvent_retriesAcrossAttempts_useSameAppendIdAndCallSupplierOnce() throws Exception {
    // Arrange — a supplier that returns a distinct id per call. A per-attempt regression (mint
    // inside the retry loop instead of before it) would surface as diverging append_ids across the
    // captured inserts and a bumped call count.
    AtomicInteger supplierCalls = new AtomicInteger();
    ScalarDbSagaStore countingStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().transactionRetryCount(2).build(),
            () -> "test-append-id-" + supplierCalls.incrementAndGet());
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction retryTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(retryTx);
    // Attempt 1: commit is unknown; verifier says "not us" — outer loop retries.
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());
    // Attempt 2: commit-conflict, exhaust; the append-family path reclassifies as CME.
    doThrow(mock(CommitConflictException.class)).when(retryTx).commit();

    // Act
    assertThatThrownBy(
            () ->
                countingStore.recordStepEvent("saga-1", 3, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);

    // Assert — supplier invoked exactly once, and both attempts' event inserts carried that id.
    assertThat(supplierCalls.get()).isEqualTo(1);
    ArgumentCaptor<Insert> attempt1 = ArgumentCaptor.forClass(Insert.class);
    ArgumentCaptor<Insert> attempt2 = ArgumentCaptor.forClass(Insert.class);
    verify(tx).insert(attempt1.capture());
    verify(retryTx).insert(attempt2.capture());
    assertThat(requireAppendId(attempt1.getValue())).isEqualTo("test-append-id-1");
    assertThat(requireAppendId(attempt2.getValue())).isEqualTo("test-append-id-1");
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
  void getEvents_stepPendingEvent_deserializesAsStepEvent() throws Exception {
    // Arrange
    Result r = mockEventResult("STEP_PENDING", 1, "charge", null);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r));

    // Act
    List<SagaEvent> events = store.getEvents("saga-1");

    // Assert
    assertThat(events).hasSize(1);
    assertThat(events.get(0)).isInstanceOf(StepEvent.class);
    StepEvent stepEvent = (StepEvent) events.get(0);
    assertThat(stepEvent.getEventType()).isEqualTo(EventType.STEP_PENDING);
    assertThat(stepEvent.getStepIndex()).isEqualTo(1);
    assertThat(stepEvent.getStepName()).isEqualTo("charge");
    assertThat(stepEvent.getPayload()).isNull();
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
    when(tx.scan(any(Scan.class))).thenReturn(List.of(result));

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
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

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
    Recoverables result = store.findRecoverable(Instant.now(), null);

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
    Recoverables r0 = store.findRecoverable(Instant.now(), null);
    assertThat(r0.hasMore()).isTrue();

    Recoverables r1 = store.findRecoverable(Instant.now(), r0.nextCursor());
    assertThat(r1.hasMore()).isTrue();

    Recoverables r2 = store.findRecoverable(Instant.now(), r1.nextCursor());
    assertThat(r2.hasMore()).isTrue();

    Recoverables r3 = store.findRecoverable(Instant.now(), r2.nextCursor());

    // Assert — last bucket returns null cursor
    assertThat(r3.hasMore()).isFalse();
    assertThat(r3.nextCursor()).isNull();
  }

  @Test
  void findRecoverable_nullCursorGiven_startsFromBucketZero() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    Recoverables result = store.findRecoverable(Instant.now(), null);

    // Assert
    assertThat(result.sagas()).isEmpty();
    assertThat(result.hasMore()).isTrue(); // bucket 1..3 remain
  }

  @Test
  void findRecoverable_bothStatuses_returnsResultsFromAllScans() throws Exception {
    // Arrange — recovery scans RUNNING and COMPENSATING per bucket (WAITING is timed out via the
    // saga_parked index, not the staleness scan); each returns one result
    Result running = mockStateResult("saga-running", SagaStatus.RUNNING);
    Result compensating = mockStateResult("saga-compensating", SagaStatus.COMPENSATING);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(running)).thenReturn(List.of(compensating));

    // Act
    Recoverables result = store.findRecoverable(Instant.now(), null);

    // Assert — both active statuses collected
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
    assertThatThrownBy(() -> store.findRecoverable(Instant.now(), null))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // findOverdueParkedSagas
  // ---------------------------------------------------------------------------

  @Test
  void findOverdueParkedSagas_firstCall_returnsFirstBucketIdsAndCursor() throws Exception {
    // Arrange — the first bucket has two parked sagas whose deadline has passed
    Result p1 = mock(Result.class);
    when(p1.getText("saga_id")).thenReturn("saga-a");
    Result p2 = mock(Result.class);
    when(p2.getText("saga_id")).thenReturn("saga-b");
    when(tx.scan(any(Scan.class))).thenReturn(List.of(p1, p2));

    // Act — one bucket per call, starting from bucket 0
    OverdueParked result = store.findOverdueParkedSagas(Instant.now(), null);

    // Assert
    assertThat(result.sagaIds()).containsExactly("saga-a", "saga-b");
    assertThat(result.hasMore()).isTrue(); // buckets 1..3 remain
  }

  @Test
  void findOverdueParkedSagas_noneDue_returnsEmptyBatchWithCursor() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    OverdueParked result = store.findOverdueParkedSagas(Instant.now(), null);

    // Assert
    assertThat(result.sagaIds()).isEmpty();
    assertThat(result.hasMore()).isTrue();
  }

  @Test
  void findOverdueParkedSagas_chainedCalls_exhaustAllBuckets() throws Exception {
    // Arrange — 4 buckets, all empty
    when(tx.scan(any(Scan.class))).thenReturn(List.of());
    Instant now = Instant.now();

    // Act — chain through all buckets via the returned cursor
    OverdueParked r0 = store.findOverdueParkedSagas(now, null);
    OverdueParked r1 = store.findOverdueParkedSagas(now, r0.nextCursor());
    OverdueParked r2 = store.findOverdueParkedSagas(now, r1.nextCursor());
    OverdueParked r3 = store.findOverdueParkedSagas(now, r2.nextCursor());

    // Assert — last bucket returns a null cursor
    assertThat(r0.hasMore()).isTrue();
    assertThat(r3.hasMore()).isFalse();
    assertThat(r3.nextCursor()).isNull();
  }

  @Test
  void findOverdueParkedSagas_storageFailureGiven_throwsSagaPersistenceException()
      throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.findOverdueParkedSagas(Instant.now(), null))
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
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r));

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
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    store.markForRecovery("saga-1");

    // Assert
    verify(tx, never()).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void markForRecovery_transactionFails_doesNotThrow() throws Exception {
    // Arrange — markForRecovery is best-effort
    when(tx.scan(any(Scan.class))).thenThrow(new RuntimeException("db error"));

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
    when(tx.scan(scanForTable(SagaSchema.STATE_TABLE))).thenReturn(List.of(stateRow));
    when(tx.scan(scanForTable(SagaSchema.EVENTS_TABLE))).thenReturn(List.of(eventRow1, eventRow2));

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
    when(tx.scan(scanForTable(SagaSchema.STATE_TABLE))).thenReturn(List.of(stateRow));
    when(tx.scan(scanForTable(SagaSchema.EVENTS_TABLE))).thenReturn(List.of(eventRow1));

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
    when(tx.scan(scanForTable(SagaSchema.STATE_TABLE))).thenReturn(List.of(stateRow));
    when(tx.scan(scanForTable(SagaSchema.EVENTS_TABLE))).thenReturn(List.of(eventRow1));

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
    when(tx.scan(any(Scan.class))).thenReturn(List.of(stateRow));

    // Act & Assert
    assertThatThrownBy(() -> store.deleteSaga("saga-1")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void deleteSaga_compensatingSaga_throwsIllegalStateException() throws Exception {
    // Arrange
    Result stateRow = mockStateResult("saga-1", SagaStatus.COMPENSATING);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(stateRow));

    // Act & Assert
    assertThatThrownBy(() -> store.deleteSaga("saga-1")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void deleteSaga_transactionFails_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

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
        SagaDefinition.newBuilder("order-saga")
            .saga()
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

    // Act & Assert — an unreadable stored event is a permanent failure: not retryable.
    assertThatThrownBy(() -> store.getEvents("saga-1"))
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isFalse());
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
    // First bucket returns results; remaining buckets return empty
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r1, r2)).thenReturn(List.of());

    // Act
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(SagaStatus.COMPLETED, threshold, 100);

    // Assert
    assertThat(result).hasSize(2);
  }

  @Test
  void findByStatusOlderThan_noMatchingSagas_returnsEmptyList() throws Exception {
    // Arrange
    Instant threshold = Instant.parse("2026-01-08T00:00:00Z");
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(SagaStatus.COMPENSATED, threshold, 100);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  void findByStatusOlderThan_transactionFails_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.findByStatusOlderThan(SagaStatus.COMPLETED, Instant.now(), 100))
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

    // Act & Assert — a retry-exhausted store failure is transient: retryable.
    assertThatThrownBy(
            () -> retryStore.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isTrue());
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
    when(tx2.scan(any(Scan.class))).thenReturn(List.of(stateResult));

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
    when(tx2.scan(any(Scan.class))).thenReturn(List.of());
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
    when(tx2.scan(any(Scan.class))).thenThrow(mock(CrudException.class));
    when(tx3.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

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
  void
      runInTransaction_unknownStatusWithVerifierThrowsNonRetryablePersistenceException_propagatesImmediately()
          throws Exception {
    // Arrange — UTSE on commit; the verifier throws a permanent (non-retryable) persistence
    // failure. It must propagate as-is, not be retried and masked as a retryable failure.
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    SagaPersistenceException verifierError =
        SagaPersistenceException.nonRetryable("bad payload", new RuntimeException("parse"));
    ScalarDbSagaStore store2 =
        new ScalarDbSagaStore(
            txManager, objectMapper, schema, ScalarDbSagaStoreConfig.builder().build());

    // Act & Assert
    assertThatThrownBy(
            () ->
                store2.runInTransaction(
                    tx -> Boolean.TRUE,
                    () -> {
                      throw verifierError;
                    },
                    "test operation"))
        .isSameAs(verifierError)
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isFalse());
    // Only one transaction attempt — no retries after a non-retryable failure from the verifier.
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
    assertThat(config.getRecoveryScanLimit()).isEqualTo(100);
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
  // listStateSnapshots (orchestration + fail-closed token; boundary no-drop unit + full IT)
  // ---------------------------------------------------------------------------

  @Test
  void listStateSnapshots_updatedAfterAboveTimestampTzMaxGiven_throwsException() {
    // Arrange
    SagaQuery query =
        SagaQuery.newBuilder().updatedAfter(TimestampTZColumn.MAX_VALUE.plusMillis(1)).build();

    // Act & Assert
    assertThatThrownBy(() -> store.listStateSnapshots(query))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_updatedBeforeBelowTimestampTzMinGiven_throwsException() {
    // Arrange
    SagaQuery query =
        SagaQuery.newBuilder().updatedBefore(TimestampTZColumn.MIN_VALUE.minusMillis(1)).build();

    // Act & Assert
    assertThatThrownBy(() -> store.listStateSnapshots(query))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_updatedBoundsAtTimestampTzExtremesGiven_accepted() throws Exception {
    // Arrange — the inclusive endpoints of the TIMESTAMPTZ domain are valid and start a real scan
    stubScanner();

    // Act
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(
            SagaQuery.newBuilder()
                .updatedAfter(TimestampTZColumn.MIN_VALUE)
                .updatedBefore(TimestampTZColumn.MAX_VALUE)
                .build());

    // Assert
    assertThat(page.getItems()).isEmpty();
  }

  @Test
  void listStateSnapshots_emptyStore_returnsEmptyPageWithNullToken() throws Exception {
    // Arrange
    stubScanner();

    // Act
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(SagaQuery.newBuilder().status(SagaStatus.RUNNING).build());

    // Assert
    assertThat(page.getItems()).isEmpty();
    assertThat(page.hasMore()).isFalse();
    assertThat(page.getNextPageToken()).isNull();
  }

  @Test
  void listStateSnapshots_statusFilter_scansOneSlicePerBucket() throws Exception {
    // Arrange — 4 buckets, single status filter => 4 slice scanners
    stubScanner();

    // Act
    store.listStateSnapshots(SagaQuery.newBuilder().status(SagaStatus.ESCALATED).build());

    // Assert
    verify(tx, times(4)).getScanner(any(Scan.class));
  }

  @Test
  void listStateSnapshots_noStatusFilter_scansEveryStatusPerBucket() throws Exception {
    // Arrange — 4 buckets x 6 status codes => 24 slice scanners when nothing matches
    stubScanner();

    // Act
    store.listStateSnapshots(SagaQuery.newBuilder().build());

    // Assert
    verify(tx, times(4 * SagaStatus.values().length)).getScanner(any(Scan.class));
  }

  @Test
  void listStateSnapshots_pageNotFilled_returnsNullToken() throws Exception {
    // Arrange — one match total, well under the default page size
    stubScanner(mockStateResult("saga-a", SagaStatus.RUNNING));

    // Act
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(SagaQuery.newBuilder().status(SagaStatus.RUNNING).build());

    // Assert
    assertThat(page.getItems()).extracting(SagaStateSnapshot::getSagaId).containsExactly("saga-a");
    assertThat(page.getNextPageToken()).isNull();
  }

  @Test
  void listStateSnapshots_pageFilled_returnsNonNullToken() throws Exception {
    // Arrange — a full page (pageSize 1) means there may be more
    stubScanner(mockStateResult("saga-a", SagaStatus.RUNNING));

    // Act
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(
            SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(1).build());

    // Assert
    assertThat(page.getItems()).hasSize(1);
    assertThat(page.hasMore()).isTrue();
    assertThat(page.getNextPageToken()).isNotBlank();
  }

  @Test
  void listStateSnapshots_cohortStraddlesPageBoundary_completesCohortWithoutSplitting()
      throws Exception {
    // Arrange — pageSize 2, but three sagas share timestamp t2, straddling the boundary.
    Instant t1 = Instant.parse("2026-01-01T00:00:01Z");
    Instant t2 = Instant.parse("2026-01-01T00:00:02Z");
    Instant t3 = Instant.parse("2026-01-01T00:00:03Z");
    stubScanner(
        mockStateResult("a", SagaStatus.RUNNING, t1),
        mockStateResult("b", SagaStatus.RUNNING, t2),
        mockStateResult("c", SagaStatus.RUNNING, t2),
        mockStateResult("d", SagaStatus.RUNNING, t2),
        mockStateResult("e", SagaStatus.RUNNING, t3));

    // Act
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(
            SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(2).build());

    // Assert — the t2 cohort (b, c, d) is completed rather than split; e is left for the next page.
    assertThat(page.getItems())
        .extracting(SagaStateSnapshot::getSagaId)
        .containsExactly("a", "b", "c", "d");
    assertThat(page.hasMore()).isTrue();
  }

  @Test
  void listStateSnapshots_cohortLargerThanPageSize_returnsWholeCohortInOneOversizedPage()
      throws Exception {
    // Arrange — pageSize 2, but a single t1 cohort of 5 fills the whole slice with no other
    // timestamp, so the scanner drains without ever reaching a new cohort.
    Instant t1 = Instant.parse("2026-01-01T00:00:01Z");
    stubScannerHonoringRange(
        mockStateResult("a", SagaStatus.RUNNING, t1),
        mockStateResult("b", SagaStatus.RUNNING, t1),
        mockStateResult("c", SagaStatus.RUNNING, t1),
        mockStateResult("d", SagaStatus.RUNNING, t1),
        mockStateResult("e", SagaStatus.RUNNING, t1));

    // Act — page 1, then page 2 resuming from page 1's token under the same filters.
    SagaPage<SagaStateSnapshot> page1 =
        store.listStateSnapshots(
            SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(2).build());
    SagaPage<SagaStateSnapshot> page2 =
        store.listStateSnapshots(
            SagaQuery.newBuilder()
                .status(SagaStatus.RUNNING)
                .pageSize(2)
                .pageToken(page1.getNextPageToken())
                .build());

    // Assert — the cohort is never split: all 5 come back in one over-sized page.
    assertThat(page1.getItems())
        .extracting(SagaStateSnapshot::getSagaId)
        .containsExactly("a", "b", "c", "d", "e");
    // An exactly-drained slice is indistinguishable from a stopped-at-boundary one, so the page
    // still carries a token; following it costs one extra round trip and yields nothing.
    assertThat(page1.getNextPageToken()).isNotBlank();
    assertThat(page2.getItems()).isEmpty();
    assertThat(page2.getNextPageToken()).isNull();
  }

  /**
   * Guards the one case that makes {@code scanSlice} return a resume timestamp even though the
   * slice drained. That looks like a wart worth removing: track why the loop exited, and report no
   * resume point on a clean drain. Doing only that drops rows. A non-null resume timestamp is also
   * what stops the sweep, so suppressing it here lets the sweep reach the next slice with {@code
   * limit = pageSize - items.size() = 0}; a zero-limit scan then breaks on its first row before
   * recording a timestamp, reports "drained" in turn, and the sweep swallows every slice left. The
   * spurious token costs one empty round trip. Removing it costs correctness unless the zero limit
   * is handled too.
   */
  @Test
  void listStateSnapshots_sliceDrainsExactlyAtLimitWithLaterBucketPending_keepsLaterRows()
      throws Exception {
    // Arrange — pageSize 2 and bucket 0 holds exactly 2 rows, so its slice drains precisely at the
    // limit with no next cohort to break on. Bucket 1 still holds a row that must not be lost.
    Instant t1 = Instant.parse("2026-01-01T00:00:01Z");
    Instant t2 = Instant.parse("2026-01-01T00:00:02Z");
    stubScannerHonoringRange(
        mockStateResult("a", SagaStatus.RUNNING, t1),
        mockStateResult("b", SagaStatus.RUNNING, t1),
        inBucket(mockStateResult("c", SagaStatus.RUNNING, t2), 1));

    // Act — sweep every page until the token runs out.
    List<String> reassembled = new ArrayList<>();
    String token = null;
    for (int i = 0; i < 5; i++) {
      SagaPage<SagaStateSnapshot> page =
          store.listStateSnapshots(
              SagaQuery.newBuilder()
                  .status(SagaStatus.RUNNING)
                  .pageSize(2)
                  .pageToken(token)
                  .build());
      page.getItems().forEach(s -> reassembled.add(s.getSagaId()));
      token = page.getNextPageToken();
      if (token == null) {
        break;
      }
    }

    // Assert — the exact drain must not swallow bucket 1's row.
    assertThat(token).isNull();
    assertThat(reassembled).containsExactly("a", "b", "c");
  }

  @Test
  void listStateSnapshots_boundaryAcrossTwoPages_noDropNoDuplicate() throws Exception {
    // Arrange — three cohorts; the t2 cohort (b, c, d) straddles the pageSize-2 boundary. The
    // scanner honors the resume range, so page 2 sees exactly what a real backend would after the
    // cursor — this exercises the no-drop, no-duplicate boundary rather than hand-feeding page 2.
    Instant t1 = Instant.parse("2026-01-01T00:00:01Z");
    Instant t2 = Instant.parse("2026-01-01T00:00:02Z");
    Instant t3 = Instant.parse("2026-01-01T00:00:03Z");
    stubScannerHonoringRange(
        mockStateResult("a", SagaStatus.RUNNING, t1),
        mockStateResult("b", SagaStatus.RUNNING, t2),
        mockStateResult("c", SagaStatus.RUNNING, t2),
        mockStateResult("d", SagaStatus.RUNNING, t2),
        mockStateResult("e", SagaStatus.RUNNING, t3));

    // Act — page 1, then page 2 resuming from page 1's token under the same filters.
    SagaPage<SagaStateSnapshot> page1 =
        store.listStateSnapshots(
            SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(2).build());
    SagaPage<SagaStateSnapshot> page2 =
        store.listStateSnapshots(
            SagaQuery.newBuilder()
                .status(SagaStatus.RUNNING)
                .pageSize(2)
                .pageToken(page1.getNextPageToken())
                .build());

    // Assert — page 1 completes the straddling t2 cohort and reports more.
    assertThat(page1.getItems())
        .extracting(SagaStateSnapshot::getSagaId)
        .containsExactly("a", "b", "c", "d");
    assertThat(page1.getNextPageToken()).isNotBlank();
    // Page 2 resumes strictly after t2: e is not dropped, and none of a–d reappear.
    assertThat(page2.getItems()).extracting(SagaStateSnapshot::getSagaId).containsExactly("e");
    assertThat(page2.getNextPageToken()).isNull();
    // The two pages reassemble the full set, each row exactly once.
    List<String> reassembled = new ArrayList<>();
    page1.getItems().forEach(s -> reassembled.add(s.getSagaId()));
    page2.getItems().forEach(s -> reassembled.add(s.getSagaId()));
    assertThat(reassembled).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  void listStateSnapshots_malformedTokenGiven_throwsIllegalArgumentException() {
    // Act & Assert — not valid Base64URL; rejected before any scan
    assertThatThrownBy(
            () ->
                store.listStateSnapshots(
                    SagaQuery.newBuilder().pageToken("!!!not-base64!!!").build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenTooLongGiven_throwsIllegalArgumentException() {
    // Arrange — far longer than any valid cursor; rejected before Base64 decoding
    String token = "A".repeat(1000);

    // Act & Assert
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenBucketOutOfRangeGiven_throwsIllegalArgumentException() {
    // Arrange — bucket 999 with a 4-bucket schema; filter key matches the unfiltered query
    String token = encodePageToken("1", "*|-|-", 999, 0, "2026-01-01T00:00:00Z");

    // Act & Assert
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenUnknownVersionGiven_throwsIllegalArgumentException() {
    // Arrange — version "2" is not recognized
    String token = encodePageToken("2", "*|-|-", 0, 0, "2026-01-01T00:00:00Z");

    // Act & Assert
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenStatusNotInFilterGiven_throwsIllegalArgumentException() {
    // Arrange — query filters RUNNING(0) and the filter key matches, but the cursor status is
    // COMPLETED(1), outside the swept set: the defense-in-depth membership check rejects it.
    String token = encodePageToken("1", "0|-|-", 0, 1, "2026-01-01T00:00:00Z");

    // Act & Assert
    assertThatThrownBy(
            () ->
                store.listStateSnapshots(
                    SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageToken(token).build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenFromStatusFilterReusedByUnfilteredQuery_throwsException()
      throws Exception {
    // Arrange — mint a real token from a status=RUNNING query (pageSize 1 fills the page).
    stubScanner(mockStateResult("saga-a", SagaStatus.RUNNING));
    String token =
        store
            .listStateSnapshots(
                SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(1).build())
            .getNextPageToken();
    assertThat(token).isNotBlank();

    // Act & Assert — reusing it on an unfiltered query would silently skip earlier buckets and
    // status slices; the filter-key mismatch rejects it instead.
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenReusedWithDifferentUpdatedAfter_throwsException() throws Exception {
    // Arrange — mint a token from a query bounded by updatedAfter = Jan 1.
    Instant after = Instant.parse("2026-01-01T00:00:00Z");
    stubScanner(mockStateResult("saga-a", SagaStatus.RUNNING));
    String token =
        store
            .listStateSnapshots(
                SagaQuery.newBuilder()
                    .status(SagaStatus.RUNNING)
                    .updatedAfter(after)
                    .pageSize(1)
                    .build())
            .getNextPageToken();
    assertThat(token).isNotBlank();

    // Act & Assert — reusing it under a later updatedAfter would let the cursor timestamp override
    // the new lower bound and return out-of-window rows; the filter-key mismatch rejects it.
    Instant laterAfter = Instant.parse("2026-02-01T00:00:00Z");
    assertThatThrownBy(
            () ->
                store.listStateSnapshots(
                    SagaQuery.newBuilder()
                        .status(SagaStatus.RUNNING)
                        .updatedAfter(laterAfter)
                        .pageToken(token)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenReusedBySameQuery_resumesSuccessfully() throws Exception {
    // Arrange — mint a token from a status=RUNNING query, then reuse it with the SAME filters.
    stubScanner(mockStateResult("saga-a", SagaStatus.RUNNING));
    String token =
        store
            .listStateSnapshots(
                SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(1).build())
            .getNextPageToken();
    assertThat(token).isNotBlank();
    stubScanner(); // second page drains empty

    // Act — the matching filter key is accepted and the query resumes without throwing.
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(
            SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(1).pageToken(token).build());

    // Assert
    assertThat(page.getItems()).isEmpty();
    assertThat(page.getNextPageToken()).isNull();
  }

  /** Stubs {@code tx.getScanner(...)} to return a scanner that yields {@code rows} then drains. */
  private void stubScanner(Result... rows) throws Exception {
    // Build the scanner before the outer stubbing starts; nesting when() inside thenReturn(...)
    // trips Mockito's UnfinishedStubbingException.
    TransactionCrudOperable.Scanner scanner = scannerOver(List.of(rows));
    when(tx.getScanner(any(Scan.class))).thenReturn(scanner);
  }

  /**
   * Stubs {@code tx.getScanner(...)} to honor each scan's clustering-key range: a scanner yields
   * exactly the {@code master} rows in the requested {@code (bucket, status)} slice whose {@code
   * updated_at} falls within the scan's bounds, respecting {@code startInclusive}. Unlike {@link
   * #stubScanner}, this lets one dataset drive real multi-page pagination, so the resume cursor
   * genuinely excludes already-returned cohorts rather than the test hand-feeding each page.
   */
  private void stubScannerHonoringRange(Result... master) throws Exception {
    when(tx.getScanner(any(Scan.class)))
        .thenAnswer(
            invocation -> {
              Scan scan = invocation.getArgument(0);
              int bucket = scan.getPartitionKey().getIntValue(0);
              Key start = scan.getStartClusteringKey().orElseThrow();
              int status = start.getIntValue(0);
              Instant startTs = start.getTimestampTZValue(1);
              boolean startInclusive = scan.getStartInclusive();
              Instant endTs = scan.getEndClusteringKey().orElseThrow().getTimestampTZValue(1);

              List<Result> slice = new ArrayList<>();
              for (Result r : master) {
                if (r.getInt("bucket") != bucket || r.getInt("status") != status) {
                  continue;
                }
                Instant ts = r.getTimestampTZ("updated_at");
                boolean afterStart = startInclusive ? !ts.isBefore(startTs) : ts.isAfter(startTs);
                if (afterStart && !ts.isAfter(endTs)) {
                  slice.add(r);
                }
              }
              slice.sort(Comparator.comparing(r -> r.getTimestampTZ("updated_at")));
              return scannerOver(slice);
            });
  }

  /** A scanner mock that yields {@code rows} in order, then drains. */
  private static TransactionCrudOperable.Scanner scannerOver(List<Result> rows)
      throws CrudException {
    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    OngoingStubbing<Optional<Result>> stub = when(scanner.one());
    for (Result r : rows) {
      stub = stub.thenReturn(Optional.of(r));
    }
    stub.thenReturn(Optional.empty());
    return scanner;
  }

  private static String encodePageToken(
      String version, String filterKey, int bucket, int statusCode, String updatedAtIso) {
    String payload =
        String.join(
            "|",
            version,
            filterKey,
            Integer.toString(bucket),
            Integer.toString(statusCode),
            updatedAtIso);
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
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

  /**
   * Matches a {@link Scan} by its target table so stubs are independent of the order in which a
   * method issues its scans (e.g., {@code deleteSaga} scans the state table then the events table).
   */
  private static Scan scanForTable(String table) {
    return argThat(scan -> scan != null && scan.forTable().filter(table::equals).isPresent());
  }

  /**
   * Captures every {@code tx.insert(...)} on the given transaction, finds the one that carries an
   * {@code append_id} column (the event insert; only event rows have it, so this reliably picks the
   * event insert out of methods that also write state or parked rows), and asserts its value.
   */
  private static void assertEventInsertCarriesAppendId(
      DistributedTransaction tx, String expectedAppendId) throws Exception {
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx, atLeastOnce()).insert(captor.capture());
    Insert eventInsert =
        captor.getAllValues().stream()
            .filter(i -> i.getColumns().containsKey("append_id"))
            .findFirst()
            .orElseThrow(
                () -> new AssertionError("no event insert with append_id column was captured"));
    assertThat(requireAppendId(eventInsert)).isEqualTo(expectedAppendId);
  }

  /** Reads {@code append_id} from an {@link Insert}, failing loudly if the column is absent. */
  private static String requireAppendId(Insert insert) {
    return Objects.requireNonNull(insert.getColumns().get("append_id"), "append_id column missing")
        .getTextValue();
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

  /**
   * Like {@link #mockStateResult(String, SagaStatus)} but with a caller-chosen {@code updated_at}.
   */
  private Result mockStateResult(String sagaId, SagaStatus status, Instant updatedAt) {
    Result r = mockStateResult(sagaId, status);
    lenient().when(r.getTimestampTZ("updated_at")).thenReturn(updatedAt);
    return r;
  }

  /** Moves a mocked row into {@code bucket}, so one dataset can span several partitions. */
  private static Result inBucket(Result r, int bucket) {
    lenient().when(r.getInt("bucket")).thenReturn(bucket);
    return r;
  }

  // ---------------------------------------------------------------------------
  // close
  // ---------------------------------------------------------------------------

  @Test
  void close_always_closesTxManager() {
    // Act
    store.close();

    // Assert
    verify(txManager).close();
  }

  @Test
  void close_txManagerThrows_throwsSagaPersistenceException() {
    // Arrange
    doThrow(new RuntimeException("connection error")).when(txManager).close();

    // Act & Assert
    assertThatThrownBy(() -> store.close()).isInstanceOf(SagaPersistenceException.class);
  }
}
