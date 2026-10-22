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
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.store.SagaStore.OverdueParked;
import com.scalar.db.saga.store.SagaStore.Recoverables;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
    assertThat(result.getDefinitionVersion()).isEqualTo("v1");
    verify(tx).commit();
    // The seq-0 event carries the saga's state, and its sweep row goes in alongside it
    Map<String, Insert> inserts = insertsByTable(tx);
    assertThat(inserts.keySet())
        .containsExactlyInAnyOrder(SagaSchema.EVENTS_TABLE, SagaSchema.SWEEP_TABLE);
    Insert event = Objects.requireNonNull(inserts.get(SagaSchema.EVENTS_TABLE));
    assertThat(Objects.requireNonNull(event.getColumns().get("state_slice")).getIntValue())
        .isEqualTo(SagaStatus.RUNNING.getStatusCode());
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
  void createSaga_invalidSagaIdGiven_throwsSagaIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(
            () -> store.createSaga("invalid id!", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void createSaga_commitConflictWithExistingSaga_throwsSagaAlreadyExistsException()
      throws Exception {
    // Arrange — commit conflict is not retried; lookup finds the existing saga
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    DistributedTransaction txLookup = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(txLookup);
    Result stateResult = mockStateResult("saga-1", SagaStatus.RUNNING);
    stubStateRead(txLookup, stateResult);

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
    stubStateRead(txLookup);

    // Act & Assert
    assertThatThrownBy(() -> store.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void createSaga_unknownStatusAndForeignAppendIdAtSequenceZero_throwsSagaAlreadyExists()
      throws Exception {
    // The bug this guards (todos/089). Our commit's status is unknown AND the id is already taken,
    // so the verifier's read-back finds a seq-0 event carrying somebody else's append_id. A
    // state-row verifier could not tell that from our own insert landing: it would confirm the
    // commit and return the other caller's snapshot, which the REST layer then answers 200/202
    // with. The append_id makes the difference visible, and at sequence 0 it means "this id
    // belongs to another saga", so the honest answer is the duplicate, not a retryable conflict.
    // Arrange
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    // One transaction serves every post-commit read, the way a real store does. Splitting the
    // verify and the reconciliation lookup across two mocks would leave the verifier's read
    // unstubbed, and an empty read-back passes this assertion through the not-landed arm whichever
    // verifier runs; a state-row verifier reading this same foreign row is what has to fail here.
    DistributedTransaction readTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(readTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result foreignEvent = mock(Result.class);
    when(foreignEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    Result foreignState = mockStateResult("saga-1", SagaStatus.RUNNING);
    when(readTx.get(any(Get.class))).thenReturn(Optional.of(foreignEvent));
    stubStateRead(readTx, foreignState);

    // Act & Assert — the duplicate, not SagaConcurrentModificationException: retrying an id that
    // another saga permanently owns can never succeed, so "typically transient" would misdirect.
    assertThatThrownBy(
            () -> singleAttemptStore.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(SagaAlreadyExistsException.class)
        .isNotInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void createSaga_unknownStatusAndOwnAppendIdAtSequenceZero_returnsOurSnapshot() throws Exception {
    // The other side of the same read: our own append_id proves the insert did land, so the create
    // succeeds rather than reporting a failure for work that is durably there.
    // Arrange
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction loadTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(loadTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    Result ownState = mockStateResult("saga-1", SagaStatus.RUNNING);
    stubStateRead(loadTx, ownState);

    // Act
    SagaStateSnapshot result =
        singleAttemptStore.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1");

    // Assert
    assertThat(result.getSagaId()).isEqualTo("saga-1");
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void createSaga_unknownStatusAndNoEventAtSequenceZero_throwsRetryablePersistenceException()
      throws Exception {
    // Absent means our insert did not land. With a single attempt configured there is no retry
    // left, so this surfaces as the retryable store failure — not as a duplicate, and not as a
    // success built on a row nobody wrote.
    // Arrange
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction lookupTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(lookupTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());
    stubStateRead(lookupTx);

    // Act & Assert
    assertThatThrownBy(
            () -> singleAttemptStore.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1"))
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
  void createSaga_payloadExceedsLimit_throwsSagaIllegalArgumentException() {
    // Arrange — 5-byte limit is too small for any valid payload
    ScalarDbSagaStore limitedStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().maxEventPayloadBytes(5).build());

    // Act & Assert — the typed exception, so the size and the configurable limit survive the wire
    // instead of being replaced by the mappers' fixed bare-IllegalArgumentException detail
    assertThatThrownBy(
            () -> limitedStore.createSaga(null, "order", "engine-1", Map.of("k", "v"), "v1"))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // registerDefinition / getDefinition
  // ---------------------------------------------------------------------------

  @Test
  void registerDefinition_firstRegistration_insertsDefinitionStampedWithTheClock()
      throws Exception {
    // Arrange — nothing registered under the name yet, and a clock with sub-millisecond digits so
    // the assertion also shows the stamp is truncated to what the column stores
    Instant now = Instant.parse("2026-08-26T12:00:00.123456789Z");
    ScalarDbSagaStore clockedStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().build(),
            () -> OWN_APPEND_ID,
            () -> now);
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("v1")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    clockedStore.registerDefinition(def);

    // Assert — with no earlier row the stamp is the clock itself, at millisecond precision
    verify(tx).get(any(Get.class));
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx).insert(captor.capture());
    Instant stamped =
        Objects.requireNonNull(
                captor.getValue().getColumns().get("registered_at"), "registered_at column missing")
            .getTimestampTZValue();
    assertThat(stamped).isEqualTo(now.truncatedTo(ChronoUnit.MILLIS));
    verify(tx).commit();
  }

  @Test
  void registerDefinition_clockBehindTheLatestVersion_stampsStrictlyAfterIt() throws Exception {
    // registered_at decides which version is "latest", and the value comes from whichever replica
    // serves the registration. Replica clocks disagree, so a replica running behind would stamp a
    // NEWER version with an OLDER time and lose the selection race to the version it replaces, so
    // the upgrade would silently not take and the version it replaced would keep serving.
    // Arrange — this replica's clock is a minute behind the newest existing registration
    Instant behind = Instant.parse("2026-08-26T12:00:00Z");
    Instant existingLatest = behind.plusSeconds(60);
    ScalarDbSagaStore skewedStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().build(),
            () -> OWN_APPEND_ID,
            () -> behind);
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("v2")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    Result olderRow = mock(Result.class);
    when(olderRow.getTimestampTZ("registered_at")).thenReturn(existingLatest);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());
    when(tx.scan(any(Scan.class))).thenReturn(List.of(olderRow));

    // Act
    skewedStore.registerDefinition(def);

    // Assert — strictly after, so the newer version wins the latest-version lookup
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx).insert(captor.capture());
    Instant stamped =
        Objects.requireNonNull(
                captor.getValue().getColumns().get("registered_at"), "registered_at column missing")
            .getTimestampTZValue();
    assertThat(stamped).isAfter(existingLatest);
  }

  @Test
  void registerDefinition_clockAheadByLessThanAMillisecond_stillStampsStrictlyAfter()
      throws Exception {
    // The column does not keep sub-millisecond precision. A clock a fraction of a millisecond past
    // the latest row looks strictly later, but persists as the SAME millisecond and ties it —
    // leaving the latest-version scan free to pick either row, so an upgrade can silently not
    // take and the version it replaced keeps serving.
    // Arrange
    Instant existingLatest = Instant.parse("2026-08-26T12:00:00.123Z");
    Instant aFractionLater = existingLatest.plusNanos(500_000);
    ScalarDbSagaStore store =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().build(),
            () -> OWN_APPEND_ID,
            () -> aFractionLater);
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("v2")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    Result olderRow = mock(Result.class);
    when(olderRow.getTimestampTZ("registered_at")).thenReturn(existingLatest);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());
    when(tx.scan(any(Scan.class))).thenReturn(List.of(olderRow));

    // Act
    store.registerDefinition(def);

    // Assert — a whole millisecond later, which is what the column can still tell apart
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx).insert(captor.capture());
    assertThat(
            Objects.requireNonNull(captor.getValue().getColumns().get("registered_at"))
                .getTimestampTZValue())
        .isEqualTo(existingLatest.plusMillis(1));
  }

  @Test
  void registerDefinition_clockAheadOfTheLatestVersion_stampsWallTime() throws Exception {
    // The ordinary case must not drift forward: with no skew the stamp is the clock, not
    // latest + 1ms compounding over every registration.
    // Arrange
    Instant now = Instant.parse("2026-08-26T12:00:00Z");
    ScalarDbSagaStore clockStore =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().build(),
            () -> OWN_APPEND_ID,
            () -> now);
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("v2")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    Result olderRow = mock(Result.class);
    when(olderRow.getTimestampTZ("registered_at")).thenReturn(now.minusSeconds(60));
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());
    when(tx.scan(any(Scan.class))).thenReturn(List.of(olderRow));

    // Act
    clockStore.registerDefinition(def);

    // Assert
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx).insert(captor.capture());
    assertThat(
            Objects.requireNonNull(captor.getValue().getColumns().get("registered_at"))
                .getTimestampTZValue())
        .isEqualTo(now);
  }

  @Test
  void registerDefinition_existingRowWithoutRegisteredAt_throwsSagaPersistenceException()
      throws Exception {
    // Arrange — a stored row with no stamp is corrupt (every insert writes one), so the stamp for
    // the new version cannot be placed after it
    SagaDefinition def =
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("v2")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    Result corruptRow = mock(Result.class);
    when(corruptRow.getTimestampTZ("registered_at")).thenReturn(null);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());
    when(tx.scan(any(Scan.class))).thenReturn(List.of(corruptRow));

    // Act & Assert — fails as corrupt data, and nothing is written next to it
    assertThatThrownBy(() -> store.registerDefinition(def))
        .isInstanceOf(SagaPersistenceException.class)
        .satisfies(e -> assertThat(((SagaPersistenceException) e).isRetryable()).isFalse());
    verify(tx, never()).insert(any(Insert.class));
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
  void getDefinition_equalStamps_returnsTheFirstRowInScanOrder() throws Exception {
    // Arrange — two versions with the same stamp, which only two replicas registering in the same
    // millisecond can produce. The rows arrive in clustering-key order, so the first one is the
    // lexicographically smaller version; that is the documented, accepted outcome, pinned here so
    // it cannot change silently.
    SagaDefinition defV10 =
        SagaDefinition.newBuilder("order-saga")
            .saga()
            .version("10.0")
            .step("debit", "com.example.DebitStep")
            .add()
            .build();
    Instant sameStamp = Instant.parse("2026-01-01T00:00:00Z");
    Result rowV10 = mock(Result.class);
    when(rowV10.getText("definition_json")).thenReturn(definitionSerializer.serialize(defV10));
    when(rowV10.getTimestampTZ("registered_at")).thenReturn(sameStamp);
    Result rowV3 = mock(Result.class);
    when(rowV3.getTimestampTZ("registered_at")).thenReturn(sameStamp);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(rowV10, rowV3));

    // Act
    Optional<SagaDefinition> found = store.getDefinition("order-saga");

    // Assert
    assertThat(found).isPresent();
    assertThat(found.get().getVersion()).isEqualTo("10.0");
  }

  @Test
  void getDefinition_rowWithoutRegisteredAt_throwsSagaPersistenceException() throws Exception {
    // Arrange — a single row with no stamp. Being the only row does not excuse it: the lookup
    // validates every row it ranks rather than skipping the comparison it would not need.
    Result corruptRow = mock(Result.class);
    when(corruptRow.getTimestampTZ("registered_at")).thenReturn(null);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(corruptRow));

    // Act & Assert
    assertThatThrownBy(() -> store.getDefinition("order-saga"))
        .isInstanceOf(SagaPersistenceException.class)
        .satisfies(e -> assertThat(((SagaPersistenceException) e).isRetryable()).isFalse());
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
  void recordStepEvent_validEventGiven_leavesTheStateColumnsOut() throws Exception {
    // Arrange — a step event carries no state, and writing nulls would leave tombstones on
    // Cassandra for every step
    StepEvent event = StepEvent.completed(0, "debit", "{\"ok\":true}");

    // Act
    store.recordStepEvent("saga-1", 1, event);

    // Assert
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx).insert(captor.capture());
    assertThat(captor.getValue().getColumns())
        .doesNotContainKeys(
            "state_slice", "sweep_at", "saga_name", "definition_version", "saga_created_at");
  }

  @Test
  void
      recordStepEvent_commitConflictExhaustedAndAnotherWritersEventAtSequence_throwsSagaConcurrentModification()
          throws Exception {
    // Arrange — insert succeeds (OCC buffers locally), but every commit throws CommitConflict.
    // Exhaustion alone proves only that we could not commit, so the store reads the sequence back:
    // another writer's append_id holds it, a proven collision, so this is a 409 and not a 503.
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      recordStepEvent_commitConflictExhaustedAndNoEventAtSequence_throwsRetryablePersistenceException()
          throws Exception {
    // Arrange — every commit throws CommitConflict and the sequence is still free. Two writers can
    // contend and both fail to commit, so nobody won: reporting a proven collision here would tell
    // the caller a race was lost that is still open. It stays a retryable 503 so the caller
    // retries.
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .isInstanceOf(SagaPersistenceException.class)
        .isNotInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void recordStepEvent_commitConflictExhaustedAndOwnEventAtSequence_succeeds() throws Exception {
    // Arrange — an earlier attempt's commit was in doubt and has since been rolled forward, which
    // is what makes the later attempt conflict. The read-back finds our own append_id, so the
    // append did land and the op succeeds rather than reporting someone else's win.
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));

    // Act & Assert
    assertThatCode(
            () ->
                singleAttemptStore.recordStepEvent("saga-1", 1, StepEvent.completed(0, "s", null)))
        .doesNotThrowAnyException();
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
  void recordStatusEvent_validTransition_movesTheSweepRowAndRecordsTheNewState() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    StatusEvent event = StatusEvent.completed();
    Result existingRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));

    // Act
    SagaStateSnapshot result = store.recordStatusEvent(current, 5, event, "engine-2");

    // Assert
    assertThat(result.getSagaId()).isEqualTo("saga-1");
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(result.getUpdatedAt()).isAfterOrEqualTo(now);
    verify(tx).get(any(Get.class));
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaStatus.RUNNING.getStatusCode());
    verify(tx).commit();
    Map<String, Insert> inserts = insertsByTable(tx);
    assertThat(
            Objects.requireNonNull(inserts.get(SagaSchema.SWEEP_TABLE))
                .getClusteringKey()
                .orElseThrow()
                .getIntValue(0))
        .isEqualTo(SagaStatus.COMPLETED.getStatusCode());
    Insert stateEvent = Objects.requireNonNull(inserts.get(SagaSchema.EVENTS_TABLE));
    assertThat(stateEvent.getClusteringKey().orElseThrow().getIntValue(0)).isEqualTo(5);
    assertThat(Objects.requireNonNull(stateEvent.getColumns().get("state_slice")).getIntValue())
        .isEqualTo(SagaStatus.COMPLETED.getStatusCode());
  }

  @Test
  void recordStatusEvent_waitingSagaGiven_throwsIllegalArgumentException() throws Exception {
    // Arrange — a parked saga's sweep key can be its deadline, which the snapshot does not carry
    Instant now = Instant.now();
    SagaStateSnapshot waiting =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);

    // Act & Assert
    assertThatThrownBy(
            () -> store.recordStatusEvent(waiting, 5, StatusEvent.compensating(), "engine-1"))
        .isInstanceOf(IllegalArgumentException.class);
    verify(txManager, never()).begin();
  }

  @Test
  void recordStatusEvent_epochUpdatedAtGiven_putsTheSweepRowAtEpochForImmediateRecovery()
      throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.ESCALATED, "v1", now, now);
    StatusEvent event = StatusEvent.reset(SagaStatus.COMPENSATING, "op", "sweep");
    Result existingRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(existingRow));

    // Act — pass EPOCH so the transition and the recovery mark co-commit in one transaction
    SagaStateSnapshot result =
        store.recordStatusEvent(current, 5, event, "engine-2", Instant.EPOCH);

    // Assert — the transition applies, and the saga's sweep key is EPOCH (immediate pickup by the
    // sweeper), all within a single transaction: event insert + sweep insert + commit
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () -> store.recordStatusEvent(current, 1, StatusEvent.completed(), "engine-1"))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      recordStatusEvent_commitConflictExhaustedAndAnotherWritersEventAtSequence_throwsSagaConcurrentModification()
          throws Exception {
    // Arrange — the CK pre-check passes on every attempt (we never see the other writer take the
    // row) but every commit throws CommitConflict. The read-back is what settles it: another
    // writer's append_id holds the sequence, so this is a proven collision and a 409.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStatusEvent(
                    current, 1, StatusEvent.completed(), "engine-1"))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      recordStatusEvent_commitConflictExhaustedAndNoEventAtSequence_throwsRetryablePersistenceException()
          throws Exception {
    // Arrange — same sustained contention, but the sequence is still free, so no writer won it.
    // The transition stays a retryable 503 rather than claiming a collision it cannot prove.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStatusEvent(
                    current, 1, StatusEvent.completed(), "engine-1"))
        .isInstanceOf(SagaPersistenceException.class)
        .isNotInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      recordStatusEvent_crudConflictExhaustedAndAnotherWritersEventAtSequence_throwsSagaConcurrentModification()
          throws Exception {
    // Arrange — the CK pre-check itself conflicts on every attempt, so the transition aborts during
    // CRUD rather than at commit and we never observe the other writer directly. The read-back is
    // what settles it, and it finds another writer's append_id on the sequence: a proven collision
    // and a 409, the same answer a commit conflict would have produced on the same evidence.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(CrudConflictException.class)).when(tx).get(any(Get.class));
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStatusEvent(
                    current, 1, StatusEvent.completed(), "engine-1"))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      recordStatusEvent_crudConflictExhaustedAndNoEventAtSequence_throwsRetryablePersistenceException()
          throws Exception {
    // Arrange — same sustained CRUD contention, but the sequence is still free. A CRUD conflict is
    // raised on a record another transaction has prepared and not resolved, and that writer may
    // still abort, so nobody has won yet. It stays a retryable 503.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(CrudConflictException.class)).when(tx).get(any(Get.class));
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.recordStatusEvent(
                    current, 1, StatusEvent.completed(), "engine-1"))
        .isInstanceOf(SagaPersistenceException.class)
        .isNotInstanceOf(SagaConcurrentModificationException.class);
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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    // Attempt 1: the optimistic CK check passes, then commit is ambiguous.
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));
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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction loadTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(loadTx);
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    Result compensatingState = mockStateResult("saga-1", SagaStatus.COMPENSATING);
    stubStateRead(loadTx, compensatingState);

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
  void park_boundedDeadlineGiven_movesTheSweepRowToTheDeadlineInTheTimeoutSlice() throws Exception {
    // Arrange
    Instant now = Instant.now();
    Instant deadline = now.plusSeconds(600);
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));

    // Act
    SagaStateSnapshot result = store.park(current, 3, StepEvent.pending(1, "charge"), deadline);

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.WAITING);
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaStatus.RUNNING.getStatusCode());
    verify(tx).commit();
    Map<String, Insert> inserts = insertsByTable(tx);
    Key sweepKey =
        Objects.requireNonNull(inserts.get(SagaSchema.SWEEP_TABLE))
            .getClusteringKey()
            .orElseThrow();
    assertThat(sweepKey.getIntValue(0)).isEqualTo(SagaSchema.WAITING_WITH_TIMEOUT_SLICE);
    assertThat(sweepKey.getBigIntValue(1)).isEqualTo(deadline.toEpochMilli());
    Insert event = Objects.requireNonNull(inserts.get(SagaSchema.EVENTS_TABLE));
    assertThat(Objects.requireNonNull(event.getColumns().get("state_slice")).getIntValue())
        .isEqualTo(SagaSchema.WAITING_WITH_TIMEOUT_SLICE);
    assertThat(Objects.requireNonNull(event.getColumns().get("sweep_at")).getBigIntValue())
        .isEqualTo(deadline.toEpochMilli());
  }

  @Test
  void park_nullDeadlineGiven_putsTheSweepRowInTheWaitingSlice() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));

    // Act
    SagaStateSnapshot result = store.park(current, 3, StepEvent.pending(1, "charge"), null);

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.WAITING);
    // event insert + sweep insert, in the slice without a timeout
    verify(tx, times(2)).insert(any(Insert.class));
    Insert sweep = Objects.requireNonNull(insertsByTable(tx).get(SagaSchema.SWEEP_TABLE));
    assertThat(sweep.getClusteringKey().orElseThrow().getIntValue(0))
        .isEqualTo(SagaStatus.WAITING.getStatusCode());
    verify(tx).commit();
  }

  @Test
  void park_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () -> store.park(current, 1, StepEvent.pending(1, "charge"), now.plusSeconds(60)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void resumeParkedStep_boundedPark_movesTheSweepRowOutOfTheTimeoutSlice() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result parkEvent = boundedParkEventRow(now, now.plusSeconds(600));
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));

    // Act
    SagaStateSnapshot result =
        store.resumeParkedStep(
            current, 4, StepEvent.completed(1, "charge", "{\"paymentId\":\"p1\"}"));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
    // STEP_COMPLETED event insert + sweep insert; the sweep row leaves the timeout slice
    verify(tx, times(2)).insert(any(Insert.class));
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaSchema.WAITING_WITH_TIMEOUT_SLICE);
    verify(tx).commit();
  }

  @Test
  void resumeParkedStep_unboundedPark_movesTheSweepRowOutOfTheWaitingSlice() throws Exception {
    // Arrange — an unbounded park (callbackTimeoutMillis=0 and no saga-level timeout) sits in the
    // plain WAITING slice
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));

    // Act
    SagaStateSnapshot result =
        store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
    verify(tx, times(2)).insert(any(Insert.class));
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaStatus.WAITING.getStatusCode());
    verify(tx).commit();
  }

  @Test
  void resumeParkedStep_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 1, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void resumeParkedStep_sweepRowGone_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange — the park event matches, but another writer already moved the saga's sweep row
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result parkEvent = parkEventRow(now);
    when(tx.get(getOn(SagaSchema.EVENTS_TABLE))).thenReturn(Optional.of(parkEvent));
    when(tx.get(getOn(SagaSchema.SWEEP_TABLE))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
    verify(tx, never()).insert(any(Insert.class));
  }

  @Test
  void resumeParkedStep_firstSequenceGiven_throwsSagaConcurrentModificationExceptionWithoutReading()
      throws Exception {
    // Arrange — no event precedes sequence 0, so no park can
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 0, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
    verify(tx, never()).get(any(Get.class));
  }

  @Test
  void resumeParkedStep_stepEventBeforeAndSagaMovedOn_throwsSagaConcurrentModificationException()
      throws Exception {
    // Arrange — the caller is behind: the saga resumed and later step events filled the log
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result stepEvent = stepEventRow(3, EventType.STEP_FAILED, now.plusSeconds(1));
    when(tx.get(getOn(SagaSchema.EVENTS_TABLE))).thenReturn(Optional.of(stepEvent));
    stubScanner(
        stepEvent,
        stateEventRow(2, EventType.STEP_COMPLETED, SagaStatus.RUNNING, now.plusSeconds(1)));

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void resumeParkedStep_stepEventBeforeAndSagaStillParked_throwsNonRetryablePersistenceException()
      throws Exception {
    // Arrange — a still-parked saga whose log grew by an event that changed nothing: damage, not a
    // race, so it must not be reported as one and stall the saga silently
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result stepEvent = stepEventRow(3, EventType.STEP_FAILED, now);
    when(tx.get(getOn(SagaSchema.EVENTS_TABLE))).thenReturn(Optional.of(stepEvent));
    stubScanner(stepEvent, parkEventRow(now));

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isFalse());
  }

  @Test
  void resumeParkedStep_eventBeforeIsNotAPark_throwsSagaConcurrentModificationException()
      throws Exception {
    // Arrange — the event before the caller's sequence moved the saga to RUNNING
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result running = stateEventRow(3, EventType.STEP_COMPLETED, SagaStatus.RUNNING, now);
    when(tx.get(getOn(SagaSchema.EVENTS_TABLE))).thenReturn(Optional.of(running));

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void resumeParkedStep_parkedAtAnotherTime_throwsSagaConcurrentModificationException()
      throws Exception {
    // Arrange — a later park than the one the caller saw: the saga resumed and parked again
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result laterPark = parkEventRow(now.plusSeconds(1));
    when(tx.get(getOn(SagaSchema.EVENTS_TABLE))).thenReturn(Optional.of(laterPark));

    // Act & Assert
    assertThatThrownBy(
            () -> store.resumeParkedStep(current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      resumeParkedStep_commitConflictExhaustedAndNoEventAtSequence_throwsRetryablePersistenceException()
          throws Exception {
    // Arrange — a callback under sustained contention: the parked-state pre-check keeps finding the
    // row (we never observe another writer take it) and every commit conflicts, so nothing was
    // written by anyone. This must stay a retryable 503. Reporting a proven collision would have
    // the daemon ack the participant 200 for a step that was never recorded, and an unbounded park
    // has no deadline, so nothing would ever re-deliver it.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.resumeParkedStep(
                    current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaPersistenceException.class)
        .isNotInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      resumeParkedStep_commitConflictExhaustedAndAnotherWritersEventAtSequence_throwsSagaConcurrentModificationException()
          throws Exception {
    // Arrange — same contention, but here the timeout sweep really did take the sequence. Its
    // append_id on the row is the proof, so the callback lost and 409 is truthful; the daemon's
    // idempotent 200 for a saga someone else resolved is then correct.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    ScalarDbSagaStore singleAttemptStore = singleAttemptStore();
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));
    doThrow(mock(CommitConflictException.class)).when(tx).commit();
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act & Assert
    assertThatThrownBy(
            () ->
                singleAttemptStore.resumeParkedStep(
                    current, 4, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void
      resumeParkedStep_unknownStatusAndVerifierFindsAnotherWritersEvent_throwsSagaConcurrentModificationException()
          throws Exception {
    // Arrange — the callback's commit returns an unknown status, and a concurrent timeout sweep won
    // the parked sweep row and wrote its own event at the same sequence. The verifier must NOT
    // mistake
    // that other writer's row for ours: append_id differs, a proven collision, so it throws
    // SagaConcurrentModificationException at once rather than reporting a retryable failure.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    // Attempt 1: the optimistic parked-state check passes, then commit is ambiguous.
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));
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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction loadTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(loadTx);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier finds our own event by append_id, then loadStateSnapshot re-reads the state.
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    Result runningState = mockStateResult("saga-1", SagaStatus.RUNNING);
    stubStateRead(loadTx, runningState);

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
  void failParkedStep_toCompensating_movesTheSweepRowOutOfTheTimeoutSlice() throws Exception {
    // Arrange — pre-pivot timeout: WAITING -> COMPENSATING, STEP_FAILED
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result parkEvent = boundedParkEventRow(now, now.plusSeconds(600));
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));

    // Act
    SagaStateSnapshot result =
        store.failParkedStep(
            current, 4, StepEvent.failed(1, "charge", null), SagaStatus.COMPENSATING);

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    // STEP_FAILED event insert + sweep insert; the sweep row leaves the timeout slice
    verify(tx, times(2)).insert(any(Insert.class));
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaSchema.WAITING_WITH_TIMEOUT_SLICE);
    verify(tx).commit();
  }

  @Test
  void failParkedStep_toEscalatedUnboundedPark_movesTheSweepRowOutOfTheWaitingSlice()
      throws Exception {
    // Arrange — post-pivot timeout of an unbounded park: WAITING -> ESCALATED
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));

    // Act
    SagaStateSnapshot result =
        store.failParkedStep(current, 4, StepEvent.failed(1, "charge", null), SagaStatus.ESCALATED);

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.ESCALATED);
    verify(tx, times(2)).insert(any(Insert.class));
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaStatus.WAITING.getStatusCode());
    verify(tx).commit();
  }

  @Test
  void failParkedStep_invalidTargetStatusGiven_throwsIllegalArgumentException() {
    // Arrange
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);

    // Act & Assert — only COMPENSATING / ESCALATED are valid targets
    assertThatThrownBy(
            () ->
                store.failParkedStep(
                    current, 4, StepEvent.failed(1, "charge", null), SagaStatus.RUNNING))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void failParkedStep_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange — a concurrent callback won the parked sweep row
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
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
    // the parked sweep row, writing its own event at the sequence. The verifier must not accept
    // that
    // other writer's row (different append_id) as its own — a proven collision, so it throws
    // SagaConcurrentModificationException at once.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));
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
  void redriveParkedStep_boundedPark_movesTheSweepRowOutOfTheTimeoutSlice() throws Exception {
    // Arrange — un-park a timed-out step to re-drive it: WAITING -> RUNNING, STEP_REISSUING
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result parkEvent = boundedParkEventRow(now, now.plusSeconds(600));
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));

    // Act
    SagaStateSnapshot result =
        store.redriveParkedStep(current, 4, StepEvent.reissuing(1, "charge"));

    // Assert
    assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
    // STEP_REISSUING event insert + sweep insert
    verify(tx, times(2)).insert(any(Insert.class));
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaSchema.WAITING_WITH_TIMEOUT_SLICE);
    verify(tx).commit();
  }

  @Test
  void redriveParkedStep_rowNotFound_throwsSagaConcurrentModificationException() throws Exception {
    // Arrange — a concurrent callback / timeout won the parked sweep row
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
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
    // parked sweep row and wrote its own event at the same sequence. The verifier must NOT mistake
    // that
    // other writer's row (different append_id) for our STEP_REISSUING: a proven collision, so it
    // throws SagaConcurrentModificationException at once rather than reporting a retryable failure.
    Instant now = Instant.now();
    SagaStateSnapshot current =
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    // Attempt 1: the optimistic parked-state check passes, then commit is ambiguous.
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));
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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction loadTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(loadTx);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    // Verifier finds our own event by append_id, then loadStateSnapshot re-reads the state.
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    Result runningState = mockStateResult("saga-1", SagaStatus.RUNNING);
    stubStateRead(loadTx, runningState);

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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));

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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.RUNNING, "v1", now, now);
    Result sweepRow = mock(Result.class);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(sweepRow));

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
        new SagaStateSnapshot("saga-1", "order-saga", SagaStatus.WAITING, "v1", now, now);
    Result parkEvent = parkEventRow(now);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(parkEvent));

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
    DistributedTransaction finalVerifyTx = mock(DistributedTransaction.class);
    when(txManager.begin())
        .thenReturn(tx)
        .thenReturn(verifyTx)
        .thenReturn(retryTx)
        .thenReturn(finalVerifyTx);
    // Attempt 1: commit is unknown; verifier says "not us" — outer loop retries.
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());
    // Attempt 2: commit-conflict, exhaust. The read-back finds the sequence still free, so no
    // writer won it and the op stays a retryable failure.
    doThrow(mock(CommitConflictException.class)).when(retryTx).commit();
    when(finalVerifyTx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    assertThatThrownBy(
            () ->
                countingStore.recordStepEvent("saga-1", 3, StepEvent.completed(1, "charge", null)))
        .isInstanceOf(SagaPersistenceException.class);

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

  @SuppressWarnings("NullAway")
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
  // getNewestEvent
  // ---------------------------------------------------------------------------

  @Test
  void getNewestEvent_noEvents_returnsEmpty() throws Exception {
    // Arrange
    stubScanner();

    // Act
    Optional<SagaStore.NewestEvent> newest = store.getNewestEvent("saga-1");

    // Assert
    assertThat(newest).isEmpty();
  }

  @Test
  void getNewestEvent_eventsExist_returnsNewestEventAndItsSequence() throws Exception {
    // Arrange — a step event newer than the saga's last status change
    Instant newestStamp = Instant.parse("2026-08-25T10:00:00Z");
    stubScanner(
        stepEventRow(3, EventType.STEP_COMPLETED, newestStamp),
        stateEventRow(2, EventType.SAGA_STARTED, SagaStatus.RUNNING, newestStamp.minusSeconds(1)));

    // Act
    Optional<SagaStore.NewestEvent> newest = store.getNewestEvent("saga-1");

    // Assert
    Instant updatedAt = newestStamp.minusSeconds(1);
    SagaStateSnapshot state =
        new SagaStateSnapshot(
            "saga-1",
            "order-saga",
            SagaStatus.RUNNING,
            "v1",
            Instant.parse("2026-01-01T00:00:00Z"),
            updatedAt);
    assertThat(newest)
        .contains(new SagaStore.NewestEvent(EventType.STEP_COMPLETED, newestStamp, 3, null, state));
  }

  @Test
  void getNewestEvent_eventsExist_scansNewestFirstAndStopsAtTheNewestStateRow() throws Exception {
    // Arrange — "newest" means highest sequence, and the probe must not read past the saga's state
    Instant t = Instant.parse("2026-08-25T10:00:00Z");
    TransactionCrudOperable.Scanner scanner =
        scannerOver(
            List.of(
                stepEventRow(2, EventType.STEP_COMPLETED, t),
                stateEventRow(1, EventType.SAGA_STARTED, SagaStatus.RUNNING, t),
                stateEventRow(0, EventType.SAGA_STARTED, SagaStatus.RUNNING, t)));
    when(tx.getScanner(any(Scan.class))).thenReturn(scanner);

    // Act
    store.getNewestEvent("saga-1");

    // Assert
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(tx).getScanner(captor.capture());
    assertThat(captor.getValue().getOrderings()).containsExactly(Scan.Ordering.desc("sequence"));
    verify(scanner, times(2)).one();
  }

  @Test
  void getNewestEvent_newestStateRowIsAClaim_reportsWhenItWasClaimed() throws Exception {
    // Arrange
    Instant claimedAt = Instant.parse("2026-08-25T10:00:00Z");
    stubScanner(
        stepEventRow(4, EventType.STEP_COMPLETED, claimedAt.plusSeconds(1)),
        stateEventRow(3, EventType.SAGA_CLAIMED, SagaStatus.RUNNING, claimedAt));

    // Act
    Optional<SagaStore.NewestEvent> newest = store.getNewestEvent("saga-1");

    // Assert
    assertThat(newest).map(SagaStore.NewestEvent::lastClaimedAt).contains(claimedAt);
  }

  @Test
  void getNewestEvent_statusChangedSinceTheClaim_reportsNoClaim() throws Exception {
    // Arrange — the status change after the claim is the saga's state now
    Instant t = Instant.parse("2026-08-25T10:00:00Z");
    stubScanner(
        stateEventRow(4, EventType.SAGA_COMPENSATING, SagaStatus.COMPENSATING, t),
        stateEventRow(3, EventType.SAGA_CLAIMED, SagaStatus.RUNNING, t.minusSeconds(1)));

    // Act
    Optional<SagaStore.NewestEvent> newest = store.getNewestEvent("saga-1");

    // Assert
    assertThat(newest).isPresent();
    assertThat(newest.get().lastClaimedAt()).isNull();
  }

  @Test
  void getNewestEvent_eventsButNoneCarriesTheState_throwsNonRetryablePersistenceException()
      throws Exception {
    // Arrange — the store never writes such a log, so reading one is a deserialization failure
    stubScanner(stepEventRow(1, EventType.STEP_COMPLETED, Instant.parse("2026-08-25T10:00:00Z")));

    // Act & Assert
    assertThatThrownBy(() -> store.getNewestEvent("saga-1"))
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isFalse());
  }

  @Test
  void getNewestEvent_newestEventHasNoTimestamp_returnsEpoch() throws Exception {
    // Arrange
    // Unreachable through this store, which stamps created_at on every append. Reporting the epoch
    // keeps the saga claimable: one that cannot report progress must not become unclaimable.
    Result stampless = stepEventRow(1, EventType.STEP_COMPLETED, Instant.EPOCH);
    when(stampless.getTimestampTZ("created_at")).thenReturn(null);
    stubScanner(
        stampless, stateEventRow(0, EventType.SAGA_STARTED, SagaStatus.RUNNING, Instant.now()));

    // Act
    Optional<SagaStore.NewestEvent> newest = store.getNewestEvent("saga-1");

    // Assert
    assertThat(newest).map(SagaStore.NewestEvent::createdAt).contains(Instant.EPOCH);
  }

  @Test
  void getNewestEvent_unknownEventType_throwsSagaPersistenceException() throws Exception {
    // Arrange
    // A rolling upgrade can leave an older replica reading a type only a newer one writes. The
    // probe has to translate it the way the full row mapper does; a raw IllegalArgumentException
    // would escape this method unwrapped, on every pass, for as long as that saga is a candidate.
    Instant t = Instant.parse("2026-08-25T10:00:00Z");
    Result newest = stepEventRow(1, EventType.STEP_COMPLETED, t);
    when(newest.getText("event_type")).thenReturn("A_TYPE_FROM_A_NEWER_VERSION");
    stubScanner(newest, stateEventRow(0, EventType.SAGA_STARTED, SagaStatus.RUNNING, t));

    // Act & Assert — an unreadable stored event is a permanent failure: not retryable.
    assertThatThrownBy(() -> store.getNewestEvent("saga-1"))
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isFalse());
  }

  @Test
  void getNewestEvent_storageFailureGiven_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.getScanner(any(Scan.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.getNewestEvent("saga-1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // getStateSnapshot
  // ---------------------------------------------------------------------------

  @Test
  void getStateSnapshot_existsInDb_returnsTheNewestStateRowAsTheSnapshot() throws Exception {
    // Arrange — a step event sits above the saga's last status change
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    stubScanner(
        stepEventRow(1, EventType.STEP_COMPLETED, updatedAt.plusSeconds(1)),
        stateEventRow(0, EventType.SAGA_STARTED, SagaStatus.RUNNING, updatedAt));

    // Act
    Optional<SagaStateSnapshot> snapshot = store.getStateSnapshot("saga-1");

    // Assert
    assertThat(snapshot)
        .contains(
            new SagaStateSnapshot(
                "saga-1",
                "order-saga",
                SagaStatus.RUNNING,
                "v1",
                Instant.parse("2026-01-01T00:00:00Z"),
                updatedAt));
  }

  @Test
  void getStateSnapshot_parkedWithTimeout_readsUpdatedAtFromTheEventNotTheDeadline()
      throws Exception {
    // Arrange — in the timeout slice sweep_at is the deadline; the park time is the event's own
    Instant parkedAt = Instant.parse("2026-01-02T00:00:00Z");
    Result row = stateEventRow(2, EventType.STEP_PENDING, SagaStatus.WAITING, parkedAt);
    when(row.getInt("state_slice")).thenReturn(SagaSchema.WAITING_WITH_TIMEOUT_SLICE);
    lenient().when(row.getBigInt("sweep_at")).thenReturn(parkedAt.plusSeconds(600).toEpochMilli());
    stubScanner(row);

    // Act
    Optional<SagaStateSnapshot> snapshot = store.getStateSnapshot("saga-1");

    // Assert
    assertThat(snapshot).map(SagaStateSnapshot::getStatus).contains(SagaStatus.WAITING);
    assertThat(snapshot).map(SagaStateSnapshot::getUpdatedAt).contains(parkedAt);
  }

  @Test
  void getStateSnapshot_notFound_returnsEmpty() throws Exception {
    // Arrange
    stubScanner();

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
    TransactionCrudOperable.Scanner running = scannerOver(List.of(r));
    TransactionCrudOperable.Scanner compensating = scannerOver(List.of());
    when(tx.getScanner(any(Scan.class))).thenReturn(running).thenReturn(compensating);

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
    // Arrange — schema has 4 buckets; every slice scan is empty
    when(tx.getScanner(any(Scan.class))).thenAnswer(invocation -> scannerOver(List.of()));

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
    when(tx.getScanner(any(Scan.class))).thenAnswer(invocation -> scannerOver(List.of()));

    // Act
    Recoverables result = store.findRecoverable(Instant.now(), null);

    // Assert
    assertThat(result.sagas()).isEmpty();
    assertThat(result.hasMore()).isTrue(); // bucket 1..3 remain
  }

  @Test
  void findRecoverable_bothStatuses_returnsResultsFromAllScans() throws Exception {
    // Arrange — recovery scans RUNNING and COMPENSATING per bucket (WAITING is timed out via its
    // own sweep slice, not the staleness scan); each returns one result
    Result running = mockStateResult("saga-running", SagaStatus.RUNNING);
    Result compensating = mockStateResult("saga-compensating", SagaStatus.COMPENSATING);
    TransactionCrudOperable.Scanner runningScan = scannerOver(List.of(running));
    TransactionCrudOperable.Scanner compensatingScan = scannerOver(List.of(compensating));
    when(tx.getScanner(any(Scan.class))).thenReturn(runningScan).thenReturn(compensatingScan);

    // Act
    Recoverables result = store.findRecoverable(Instant.now(), null);

    // Assert — both active statuses collected
    assertThat(result.sagas()).hasSize(2);
    assertThat(result.sagas())
        .extracting(SagaStateSnapshot::getSagaId)
        .containsExactly("saga-running", "saga-compensating");
    verify(tx, times(2)).getScanner(any(Scan.class));
  }

  /**
   * The scan limit is no longer settable from configuration, so this builder is its only remaining
   * caller and the only way to vary the page size. Pinning that the configured value caps every
   * slice scan is what keeps that seam honest — without it the setter would have no consumer at all
   * beyond a getter round-trip.
   */
  @Test
  void findRecoverable_withCustomScanLimit_capsEverySliceScanAtThatLimit() throws Exception {
    // Arrange — 3 rows in each slice, 2 allowed
    ScalarDbSagaStore limited =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().recoveryScanLimit(2).build(),
            () -> OWN_APPEND_ID);
    when(tx.getScanner(any(Scan.class)))
        .thenAnswer(invocation -> scannerOver(staleRows("a", "b", "c")));

    // Act
    Recoverables result = limited.findRecoverable(Instant.now(), null);

    // Assert — two from each of RUNNING and COMPENSATING
    assertThat(result.sagas())
        .extracting(SagaStateSnapshot::getSagaId)
        .containsExactly("a", "b", "a", "b");
  }

  @Test
  void findRecoverable_fullSliceRescanned_continuesAfterTheLastReturnedRow() throws Exception {
    // Arrange — a claim leaves its sweep row in place, so the next pass reads the same rows first
    ScalarDbSagaStore limited =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().recoveryScanLimit(2).build(),
            () -> OWN_APPEND_ID);
    stubRunningSlice(staleRows("a", "b", "c"));

    // Act — three passes over bucket 0
    List<String> first = ids(limited.findRecoverable(Instant.now(), null));
    List<String> second = ids(limited.findRecoverable(Instant.now(), null));
    List<String> third = ids(limited.findRecoverable(Instant.now(), null));

    // Assert — the second pass skips what the first returned; the short second pass starts over
    assertThat(first).containsExactly("a", "b");
    assertThat(second).containsExactly("c");
    assertThat(third).containsExactly("a", "b");
  }

  @Test
  void findRecoverable_handOffBehindTheResumePoint_returnsItOnTheNextPass() throws Exception {
    // Arrange — the first pass fills the slice, then a saga is handed off at EPOCH
    ScalarDbSagaStore limited =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().recoveryScanLimit(2).build(),
            () -> OWN_APPEND_ID);
    List<Result> rows = new ArrayList<>(staleRows("a", "b", "c"));
    stubRunningSlice(rows);
    limited.findRecoverable(Instant.now(), null);
    rows.addAll(0, rowsAt(0L, "h"));

    // Act
    List<String> next = ids(limited.findRecoverable(Instant.now(), null));

    // Assert — the hand-off comes first, and the resumed range still continues after b
    assertThat(next).containsExactly("h", "c");
  }

  @Test
  void findRecoverable_handOffsFillTheSlice_keepsTheResumePoint() throws Exception {
    // Arrange — the first pass fills the slice, then two hand-offs fill the next one
    ScalarDbSagaStore limited =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().recoveryScanLimit(2).build(),
            () -> OWN_APPEND_ID);
    List<Result> rows = new ArrayList<>(staleRows("a", "b", "c"));
    stubRunningSlice(rows);
    limited.findRecoverable(Instant.now(), null);
    List<Result> handOffs = rowsAt(0L, "h1", "h2");
    rows.addAll(0, handOffs);

    // Act — the hand-offs are claimed off EPOCH between the passes
    List<String> second = ids(limited.findRecoverable(Instant.now(), null));
    rows.removeAll(handOffs);
    List<String> third = ids(limited.findRecoverable(Instant.now(), null));

    // Assert
    assertThat(second).containsExactly("h1", "h2");
    assertThat(third).containsExactly("c");
  }

  /** Answers each sweep scan with the RUNNING rows inside its {@code sweep_at} range. */
  private void stubRunningSlice(List<Result> rows) throws CrudException {
    when(tx.getScanner(any(Scan.class)))
        .thenAnswer(
            invocation -> {
              Scan scan = invocation.getArgument(0);
              Key start = scan.getStartClusteringKey().orElseThrow();
              long from = start.getBigIntValue(1);
              long to = scan.getEndClusteringKey().orElseThrow().getBigIntValue(1);
              if (start.getIntValue(0) != SagaStatus.RUNNING.getStatusCode()) {
                return scannerOver(List.of());
              }
              return scannerOver(
                  rows.stream()
                      .filter(r -> r.getBigInt("sweep_at") >= from && r.getBigInt("sweep_at") <= to)
                      .toList());
            });
  }

  /**
   * One call returns one scan per recoverable status, so the page the recovery manager sizes its
   * budget against is the scan limit times that count — not the scan limit alone.
   */
  @Test
  void recoveryPageSize_withCustomScanLimit_countsEveryRecoverableStatus() {
    // Arrange
    ScalarDbSagaStore limited =
        new ScalarDbSagaStore(
            txManager,
            objectMapper,
            schema,
            ScalarDbSagaStoreConfig.builder().recoveryScanLimit(7).build(),
            () -> OWN_APPEND_ID);

    // Act & Assert — 7 rows per status, RUNNING and COMPENSATING
    assertThat(limited.recoveryPageSize()).isEqualTo(14);
  }

  @Test
  void findRecoverable_storageFailureGiven_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.getScanner(any(Scan.class))).thenThrow(mock(CrudException.class));

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
  void claimForRecovery_stillScheduledSaga_appendsAClaimAfterTheProbedEventOnly() throws Exception {
    // Arrange — a stale saga keeps its sweep key; the claim is recorded in its log
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class))); // the swept sweep row

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).contains(saga);
    verify(tx).get(getOn(SagaSchema.SWEEP_TABLE));
    verify(tx, never()).delete(any(Delete.class));
    Insert event = Objects.requireNonNull(insertsByTable(tx).get(SagaSchema.EVENTS_TABLE));
    assertThat(event.getClusteringKey().orElseThrow().getIntValue(0)).isEqualTo(7);
    assertThat(Objects.requireNonNull(event.getColumns().get("event_type")).getTextValue())
        .isEqualTo(EventType.SAGA_CLAIMED.name());
    verify(tx).commit();
  }

  @Test
  void claimForRecovery_statusChangedBetweenTheSweepAndTheProbe_returnsEmptyWithoutWriting()
      throws Exception {
    // Arrange — the sweep saw RUNNING; by the probe the saga was compensating. Claiming would
    // record
    // the sweep's stale RUNNING state after the probed event.
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    SagaStateSnapshot probed = saga.withTransition(SagaStatus.COMPENSATING, updatedAt);

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(saga, probeAt(6, probed), "e-2");

    // Assert
    assertThat(claimed).isEmpty();
    verify(txManager, never()).begin();
  }

  @Test
  void claimForRecovery_updatedSinceTheSweep_returnsEmptyWithoutWriting() throws Exception {
    // Arrange — same status, but a later status change than the one the sweep saw
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    SagaStateSnapshot probed = saga.withTransition(SagaStatus.RUNNING, updatedAt.plusSeconds(1));

    // Act
    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(saga, probeAt(6, probed), "e-2");

    // Assert
    assertThat(claimed).isEmpty();
    verify(txManager, never()).begin();
  }

  @Test
  void claimForRecovery_handedOffSaga_movesTheSweepRowOffEpoch() throws Exception {
    // Arrange — a hand-off parked the sweep key at EPOCH; the claim gives it a real time again
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1",
            "order-saga",
            SagaStatus.COMPENSATING,
            "v1",
            Instant.parse("2026-01-01T00:00:00Z"),
            Instant.EPOCH);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).map(SagaStateSnapshot::getUpdatedAt).get().isNotEqualTo(Instant.EPOCH);
    assertThat(deletedSweepSlice(tx)).isEqualTo(SagaStatus.COMPENSATING.getStatusCode());
    Insert sweep = Objects.requireNonNull(insertsByTable(tx).get(SagaSchema.SWEEP_TABLE));
    assertThat(sweep.getClusteringKey().orElseThrow().getBigIntValue(1)).isPositive();
  }

  @Test
  void claimForRecovery_handedOffSagaWhoseSweepRowMoved_returnsEmpty() throws Exception {
    // Arrange — another replica claimed it first and moved the row
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1",
            "order-saga",
            SagaStatus.RUNNING,
            "v1",
            Instant.parse("2026-01-01T00:00:00Z"),
            Instant.EPOCH);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).isEmpty();
    verify(tx, never()).insert(any(Insert.class));
  }

  @Test
  void claimForRecovery_sweptSweepRowGone_returnsEmptyWithoutWriting() throws Exception {
    // Arrange — the saga was purged since the probe, and maybe recreated under its id; the purge
    // freed the sequence, so only the swept row's absence tells
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).isEmpty();
    verify(tx, never()).insert(any(Insert.class));
  }

  @Test
  void claimForRecovery_commitConflict_returnsEmptyWithoutRetrying() throws Exception {
    // Arrange — another writer appended after the probe: the claim lost, and retrying it would
    // only claim on a view that is already stale
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class))); // the swept sweep row
    doThrow(mock(CommitConflictException.class)).when(tx).commit();

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).isEmpty();
    verify(txManager).begin();
  }

  @Test
  void claimForRecovery_crudConflict_returnsEmpty() throws Exception {
    // Arrange
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class))); // the swept sweep row
    doThrow(mock(CrudConflictException.class)).when(tx).insert(any(Insert.class));

    // Act
    Optional<SagaStateSnapshot> claimed =
        store.claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).isEmpty();
  }

  @Test
  void claimForRecovery_storageFailureGiven_throwsSagaPersistenceException() throws Exception {
    // Arrange
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class))); // the swept sweep row
    doThrow(mock(CrudException.class)).when(tx).insert(any(Insert.class));

    // Act & Assert
    assertThatThrownBy(() -> store.claimForRecovery(saga, probeAt(6, saga), "engine-2"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void claimForRecovery_unknownStatusAndOwnClaimLanded_returnsTheClaim() throws Exception {
    // Arrange — the claim's commit is in doubt; its own append id at the claimed sequence proves it
    // landed, so the caller may drive the saga
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class))); // the swept sweep row
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result ownClaim = mock(Result.class);
    when(ownClaim.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(ownClaim));

    // Act
    Optional<SagaStateSnapshot> claimed =
        singleAttemptStore().claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).contains(saga);
  }

  @Test
  void claimForRecovery_unknownStatusAndAnotherWritersEventAtTheSequence_returnsEmpty()
      throws Exception {
    // Arrange — the commit is in doubt and another writer's event holds the claimed sequence: a
    // lost race, so the caller must not drive the saga
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class))); // the swept sweep row
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    Result otherWritersEvent = mock(Result.class);
    when(otherWritersEvent.getText("append_id")).thenReturn(OTHER_APPEND_ID);
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.of(otherWritersEvent));

    // Act
    Optional<SagaStateSnapshot> claimed =
        singleAttemptStore().claimForRecovery(saga, probeAt(6, saga), "engine-2");

    // Assert
    assertThat(claimed).isEmpty();
  }

  @Test
  void claimForRecovery_unknownStatusAndNothingLanded_throwsRetryablePersistenceException()
      throws Exception {
    // Arrange — the commit is in doubt and nothing sits at the claimed sequence: the claim did not
    // land, and with the attempts spent the outcome is reported, not guessed
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.RUNNING, "v1", updatedAt, updatedAt);
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class))); // the swept sweep row
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    when(verifyTx.get(any(Get.class))).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(
            () -> singleAttemptStore().claimForRecovery(saga, probeAt(6, saga), "engine-2"))
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isTrue());
  }

  @Test
  void claimForRecovery_parkedSagaGiven_throwsIllegalArgumentException() throws Exception {
    // Arrange — a parked saga is timed out by its own sweep, never claimed
    Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
    SagaStateSnapshot saga =
        new SagaStateSnapshot(
            "saga-1", "order-saga", SagaStatus.WAITING, "v1", updatedAt, updatedAt);

    // Act & Assert
    assertThatThrownBy(() -> store.claimForRecovery(saga, probeAt(6, saga), "engine-2"))
        .isInstanceOf(IllegalArgumentException.class);
    verify(txManager, never()).begin();
  }

  /** A recovery probe that found {@code sequence} as the newest event and {@code state} current. */
  private static SagaStore.NewestEvent probeAt(int sequence, SagaStateSnapshot state) {
    return new SagaStore.NewestEvent(
        EventType.STEP_COMPLETED, Instant.parse("2026-01-02T00:00:00Z"), sequence, null, state);
  }

  // ---------------------------------------------------------------------------
  // markForRecovery
  // ---------------------------------------------------------------------------

  @Test
  void markForRecovery_runningSaga_appendsAReleaseAndMovesTheSweepRowToEpoch() throws Exception {
    // Arrange
    stubScanner(
        stateEventRow(
            2, EventType.SAGA_CLAIMED, SagaStatus.RUNNING, Instant.parse("2026-01-02T00:00:00Z")));
    when(tx.get(any(Get.class))).thenReturn(Optional.of(mock(Result.class)));

    // Act
    store.markForRecovery("saga-1");

    // Assert
    verify(tx).delete(any(Delete.class));
    verify(tx).commit();
    Map<String, Insert> inserts = insertsByTable(tx);
    Insert event = Objects.requireNonNull(inserts.get(SagaSchema.EVENTS_TABLE));
    assertThat(event.getClusteringKey().orElseThrow().getIntValue(0)).isEqualTo(3);
    assertThat(Objects.requireNonNull(event.getColumns().get("event_type")).getTextValue())
        .isEqualTo(EventType.SAGA_RELEASED.name());
    Insert sweep = Objects.requireNonNull(inserts.get(SagaSchema.SWEEP_TABLE));
    assertThat(sweep.getClusteringKey().orElseThrow().getBigIntValue(1)).isZero();
  }

  @Test
  void markForRecovery_parkedSaga_writesNothing() throws Exception {
    // Arrange — only a saga being driven needs handing over
    stubScanner(
        stateEventRow(
            2, EventType.STEP_PENDING, SagaStatus.WAITING, Instant.parse("2026-01-02T00:00:00Z")));

    // Act
    store.markForRecovery("saga-1");

    // Assert
    verify(tx, never()).insert(any(Insert.class));
    verify(tx, never()).delete(any(Delete.class));
  }

  @Test
  void markForRecovery_noSaga_commitsWithoutModification() throws Exception {
    // Arrange
    stubScanner();

    // Act
    store.markForRecovery("saga-1");

    // Assert
    verify(tx, never()).delete(any(Delete.class));
    verify(tx).commit();
  }

  @Test
  void markForRecovery_transactionFails_doesNotThrow() throws Exception {
    // Arrange — markForRecovery is best-effort
    when(tx.getScanner(any(Scan.class))).thenThrow(new RuntimeException("db error"));

    // Act — should not throw
    store.markForRecovery("saga-1");
  }

  // ---------------------------------------------------------------------------
  // deleteSaga
  // ---------------------------------------------------------------------------

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
  void deleteSaga_unknownCommitAfterSagaAlreadyPurged_returnsFalse() throws Exception {
    // Arrange — no events remain (another replica already purged the saga) and the resulting no-op
    // transaction's commit status is unknown. This call deleted nothing, so budgeting callers must
    // not be told a purge happened.
    when(tx.scan(any(Scan.class))).thenReturn(List.of());
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();

    // Act
    boolean deleted = store.deleteSaga("saga-1");

    // Assert — decided from the attempt's own evidence, without another read
    assertThat(deleted).isFalse();
    verify(txManager, times(1)).begin();
  }

  @Test
  void deleteSaga_unknownCommitAndSagaGone_returnsTrue() throws Exception {
    // Arrange — the attempt saw and deleted the saga, but the commit status is unknown; the
    // verifier's fresh read finds no events, proving the delete (or an equivalent) landed
    Result stateRow = mockStateResult("saga-1", SagaStatus.COMPLETED);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(stateRow));
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    TransactionCrudOperable.Scanner empty = scannerOver(List.of());
    when(verifyTx.getScanner(any(Scan.class))).thenReturn(empty);

    // Act
    boolean deleted = store.deleteSaga("saga-1");

    // Assert
    assertThat(deleted).isTrue();
  }

  @Test
  void deleteSaga_unknownCommitAndSagaStillPresent_retriesAndDeletes() throws Exception {
    // Arrange — the attempt's unknown-status commit did not land (the verifier still finds the
    // saga), so the whole transaction is retried, and the retry deletes it and commits cleanly
    Result stateRow = mockStateResult("saga-1", SagaStatus.COMPLETED);
    DistributedTransaction verifyTx = mock(DistributedTransaction.class);
    DistributedTransaction retryTx = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(verifyTx).thenReturn(retryTx);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(stateRow));
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    TransactionCrudOperable.Scanner stillThere = scannerOver(List.of(stateRow));
    when(verifyTx.getScanner(any(Scan.class))).thenReturn(stillThere);
    when(retryTx.scan(any(Scan.class))).thenReturn(List.of(stateRow));

    // Act
    boolean deleted = store.deleteSaga("saga-1");

    // Assert
    assertThat(deleted).isTrue();
    verify(retryTx).commit();
  }

  @Test
  void deleteSaga_transactionFails_throwsSagaPersistenceException() throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenThrow(mock(CrudException.class));

    // Act & Assert
    assertThatThrownBy(() -> store.deleteSaga("saga-1"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deleteSaga_completedSaga_readsOnlyKeysAndStateColumns() throws Exception {
    // Arrange
    Result stateRow = mockStateResult("saga-1", SagaStatus.COMPLETED);
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    when(tx.scan(captor.capture())).thenReturn(List.of(stateRow));

    // Act
    store.deleteSaga("saga-1");

    // Assert — payloads stay unread
    assertThat(captor.getValue().getProjections())
        .containsExactlyInAnyOrder("saga_id", "sequence", "state_slice", "sweep_at");
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
        store.findByStatusOlderThan(SagaStatus.COMPLETED, threshold, 100, "owner-1", 0);

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
        store.findByStatusOlderThan(SagaStatus.COMPENSATED, threshold, 100, "owner-1", 0);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  void findByStatusOlderThan_consecutiveBucketScansFail_throwsInsteadOfSweepingWholeRing()
      throws Exception {
    // Arrange — every bucket's scan fails: a store outage, not a poison bucket. The breaker must
    // rethrow on the third consecutive failure so the caller's pass fails loudly once instead of
    // hammering the rest of the ring at WARN level for as long as the outage lasts.
    when(tx.scan(any(Scan.class))).thenThrow(new RuntimeException("store unavailable"));

    // Act & Assert — the fourth bucket is never attempted
    assertThatThrownBy(
            () ->
                store.findByStatusOlderThan(SagaStatus.COMPLETED, Instant.now(), 100, "owner-1", 0))
        .isInstanceOf(RuntimeException.class);
    verify(tx, times(3)).scan(any(Scan.class));
  }

  @Test
  void findByStatusOlderThan_poisonBucketsBelowBreakerThreshold_sweepStillCompletes()
      throws Exception {
    // Arrange — two consecutive poison buckets, a healthy one (resetting the failure counter),
    // then another poison bucket: no three failures are consecutive, so the sweep completes and
    // keeps what it could read
    Instant threshold = Instant.parse("2026-01-08T00:00:00Z");
    Result r = mockStateResult("saga-1", SagaStatus.COMPLETED);
    when(tx.scan(any(Scan.class)))
        .thenThrow(new RuntimeException("poison bucket"))
        .thenThrow(new RuntimeException("poison bucket"))
        .thenReturn(List.of(r))
        .thenThrow(new RuntimeException("poison bucket"));

    // Act
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(SagaStatus.COMPLETED, threshold, 100, "owner-1", 0);

    // Assert — all four buckets were attempted
    assertThat(result).hasSize(1);
    verify(tx, times(4)).scan(any(Scan.class));
  }

  @Test
  void findByStatusOlderThan_maxResultsTruncates_scansOwnersFirstPermutationBucket()
      throws Exception {
    // Arrange — the first scanned bucket yields a row, filling maxResults = 1 immediately
    Instant threshold = Instant.parse("2026-01-08T00:00:00Z");
    Result r = mockStateResult("saga-1", SagaStatus.COMPLETED);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r));

    // Act
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(SagaStatus.COMPLETED, threshold, 1, "owner-1", 0);

    // Assert — exactly one bucket scanned, and it is the owner permutation's first bucket, so
    // concurrently purging replicas surface different sagas when the result is truncated
    assertThat(result).hasSize(1);
    ArgumentCaptor<Scan> scans = ArgumentCaptor.forClass(Scan.class);
    verify(tx).scan(scans.capture());
    int firstBucket = scans.getValue().getPartitionKey().getIntValue(0);
    assertThat(firstBucket).isEqualTo(SweepScatter.permutation(SweepScatter.seed("owner-1"), 4)[0]);
  }

  @Test
  void findByStatusOlderThan_rotationGiven_startsThatManyPositionsIntoTheSweepOrder()
      throws Exception {
    // Arrange — the first scanned bucket fills maxResults = 1 immediately
    Instant threshold = Instant.parse("2026-01-08T00:00:00Z");
    Result r = mockStateResult("saga-1", SagaStatus.COMPLETED);
    when(tx.scan(any(Scan.class))).thenReturn(List.of(r));

    // Act — rotation 1: the sweep starts one position into the owner's permutation
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(SagaStatus.COMPLETED, threshold, 1, "owner-1", 1);

    // Assert
    assertThat(result).hasSize(1);
    ArgumentCaptor<Scan> scans = ArgumentCaptor.forClass(Scan.class);
    verify(tx).scan(scans.capture());
    assertThat(scans.getValue().getPartitionKey().getIntValue(0))
        .isEqualTo(SweepScatter.permutation(SweepScatter.seed("owner-1"), 4)[1]);
  }

  @Test
  void findByStatusOlderThan_oneBucketScanFails_otherBucketsStillScanned() throws Exception {
    // Arrange — the first bucket's scan fails (e.g., a row that cannot be deserialized); the
    // remaining buckets must still be swept instead of being abandoned
    Instant threshold = Instant.parse("2026-01-08T00:00:00Z");
    Result r = mockStateResult("saga-1", SagaStatus.COMPLETED);
    when(tx.scan(any(Scan.class)))
        .thenThrow(new RuntimeException("row cannot be deserialized"))
        .thenReturn(List.of(r))
        .thenReturn(List.of())
        .thenReturn(List.of());

    // Act
    List<SagaStateSnapshot> result =
        store.findByStatusOlderThan(SagaStatus.COMPLETED, threshold, 100, "owner-1", 0);

    // Assert — the poison bucket was skipped, all four buckets were attempted, results kept
    assertThat(result).hasSize(1);
    verify(tx, times(4)).scan(any(Scan.class));
  }

  // ---------------------------------------------------------------------------
  // initialSweepCursor — scattered bucket order
  // ---------------------------------------------------------------------------

  @Test
  void findRecoverable_initialSweepCursorGiven_visitsBucketsInOwnersScatteredOrder()
      throws Exception {
    // Arrange
    when(tx.getScanner(any(Scan.class))).thenAnswer(invocation -> scannerOver(List.of()));

    // Act — drive one full sweep from the owner's initial cursor
    SagaStore.ScanCursor cursor = store.initialSweepCursor("owner-1");
    int pages = 0;
    while (cursor != null) {
      Recoverables page = store.findRecoverable(Instant.now(), cursor);
      cursor = page.nextCursor();
      pages++;
    }

    // Assert — one page per bucket, visited in the owner's permutation order
    assertThat(pages).isEqualTo(4);
    ArgumentCaptor<Scan> scans = ArgumentCaptor.forClass(Scan.class);
    verify(tx, times(8)).getScanner(scans.capture()); // 4 buckets x 2 recoverable statuses
    int[] expected = SweepScatter.permutation(SweepScatter.seed("owner-1"), 4);
    assertThat(distinctConsecutiveBuckets(scans.getAllValues()))
        .containsExactly(expected[0], expected[1], expected[2], expected[3]);
  }

  @Test
  void findOverdueParkedSagas_initialSweepCursorGiven_visitsBucketsInOwnersScatteredOrder()
      throws Exception {
    // Arrange
    when(tx.scan(any(Scan.class))).thenReturn(List.of());

    // Act
    SagaStore.ScanCursor cursor = store.initialSweepCursor("owner-1");
    int pages = 0;
    while (cursor != null) {
      OverdueParked page = store.findOverdueParkedSagas(Instant.now(), cursor);
      cursor = page.nextCursor();
      pages++;
    }

    // Assert — the parked sweep follows the same owner permutation, one scan per bucket
    assertThat(pages).isEqualTo(4);
    ArgumentCaptor<Scan> scans = ArgumentCaptor.forClass(Scan.class);
    verify(tx, times(4)).scan(scans.capture());
    int[] expected = SweepScatter.permutation(SweepScatter.seed("owner-1"), 4);
    assertThat(distinctConsecutiveBuckets(scans.getAllValues()))
        .containsExactly(expected[0], expected[1], expected[2], expected[3]);
  }

  @Test
  void findRecoverable_unknownCursorTypeGiven_throwsIllegalArgumentException() {
    // Arrange — a cursor this store did not create (e.g., from another store implementation).
    // Falling back silently would restart an un-scattered ascending sweep, so it must fail fast.
    SagaStore.ScanCursor foreign = new SagaStore.ScanCursor() {};

    // Act & Assert
    assertThatThrownBy(() -> store.findRecoverable(Instant.now(), foreign))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> store.advanceSweepCursor(foreign))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void advanceSweepCursor_fullSweepByAdvancing_endsAfterOneRevolution() {
    // Act — skip every page without scanning
    SagaStore.ScanCursor cursor = store.initialSweepCursor("owner-1");
    int advances = 0;
    while (cursor != null) {
      cursor = store.advanceSweepCursor(cursor);
      advances++;
    }

    // Assert — one advance per bucket, then the sweep is exhausted
    assertThat(advances).isEqualTo(4);
  }

  @Test
  void advanceSweepCursor_pageSkipped_scanContinuesAtNextPermutationBucket() throws Exception {
    // Arrange
    when(tx.getScanner(any(Scan.class))).thenAnswer(invocation -> scannerOver(List.of()));

    // Act — skip the first page (as the manager does for a failing page), then scan
    SagaStore.ScanCursor afterSkip = store.advanceSweepCursor(store.initialSweepCursor("owner-1"));
    store.findRecoverable(Instant.now(), afterSkip);

    // Assert — the scan hits the owner permutation's second bucket
    ArgumentCaptor<Scan> scans = ArgumentCaptor.forClass(Scan.class);
    verify(tx, times(2)).getScanner(scans.capture()); // 2 recoverable statuses in one bucket
    int[] expected = SweepScatter.permutation(SweepScatter.seed("owner-1"), 4);
    assertThat(scans.getAllValues().get(0).getPartitionKey().getIntValue(0)).isEqualTo(expected[1]);
  }

  /** Bucket partition keys of the given scans, with consecutive duplicates collapsed. */
  private static List<Integer> distinctConsecutiveBuckets(List<Scan> scans) {
    List<Integer> buckets = new ArrayList<>();
    for (Scan scan : scans) {
      int bucket = scan.getPartitionKey().getIntValue(0);
      if (buckets.isEmpty() || buckets.get(buckets.size() - 1) != bucket) {
        buckets.add(bucket);
      }
    }
    return buckets;
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
    // Arrange — commit throws UTSE, but the verifier confirms the saga was created. It takes two
    // further transactions: tx2 reads the seq-0 event to check the append_id is ours, then tx3
    // reads the saga's state to return.
    doThrow(mock(UnknownTransactionStatusException.class)).when(tx).commit();
    DistributedTransaction tx2 = mock(DistributedTransaction.class);
    DistributedTransaction tx3 = mock(DistributedTransaction.class);
    when(txManager.begin()).thenReturn(tx).thenReturn(tx2).thenReturn(tx3);
    Result ownEvent = mock(Result.class);
    when(ownEvent.getText("append_id")).thenReturn(OWN_APPEND_ID);
    when(tx2.get(any(Get.class))).thenReturn(Optional.of(ownEvent));
    TransactionCrudOperable.Scanner state =
        scannerOver(
            List.of(
                stateEventRow(
                    0,
                    EventType.SAGA_STARTED,
                    SagaStatus.RUNNING,
                    Instant.parse("2026-01-01T00:00:00Z"))));
    when(tx3.getScanner(any(Scan.class))).thenReturn(state);

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
    // Verifier (tx2): no event at sequence 0 → our insert did not land, so retry
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

    // Act & Assert — the exhaustion path throws a retryable (store-unavailable) exception with
    // the code's fixed message; the per-attempt cause chain carries the underlying UTSE and any
    // suppressed verifier failures for debugging.
    assertThatThrownBy(
            () -> retryStore.createSaga("saga-1", "order-saga", "engine-1", Map.of(), "v1"))
        .isInstanceOf(SagaPersistenceException.class)
        .extracting(e -> ((SagaPersistenceException) e).isRetryable())
        .isEqualTo(true);
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
        SagaPersistenceException.deserializationFailed(new RuntimeException("parse"));
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

  // Changing this default moves the documented budget floor (scanLimit x recoverable statuses),
  // which is stated as a concrete number in RecoveryConfig, SagaServerConfig and server.properties.
  @Test
  void build_withDefaults_hasDefaultRecoveryScanLimit() {
    // Act
    ScalarDbSagaStoreConfig config = ScalarDbSagaStoreConfig.builder().build();

    // Assert
    assertThat(config.getRecoveryScanLimit()).isEqualTo(100);
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
  void listStateSnapshots_updatedAfterAboveKeyMaxGiven_throwsException() {
    // Arrange
    SagaQuery query =
        SagaQuery.newBuilder()
            .updatedAfter(ScalarDbSagaStore.MAX_KEY_INSTANT.plusMillis(1))
            .build();

    // Act & Assert
    assertThatThrownBy(() -> store.listStateSnapshots(query))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_updatedBeforeBelowKeyMinGiven_throwsException() {
    // Arrange
    SagaQuery query =
        SagaQuery.newBuilder()
            .updatedBefore(ScalarDbSagaStore.MIN_KEY_INSTANT.minusMillis(1))
            .build();

    // Act & Assert
    assertThatThrownBy(() -> store.listStateSnapshots(query))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_updatedBoundsAtKeyExtremesGiven_accepted() throws Exception {
    // Arrange — the inclusive endpoints of the key domain are valid and start a real scan
    stubScanner();

    // Act
    SagaPage<SagaStateSnapshot> page =
        store.listStateSnapshots(
            SagaQuery.newBuilder()
                .updatedAfter(ScalarDbSagaStore.MIN_KEY_INSTANT)
                .updatedBefore(ScalarDbSagaStore.MAX_KEY_INSTANT)
                .build());

    // Assert
    assertThat(page.getItems()).isEmpty();
  }

  @Test
  void listStateSnapshots_withoutUpdatedBefore_endsEveryScanWithinPortableBigIntRange()
      throws Exception {
    // Arrange — Cosmos DB rejects a BIGINT key beyond +/-2^53, a scan's end key included
    stubScanner();

    // Act
    store.listStateSnapshots(SagaQuery.newBuilder().build());

    // Assert
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(tx, atLeastOnce()).getScanner(captor.capture());
    assertThat(captor.getAllValues())
        .allSatisfy(
            scan ->
                assertThat(scan.getEndClusteringKey().orElseThrow().getBigIntValue(1))
                    .isBetween(-(1L << 53), 1L << 53));
  }

  @Test
  void listStateSnapshots_tokenUpdatedAtBeyondKeyRangeGiven_throwsSagaIllegalArgumentException() {
    // Arrange — a tampered cursor timestamp would become the next scan's start key
    String token =
        encodePageToken(
            "1", "*|-|-", 0, 0, ScalarDbSagaStore.MAX_KEY_INSTANT.plusMillis(1).toString());

    // Act & Assert
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(SagaIllegalArgumentException.class);
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
    // Arrange — 4 buckets x (6 status codes + the timeout slice) => 28 scanners when nothing
    // matches
    stubScanner();

    // Act
    store.listStateSnapshots(SagaQuery.newBuilder().build());

    // Assert
    verify(tx, times(4 * (SagaStatus.values().length + 1))).getScanner(any(Scan.class));
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
  void listStateSnapshots_malformedTokenGiven_throwsSagaIllegalArgumentException() {
    // Act & Assert — not valid Base64URL; rejected before any scan
    assertThatThrownBy(
            () ->
                store.listStateSnapshots(
                    SagaQuery.newBuilder().pageToken("!!!not-base64!!!").build()))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenTooLongGiven_throwsSagaIllegalArgumentException() {
    // Arrange — far longer than any valid cursor; rejected before Base64 decoding
    String token = "A".repeat(1000);

    // Act & Assert
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenBucketOutOfRangeGiven_throwsSagaIllegalArgumentException() {
    // Arrange — bucket 999 with a 4-bucket schema; filter key matches the unfiltered query
    String token = encodePageToken("1", "*|-|-", 999, 0, "2026-01-01T00:00:00Z");

    // Act & Assert
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenUnknownVersionGiven_throwsSagaIllegalArgumentException() {
    // Arrange — version "0" is not one this store writes
    String token = encodePageToken("0", "*|-|-", 0, 0, "2026-01-01T00:00:00Z");

    // Act & Assert
    assertThatThrownBy(
            () -> store.listStateSnapshots(SagaQuery.newBuilder().pageToken(token).build()))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void listStateSnapshots_tokenStatusNotInFilterGiven_throwsSagaIllegalArgumentException() {
    // Arrange — query filters RUNNING(0) and the filter key matches, but the cursor status is
    // COMPLETED(1), outside the swept set: the defense-in-depth membership check rejects it.
    String token = encodePageToken("1", "0|-|-", 0, 1, "2026-01-01T00:00:00Z");

    // Act & Assert
    assertThatThrownBy(
            () ->
                store.listStateSnapshots(
                    SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageToken(token).build()))
        .isInstanceOf(SagaIllegalArgumentException.class);
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
        .isInstanceOf(SagaIllegalArgumentException.class);
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
        .isInstanceOf(SagaIllegalArgumentException.class);
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
   * exactly the {@code master} rows in the requested {@code (bucket, slice)} whose {@code sweep_at}
   * falls within the scan's bounds, respecting {@code startInclusive}. Unlike {@link #stubScanner},
   * this lets one dataset drive real multi-page pagination, so the resume cursor genuinely excludes
   * already-returned cohorts rather than the test hand-feeding each page.
   */
  private void stubScannerHonoringRange(Result... master) throws Exception {
    when(tx.getScanner(any(Scan.class)))
        .thenAnswer(
            invocation -> {
              Scan scan = invocation.getArgument(0);
              int bucket = scan.getPartitionKey().getIntValue(0);
              Key start = scan.getStartClusteringKey().orElseThrow();
              int slice = start.getIntValue(0);
              Instant startTs = Instant.ofEpochMilli(start.getBigIntValue(1));
              boolean startInclusive = scan.getStartInclusive();
              Instant endTs =
                  Instant.ofEpochMilli(scan.getEndClusteringKey().orElseThrow().getBigIntValue(1));

              List<Result> rows = new ArrayList<>();
              for (Result r : master) {
                if (r.getInt("bucket") != bucket || r.getInt("slice") != slice) {
                  continue;
                }
                Instant ts = Instant.ofEpochMilli(r.getBigInt("sweep_at"));
                boolean afterStart = startInclusive ? !ts.isBefore(startTs) : ts.isAfter(startTs);
                if (afterStart && !ts.isAfter(endTs)) {
                  rows.add(r);
                }
              }
              rows.sort(Comparator.comparingLong(r -> r.getBigInt("sweep_at")));
              return scannerOver(rows);
            });
  }

  /** Sweep rows in one slice, all at one stale timestamp, in the given order. */
  private List<Result> staleRows(String... sagaIds) {
    return rowsAt(1_000L, sagaIds);
  }

  /** RUNNING sweep rows in one slice, all at {@code sweepAt}, in the given order. */
  private List<Result> rowsAt(long sweepAt, String... sagaIds) {
    List<Result> rows = new ArrayList<>();
    for (String id : sagaIds) {
      Result r = mockStateResult(id, SagaStatus.RUNNING);
      lenient().when(r.getBigInt("sweep_at")).thenReturn(sweepAt);
      rows.add(r);
    }
    return rows;
  }

  private static List<String> ids(Recoverables page) {
    return page.sagas().stream().map(SagaStateSnapshot::getSagaId).toList();
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

  /**
   * A store that makes exactly one attempt before exhausting its retries, so a test can stub the
   * action's transaction and the verifier's as two consecutive {@code begin()} calls. With the
   * default retry count the action would consume several before the verifier ran.
   */
  private ScalarDbSagaStore singleAttemptStore() {
    return new ScalarDbSagaStore(
        txManager,
        objectMapper,
        schema,
        ScalarDbSagaStoreConfig.builder().transactionRetryCount(1).build(),
        () -> OWN_APPEND_ID);
  }

  /**
   * The value of {@code column} on the first captured insert that carries it. The sweep and event
   * inserts write disjoint columns, so naming the column selects the insert.
   */
  private static String capturedTextColumn(DistributedTransaction tx, String column)
      throws Exception {
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx, atLeastOnce()).insert(captor.capture());
    return captor.getAllValues().stream()
        .map(insert -> insert.getColumns().get(column))
        .filter(Objects::nonNull)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no insert carried a " + column + " column"))
        .getTextValue();
  }

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
   * Captures every {@code tx.insert(...)} on the given transaction, finds the one that carries an
   * {@code append_id} column (the event insert; only event rows have it, so this reliably picks the
   * event insert out of methods that also write sweep rows), and asserts its value.
   */
  private static void assertEventInsertCarriesAppendId(
      DistributedTransaction tx, String expectedAppendId) throws Exception {
    assertThat(capturedTextColumn(tx, "append_id")).isEqualTo(expectedAppendId);
  }

  /** Reads {@code append_id} from an {@link Insert}, failing loudly if the column is absent. */
  private static String requireAppendId(Insert insert) {
    return Objects.requireNonNull(insert.getColumns().get("append_id"), "append_id column missing")
        .getTextValue();
  }

  /**
   * A row carrying a saga's state, readable both as the newest state event of its log and as its
   * sweep row. The stubs are lenient because each caller reads only one of the two shapes.
   */
  private Result mockStateResult(String sagaId, SagaStatus status, Instant updatedAt) {
    Result r = stateEventRow(0, EventType.SAGA_STARTED, status, updatedAt);
    lenient().when(r.getText("saga_id")).thenReturn(sagaId);
    lenient().when(r.getInt("bucket")).thenReturn(0);
    lenient().when(r.getInt("slice")).thenReturn(SagaSchema.sliceOf(status, false));
    lenient().when(r.getTimestampTZ("updated_at")).thenReturn(updatedAt);
    lenient()
        .when(r.getTimestampTZ("created_at"))
        .thenReturn(Instant.parse("2026-01-01T00:00:00Z"));
    return r;
  }

  /** Like {@link #mockStateResult(String, SagaStatus, Instant)}, last changed at creation. */
  private Result mockStateResult(String sagaId, SagaStatus status) {
    return mockStateResult(sagaId, status, Instant.parse("2026-01-01T00:00:00Z"));
  }

  /** Stubs {@code t}'s newest-first read of a saga's events to yield {@code newestFirst}. */
  private static void stubStateRead(DistributedTransaction t, Result... newestFirst)
      throws Exception {
    TransactionCrudOperable.Scanner scanner = scannerOver(List.of(newestFirst));
    when(t.getScanner(any(Scan.class))).thenReturn(scanner);
  }

  /**
   * The {@code STEP_PENDING} event an unbounded park wrote, which a transition out of {@code
   * WAITING} reads back to find the saga's sweep row. Its stubs also answer the sweep-row read.
   */
  private static Result parkEventRow(Instant parkedAt) {
    return stateEventRow(3, EventType.STEP_PENDING, SagaStatus.WAITING, parkedAt);
  }

  /** Like {@link #parkEventRow}, for a park with a deadline: its sweep row sits at the deadline. */
  private static Result boundedParkEventRow(Instant parkedAt, Instant deadline) {
    Result r = parkEventRow(parkedAt);
    lenient().when(r.getInt("state_slice")).thenReturn(SagaSchema.WAITING_WITH_TIMEOUT_SLICE);
    lenient().when(r.getBigInt("sweep_at")).thenReturn(deadline.toEpochMilli());
    return r;
  }

  /** Matches a {@link Get} on {@code table}. */
  private static Get getOn(String table) {
    return argThat(get -> get != null && table.equals(get.forTable().orElse(null)));
  }

  /** The slice of the sweep row the transaction deleted, which is the slice the saga left. */
  private static int deletedSweepSlice(DistributedTransaction tx) throws Exception {
    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(tx).delete(captor.capture());
    return captor.getValue().getClusteringKey().orElseThrow().getIntValue(0);
  }

  /** Moves a mocked row into {@code bucket}, so one dataset can span several partitions. */
  private static Result inBucket(Result r, int bucket) {
    lenient().when(r.getInt("bucket")).thenReturn(bucket);
    return r;
  }

  /**
   * An event row that carries the saga's state, as every status-changing event does. The stubs are
   * lenient because a read stops at the newest such row, so no caller touches every column.
   */
  private static Result stateEventRow(
      int sequence, EventType type, SagaStatus status, Instant updatedAt) {
    Result r = mock(Result.class);
    lenient().when(r.getInt("sequence")).thenReturn(sequence);
    lenient().when(r.getText("event_type")).thenReturn(type.name());
    lenient().when(r.getTimestampTZ("created_at")).thenReturn(updatedAt);
    lenient().when(r.isNull("state_slice")).thenReturn(false);
    lenient().when(r.getInt("state_slice")).thenReturn(SagaSchema.sliceOf(status, false));
    lenient().when(r.getBigInt("sweep_at")).thenReturn(updatedAt.toEpochMilli());
    lenient().when(r.getText("saga_name")).thenReturn("order-saga");
    lenient().when(r.getText("definition_version")).thenReturn("v1");
    lenient()
        .when(r.getTimestampTZ("saga_created_at"))
        .thenReturn(Instant.parse("2026-01-01T00:00:00Z"));
    return r;
  }

  /** A step event row, which carries none of the saga's state. */
  private static Result stepEventRow(int sequence, EventType type, Instant createdAt) {
    Result r = mock(Result.class);
    lenient().when(r.getInt("sequence")).thenReturn(sequence);
    lenient().when(r.getText("event_type")).thenReturn(type.name());
    lenient().when(r.getTimestampTZ("created_at")).thenReturn(createdAt);
    lenient().when(r.isNull("state_slice")).thenReturn(true);
    return r;
  }

  /** The inserts {@code tx} received, keyed by the table each targets. */
  private static Map<String, Insert> insertsByTable(DistributedTransaction tx) throws Exception {
    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(tx, atLeastOnce()).insert(captor.capture());
    return captor.getAllValues().stream()
        .collect(Collectors.toMap(insert -> insert.forTable().orElseThrow(), insert -> insert));
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
