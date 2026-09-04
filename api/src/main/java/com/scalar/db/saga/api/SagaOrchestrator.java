package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaDefinitionNotServedException;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaOverloadedException;
import java.util.Map;

/**
 * The application-facing surface for orchestrating sagas: starting instances (synchronous or
 * asynchronous, by name or by versioned id) and querying their state. Shared by the in-process
 * {@code DefaultSagaOrchestrator} and by {@code GrpcSagaOrchestratorClient} (the Java 8 client
 * SDK), so the same application code runs embedded or against a saga server.
 *
 * <p>Implementations must be thread-safe — multiple sagas execute concurrently.
 *
 * <p>A "generated" saga ID is an opaque, unique id minted by the implementation; where it is minted
 * is an implementation detail (the embedded orchestrator mints it server-side, a remote client may
 * mint it client-side as an idempotency key). Callers depend only on receiving a unique id back.
 *
 * <p><b>Deciding what to retry.</b> Key on {@link
 * com.scalar.db.saga.exception.SagaErrorCode.Category#RETRYABLE_SERVER_ERROR} via {@code
 * e.getErrorCode().category()} rather than on a single exception type. A transient store failure
 * reaches a remote caller as {@link com.scalar.db.saga.exception.SagaPersistenceException} (the
 * type the engine threw, reconstructed from the wire) while a connectivity failure that never
 * reached the server arrives as {@link com.scalar.db.saga.exception.SagaUnavailableException}. Both
 * are retryable, and only the category covers both.
 *
 * <p><b>Why {@code startAsync} declares fewer failures.</b> A {@code startAsync} overload validates
 * the definition name and the caller-supplied id, then persists the saga and hands execution to a
 * background thread. Anything that goes wrong after that point — step resolution, a failing step,
 * compensation — cannot reach the caller, so those overloads do not declare {@link
 * SagaDefinitionException}. Such a saga is recoverable from its persisted state and its outcome is
 * observed through {@link #getStateSnapshot} or {@link #getSagaDetail}, typically as {@code
 * ESCALATED}. The synchronous {@code start} overloads run execution on the calling thread and do
 * surface it.
 *
 * <p><b>A remote implementation throws more, never less.</b> Every failure declared here arrives
 * from either implementation as the same type and {@link
 * com.scalar.db.saga.exception.SagaErrorCode}. A remote client adds the failures that only exist
 * over a network: {@link com.scalar.db.saga.exception.SagaUnavailableException}, {@link
 * com.scalar.db.saga.exception.SagaTimeoutException}, {@link
 * com.scalar.db.saga.exception.SagaErrorCode#REQUEST_ABORTED} for a caller-cancelled call — and,
 * against a secured server, {@link com.scalar.db.saga.exception.SagaUnauthenticatedException} and
 * {@link com.scalar.db.saga.exception.SagaPermissionDeniedException}, plus the client SDK's
 * fallback codes ({@code UNRECOGNIZED_SERVER_ERROR} and friends) under version skew or an
 * intermediary failure. The embedded implementation never throws any of those. {@link
 * com.scalar.db.saga.exception.SagaDefinitionNotServedException} is not among them: it is declared
 * on every start overload and both implementations throw it — the embedded one once the application
 * has published a served set, a saga server whenever its definition files no longer describe the
 * saga.
 *
 * <p>Construct the embedded implementation via its builder:
 *
 * <pre>{@code
 * DefaultSagaOrchestrator orchestrator = DefaultSagaOrchestrator.newBuilder()
 *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
 *     .build();
 * }</pre>
 */
public interface SagaOrchestrator extends AutoCloseable {

  /**
   * Starts a new saga instance with a generated ID (synchronous — blocks until the saga completes
   * or fails).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #start(SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaDefinitionException if step resolution fails
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  String start(String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID (synchronous). Enables idempotent retries:
   * if the caller crashes after the saga is persisted, it can retry with the same ID.
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #start(String, SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaDefinitionException if step resolution fails
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   * @throws SagaAlreadyExistsException if a saga already exists with {@code sagaId}
   * @throws SagaIllegalArgumentException if {@code sagaId} is not of the form {@code
   *     [a-zA-Z0-9._-]{1,128}}
   */
  void start(String sagaId, String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a generated ID, using a specific definition version
   * (synchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaDefinitionException if step resolution fails
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  String start(SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID, using a specific definition version
   * (synchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param sagaId the client-supplied saga ID
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaDefinitionException if step resolution fails
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   * @throws SagaAlreadyExistsException if a saga already exists with {@code sagaId}
   * @throws SagaIllegalArgumentException if {@code sagaId} is not of the form {@code
   *     [a-zA-Z0-9._-]{1,128}}
   */
  void start(String sagaId, SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a generated ID (asynchronous — returns immediately).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  String startAsync(String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a generated ID (asynchronous with completion callback).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(SagaDefinitionId, Map, SagaCallback)} to
   * avoid the store round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws UnsupportedOperationException if the implementation cannot deliver a local completion
   *     callback (e.g. a remote client with no server-streaming callback channel)
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback);

  /**
   * Starts a new saga instance with a client-supplied ID (asynchronous).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(String, SagaDefinitionId, Map)} to avoid the
   * store round-trip.
   *
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaAlreadyExistsException if a saga already exists with {@code sagaId}
   * @throws SagaIllegalArgumentException if {@code sagaId} is not of the form {@code
   *     [a-zA-Z0-9._-]{1,128}}
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  void startAsync(String sagaId, String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID (asynchronous with completion callback).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(String, SagaDefinitionId, Map, SagaCallback)}
   * to avoid the store round-trip.
   *
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws UnsupportedOperationException if the implementation cannot deliver a local completion
   *     callback (e.g. a remote client with no server-streaming callback channel)
   * @throws SagaAlreadyExistsException if a saga already exists with {@code sagaId}
   * @throws SagaIllegalArgumentException if {@code sagaId} is not of the form {@code
   *     [a-zA-Z0-9._-]{1,128}}
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  void startAsync(String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback);

  /**
   * Starts a new saga instance with a generated ID, using a specific definition version
   * (asynchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  String startAsync(SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a generated ID, using a specific definition version
   * (asynchronous with completion callback). The definition is resolved from the in-memory cache
   * first, falling back to the store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws UnsupportedOperationException if the implementation cannot deliver a local completion
   *     callback (e.g. a remote client with no server-streaming callback channel)
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  String startAsync(SagaDefinitionId id, Map<String, Object> input, SagaCallback callback);

  /**
   * Starts a new saga instance with a client-supplied ID, using a specific definition version
   * (asynchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param sagaId the client-supplied saga ID
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws SagaAlreadyExistsException if a saga already exists with {@code sagaId}
   * @throws SagaIllegalArgumentException if {@code sagaId} is not of the form {@code
   *     [a-zA-Z0-9._-]{1,128}}
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  void startAsync(String sagaId, SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID, using a specific definition version
   * (asynchronous with completion callback). The definition is resolved from the in-memory cache
   * first, falling back to the store only on a cache miss.
   *
   * @param sagaId the client-supplied saga ID
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionNotServedException if the definition is registered but this deployment
   *     does not serve it
   * @throws UnsupportedOperationException if the implementation cannot deliver a local completion
   *     callback (e.g. a remote client with no server-streaming callback channel)
   * @throws SagaAlreadyExistsException if a saga already exists with {@code sagaId}
   * @throws SagaIllegalArgumentException if {@code sagaId} is not of the form {@code
   *     [a-zA-Z0-9._-]{1,128}}
   * @throws SagaOverloadedException if the engine is at its concurrent saga limit; nothing was
   *     persisted, so the request can be retried unchanged
   */
  void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback);

  /**
   * Queries the current state of a saga instance.
   *
   * @param sagaId the saga instance ID
   * @return the current saga state snapshot
   * @throws SagaNotFoundException if no saga has that id
   */
  SagaStateSnapshot getStateSnapshot(String sagaId);

  /**
   * Returns a saga's current state together with its timeline — the metadata and failure error or
   * intervention reason of each recorded event, never a raw step input/output payload. The
   * application that ran the saga uses this to diagnose why it failed, without an operator's
   * involvement; it is a read, so it needs no elevated privilege.
   *
   * <p>An orchestrator configured with a timeline bound cuts a longer history to the newest events
   * and reports the cut via {@link SagaDetail#isTruncated()}; the full history remains in the
   * store. Independently of that bound, each entry's detail text is capped (see {@link
   * TimelineEvent#getDetail()}), so the response's byte size is bounded by the entry count even
   * when a step failure message embeds a large downstream response.
   *
   * @param sagaId the saga instance ID
   * @return the saga's state and timeline
   * @throws SagaNotFoundException if no saga has that id
   */
  SagaDetail getSagaDetail(String sagaId);

  /**
   * Shuts down the orchestrator, stopping background tasks and waiting for in-flight sagas to
   * complete.
   */
  @Override
  void close();
}
