package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Top-level API for saga orchestration. Manages saga definitions, execution, and recovery.
 *
 * <p>Implementations must be thread-safe — multiple sagas execute concurrently.
 */
public interface SagaManager extends AutoCloseable {

  /**
   * Registers a saga definition. The definition is validated and persisted.
   *
   * @param definition the saga definition
   * @throws SagaDefinitionException if the definition fails validation
   */
  void register(SagaDefinition definition);

  /**
   * Parses a saga definition from a file and registers it. Detects JSON or YAML by file extension
   * ({@code .json}, {@code .yaml}, {@code .yml}).
   *
   * @param definitionFile the path to the definition file
   * @throws SagaDefinitionException if the file cannot be parsed or fails validation
   */
  void register(Path definitionFile);

  /**
   * Starts a new saga instance with a server-generated ID (synchronous — blocks until the saga
   * completes or fails).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #start(SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @return the generated saga ID
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
   */
  void start(String sagaId, String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID, using a specific definition version
   * (synchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
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
   */
  void start(String sagaId, SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID (asynchronous — returns immediately).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @return the generated saga ID
   */
  String startAsync(String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID (asynchronous with completion callback).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(SagaDefinitionId, Map, SagaCallback)} to
   * avoid the store round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @return the generated saga ID
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
   */
  void startAsync(String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback);

  /**
   * Starts a new saga instance with a server-generated ID, using a specific definition version
   * (asynchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   */
  String startAsync(SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID, using a specific definition version
   * (asynchronous with completion callback). The definition is resolved from the in-memory cache
   * first, falling back to the store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
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
   */
  void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback);

  /**
   * Resumes a failed or crashed saga (crash recovery).
   *
   * @param sagaId the saga instance ID
   * @return the saga state after resumption
   */
  SagaStateSnapshot resume(String sagaId);

  /**
   * Manually triggers compensation for a saga.
   *
   * @param sagaId the saga instance ID
   * @return the saga state after compensation
   */
  SagaStateSnapshot compensate(String sagaId);

  /**
   * Queries the current state of a saga instance.
   *
   * @param sagaId the saga instance ID
   * @return the current saga state snapshot
   */
  SagaStateSnapshot getStateSnapshot(String sagaId);

  /**
   * Completes an asynchronous step via external callback (daemon mode only). Resumes a parked saga
   * with the step's output.
   *
   * @param sagaId the saga instance ID
   * @param stepName the name of the step to complete
   * @param output the step's output data
   * @return the saga state after step completion
   */
  SagaStateSnapshot completeStep(String sagaId, String stepName, Map<String, Object> output);

  /**
   * Runs a single recovery pass: scans for stale sagas, claims them, and resumes or compensates as
   * appropriate. This is the same logic that runs periodically when {@link #startBackgroundTasks()}
   * is called.
   *
   * <p>Useful for on-demand recovery (e.g., admin tooling) or testing crash recovery without
   * relying on the periodic background scanner.
   */
  void recover();

  /**
   * Starts periodic background tasks: crash recovery scanning and retention cleanup of terminal
   * sagas. Call after registering all saga definitions.
   */
  void startBackgroundTasks();

  /**
   * Shuts down the saga manager, stopping background tasks and waiting for in-flight sagas to
   * complete.
   */
  @Override
  void close();
}
