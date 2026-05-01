package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.SagaDefinitionException;
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
   * Loads and registers all saga definitions ({@code .json}, {@code .yaml}, {@code .yml}) from a
   * classpath path.
   *
   * @param resourcePath the classpath path to scan
   */
  void registerFromClasspath(String resourcePath);

  /**
   * Starts a new saga instance with a server-generated ID (synchronous — blocks until the saga
   * completes or fails).
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
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   */
  void start(String sagaId, String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID (asynchronous — returns immediately).
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @return the generated saga ID
   */
  String startAsync(String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID (asynchronous with completion callback).
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
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   */
  void startAsync(String sagaId, String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID (asynchronous with completion callback).
   *
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   */
  void startAsync(String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback);

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
   * Starts periodic crash recovery scanning. Call after registering all saga definitions. Delegates
   * to {@code SagaRecoveryManager.start()}.
   */
  void startRecovery();

  /**
   * Shuts down the saga manager, stopping recovery scanning and waiting for in-flight sagas to
   * complete.
   */
  @Override
  void close();
}
