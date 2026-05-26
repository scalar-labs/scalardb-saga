package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.store.SagaStore;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Centralized definition registry with two-tier lookup: in-memory first, then store fallback.
 *
 * <p>{@link EmbeddedSagaManager} calls {@link #register}, while recovery calls {@link #resolve}.
 * Definitions are persisted to the store <b>before</b> being put in memory to ensure recoverability
 * by other replicas.
 */
@ThreadSafe
class SagaDefinitionRegistry {

  private final ConcurrentHashMap<String, SagaDefinition> definitions = new ConcurrentHashMap<>();
  private final SagaStore store;

  SagaDefinitionRegistry(SagaStore store) {
    this.store = store;
  }

  /**
   * Registers a saga definition. Persists to the store first, then caches in memory. If the store
   * write fails, the definition is not available — fail-fast.
   */
  void register(SagaDefinition definition) {
    store.registerDefinition(definition);
    definitions.put(definition.getName() + ":" + definition.getVersion(), definition);
  }

  /**
   * Looks up a definition by name only (latest version). Always queries the store to avoid serving
   * stale versions from the in-memory cache (another instance may have registered a newer version).
   * The resolved definition is cached under its versioned key for subsequent {@link #resolve}
   * calls.
   */
  @Nullable SagaDefinition get(String sagaName) {
    SagaDefinition def = store.getDefinition(sagaName).orElse(null);
    if (def != null) {
      definitions.put(sagaName + ":" + def.getVersion(), def);
    }
    return def;
  }

  /**
   * Versioned lookup with store fallback. Used during recovery and async step completion, where the
   * definition version must match the one used when the saga was created (even after a redeploy).
   */
  @Nullable SagaDefinition resolve(String sagaName, String version) {
    String key = sagaName + ":" + version;
    SagaDefinition def = definitions.get(key);
    if (def != null) {
      return def;
    }
    def = store.getDefinition(sagaName, version).orElse(null);
    if (def != null) {
      definitions.put(key, def);
    }
    return def;
  }
}
