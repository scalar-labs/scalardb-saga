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
public class SagaDefinitionRegistry {

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
    definitions.put(definition.getName(), definition);
    definitions.put(definition.getName() + ":" + definition.getVersion(), definition);
  }

  /**
   * Looks up a definition by name only (latest registered version). Falls back to the store on
   * cache miss (e.g., after a restart when definitions were registered in a previous run).
   */
  @Nullable SagaDefinition get(String sagaName) {
    SagaDefinition def = definitions.get(sagaName);
    if (def != null) {
      return def;
    }
    def = store.getDefinition(sagaName).orElse(null);
    if (def != null) {
      definitions.put(sagaName, def);
      definitions.put(sagaName + ":" + def.getVersion(), def);
    }
    return def;
  }

  /**
   * Versioned lookup with store fallback. Used during recovery and async step completion, where the
   * definition version must match the one used when the saga was created (even after a redeploy).
   */
  public @Nullable SagaDefinition resolve(String sagaName, String version) {
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
