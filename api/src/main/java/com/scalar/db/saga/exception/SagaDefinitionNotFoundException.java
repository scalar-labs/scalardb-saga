package com.scalar.db.saga.exception;

import com.scalar.db.saga.api.SagaDefinitionId;
import org.jspecify.annotations.Nullable;

/** Thrown when a saga definition cannot be found by name or by name and version. */
public class SagaDefinitionNotFoundException extends SagaRuntimeException {

  private final String sagaName;
  private final @Nullable String version;

  public SagaDefinitionNotFoundException(String sagaName) {
    super("No saga definition registered for: " + sagaName);
    this.sagaName = sagaName;
    this.version = null;
  }

  public SagaDefinitionNotFoundException(String sagaName, String version) {
    super("No saga definition registered for: " + sagaName + " (v" + version + ")");
    this.sagaName = sagaName;
    this.version = version;
  }

  public SagaDefinitionNotFoundException(SagaDefinitionId id) {
    this(id.name(), id.version());
  }

  public String getSagaName() {
    return sagaName;
  }

  public @Nullable String getVersion() {
    return version;
  }
}
