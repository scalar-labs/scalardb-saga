package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaContext;
import java.util.Map;
import java.util.Optional;

/** Minimal map-backed {@link SagaContext} for transport tests. */
final class FakeSagaContext implements SagaContext {

  private final String sagaId;
  private final Map<String, Object> data;

  FakeSagaContext(String sagaId, Map<String, Object> data) {
    this.sagaId = sagaId;
    this.data = data;
  }

  @Override
  public String getSagaId() {
    return sagaId;
  }

  @Override
  public <T> Optional<T> get(String key, Class<T> type) {
    return Optional.ofNullable(data.get(key)).map(type::cast);
  }
}
