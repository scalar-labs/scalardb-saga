package com.scalar.db.saga.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaRuntimeException;
import org.junit.jupiter.api.Test;

class ClientProtoMappersTest {

  @Test
  void toProtoStatus_everyApiStatus_mapsByNameToANonUnspecifiedWireStatus() {
    // Every api status must have a wire counterpart named SAGA_STATUS_<name>; an unmapped one now
    // throws IllegalStateException, failing loudly rather than degrading to UNSPECIFIED.
    for (SagaStatus apiStatus : SagaStatus.values()) {
      com.scalar.db.saga.rpc.SagaStatus wire = ClientProtoMappers.toProtoStatus(apiStatus);
      assertThat(wire.name()).isEqualTo("SAGA_STATUS_" + apiStatus.name());
      assertThat(wire).isNotEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_UNSPECIFIED);
    }
  }

  @Test
  void toProtoStatus_everyWireStatus_hasAnApiCounterpart() {
    // Guards drift the other way: a wire status added without an api counterpart fails here.
    for (com.scalar.db.saga.rpc.SagaStatus wire : com.scalar.db.saga.rpc.SagaStatus.values()) {
      if (wire == com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_UNSPECIFIED
          || wire == com.scalar.db.saga.rpc.SagaStatus.UNRECOGNIZED) {
        continue;
      }
      String apiName = wire.name().substring("SAGA_STATUS_".length());
      assertThatCode(() -> SagaStatus.valueOf(apiName)).doesNotThrowAnyException();
    }
  }

  @Test
  void fromProtoStatus_inverseOfToProtoStatus_forEveryApiStatus() {
    // Composes both mappers so a one-sided regression (either direction) fails, rather than
    // rebuilding the wire enum inline and only exercising fromProtoStatus.
    for (SagaStatus apiStatus : SagaStatus.values()) {
      assertThat(ClientProtoMappers.fromProtoStatus(ClientProtoMappers.toProtoStatus(apiStatus)))
          .isEqualTo(apiStatus);
    }
  }

  @Test
  void fromProtoStatus_unspecified_throws() {
    assertThatThrownBy(
            () ->
                ClientProtoMappers.fromProtoStatus(
                    com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_UNSPECIFIED))
        .isInstanceOf(SagaRuntimeException.class);
  }

  @Test
  void fromProtoStatus_unrecognized_throws() {
    assertThatThrownBy(
            () ->
                ClientProtoMappers.fromProtoStatus(com.scalar.db.saga.rpc.SagaStatus.UNRECOGNIZED))
        .isInstanceOf(SagaRuntimeException.class);
  }

  @Test
  void fromProtoSkipReason_everyApiReason_mapsFromItsWireCounterpart() {
    // No client-side toProtoSkipReason to compose with (skip reasons only travel server → client),
    // so the wire enum is looked up by name instead of via a mirror mapper.
    for (ResetResult.SkipReason apiReason : ResetResult.SkipReason.values()) {
      com.scalar.db.saga.rpc.SkipReason wire =
          com.scalar.db.saga.rpc.SkipReason.valueOf("SKIP_REASON_" + apiReason.name());
      assertThat(ClientProtoMappers.fromProtoSkipReason(wire)).isEqualTo(apiReason);
    }
  }

  @Test
  void fromProtoSkipReason_unspecified_throws() {
    assertThatThrownBy(
            () ->
                ClientProtoMappers.fromProtoSkipReason(
                    com.scalar.db.saga.rpc.SkipReason.SKIP_REASON_UNSPECIFIED))
        .isInstanceOf(SagaRuntimeException.class);
  }

  @Test
  void fromProtoSkipReason_unrecognized_throws() {
    assertThatThrownBy(
            () ->
                ClientProtoMappers.fromProtoSkipReason(
                    com.scalar.db.saga.rpc.SkipReason.UNRECOGNIZED))
        .isInstanceOf(SagaRuntimeException.class);
  }
}
