package com.scalar.db.saga.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaRuntimeException;
import org.junit.jupiter.api.Test;

class ClientProtoMappersTest {

  @Test
  void fromProtoStatus_everyApiStatusRoundTripsByName() {
    for (SagaStatus apiStatus : SagaStatus.values()) {
      com.scalar.db.saga.rpc.SagaStatus wire =
          com.scalar.db.saga.rpc.SagaStatus.valueOf("SAGA_STATUS_" + apiStatus.name());
      assertThat(ClientProtoMappers.fromProtoStatus(wire)).isEqualTo(apiStatus);
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
  void fromProtoSkipReason_everyApiReasonRoundTripsByName() {
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
