package com.scalar.db.saga.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaRuntimeException;
import org.junit.jupiter.api.Test;

class ClientProtoMappersTest {

  @Test
  void toApiStatus_everyApiStatusRoundTripsByName() {
    for (SagaStatus apiStatus : SagaStatus.values()) {
      com.scalar.db.saga.rpc.SagaStatus wire =
          com.scalar.db.saga.rpc.SagaStatus.valueOf("SAGA_STATUS_" + apiStatus.name());
      assertThat(ClientProtoMappers.toApiStatus(wire)).isEqualTo(apiStatus);
    }
  }

  @Test
  void toApiStatus_unspecified_throws() {
    assertThatThrownBy(
            () ->
                ClientProtoMappers.toApiStatus(
                    com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_UNSPECIFIED))
        .isInstanceOf(SagaRuntimeException.class);
  }

  @Test
  void toApiStatus_unrecognized_throws() {
    assertThatThrownBy(
            () -> ClientProtoMappers.toApiStatus(com.scalar.db.saga.rpc.SagaStatus.UNRECOGNIZED))
        .isInstanceOf(SagaRuntimeException.class);
  }
}
