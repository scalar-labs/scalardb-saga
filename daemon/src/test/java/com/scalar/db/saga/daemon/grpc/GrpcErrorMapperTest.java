package com.scalar.db.saga.daemon.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * Unit-tests {@link GrpcErrorMapper}'s server-fault vs client-fault distinction — the two exception
 * types whose gRPC code differs. An {@link IllegalStateException} (a server-side invariant
 * violation, e.g. an api/proto enum skew on the response path) must map to {@code INTERNAL}, not
 * the client-facing {@code INVALID_ARGUMENT} that {@link IllegalArgumentException} (an
 * engine-rejected client value) maps to. Either way only a fixed, daemon-owned description is
 * surfaced.
 */
class GrpcErrorMapperTest {

  @Test
  void toStatusRuntimeException_illegalStateGiven_mapsToInternal() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new IllegalStateException("server invariant"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    // The internal message is never surfaced; a fixed daemon-owned description is.
    assertThat(e.getStatus().getDescription()).isEqualTo("Internal server error");
  }

  @Test
  void toStatusRuntimeException_illegalArgumentGiven_mapsToInvalidArgument() {
    StatusRuntimeException e =
        GrpcErrorMapper.toStatusRuntimeException(new IllegalArgumentException("bad client value"));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(e.getStatus().getDescription()).isEqualTo("Invalid request parameter");
  }
}
