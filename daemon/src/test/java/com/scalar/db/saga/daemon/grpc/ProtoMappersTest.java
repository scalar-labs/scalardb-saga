package com.scalar.db.saga.daemon.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Unit-tests {@link ProtoMappers} — the api&rarr;wire conversions at the server adapter boundary.
 */
class ProtoMappersTest {

  @Test
  void toProtoStatus_everyApiStatus_mapsByNameToANonUnspecifiedWireStatus() {
    // Every api status must have a wire counterpart named SAGA_STATUS_<name>; an unmapped one makes
    // SagaStatus.valueOf throw, failing loudly rather than degrading to UNSPECIFIED.
    for (SagaStatus status : SagaStatus.values()) {
      com.scalar.db.saga.rpc.SagaStatus wire = ProtoMappers.toProtoStatus(status);
      assertThat(wire.name()).isEqualTo("SAGA_STATUS_" + status.name());
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
  void toProto_mapsAllClientFacingFields() {
    // Arrange
    Instant created = Instant.ofEpochSecond(1_700_000_000L, 250);
    Instant updated = Instant.ofEpochSecond(1_700_000_500L, 0);
    SagaStateSnapshot snapshot =
        new SagaStateSnapshot(
            "s-1", "transfer", SagaStatus.COMPENSATING, "owner-7", "v3", created, updated);

    // Act
    com.scalar.db.saga.rpc.SagaSnapshot proto = ProtoMappers.toProto(snapshot);

    // Assert
    assertThat(proto.getSagaId()).isEqualTo("s-1");
    assertThat(proto.getName()).isEqualTo("transfer");
    assertThat(proto.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPENSATING);
    assertThat(proto.getDefinitionVersion()).isEqualTo("v3");
    assertThat(proto.getCreatedAt().getSeconds()).isEqualTo(1_700_000_000L);
    assertThat(proto.getCreatedAt().getNanos()).isEqualTo(250);
    assertThat(proto.getUpdatedAt().getSeconds()).isEqualTo(1_700_000_500L);
    assertThat(proto.getUpdatedAt().getNanos()).isZero();
  }

  @Test
  void sagaSnapshot_doesNotExposeOwnerIdOnTheWire() {
    // owner_id is a server-internal recovery field, deliberately absent from the wire contract.
    assertThat(com.scalar.db.saga.rpc.SagaSnapshot.getDescriptor().findFieldByName("owner_id"))
        .isNull();
  }
}
