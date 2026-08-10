package com.scalar.db.saga.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Timestamp;
import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.rpc.ListSagasRequest;
import com.scalar.db.saga.rpc.ResetEscalatedBulkRequest;
import java.time.Instant;
import java.util.List;
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
  void toProtoSkipReason_everyApiReason_mapsByNameToANonUnspecifiedWireReason() {
    // Every api skip reason must have a wire counterpart named SKIP_REASON_<name>; an unmapped one
    // now throws IllegalStateException (mapped to INTERNAL), failing loudly rather than
    // mislabelled.
    for (ResetResult.SkipReason reason : ResetResult.SkipReason.values()) {
      com.scalar.db.saga.rpc.SkipReason wire = ProtoMappers.toProtoSkipReason(reason);
      assertThat(wire.name()).isEqualTo("SKIP_REASON_" + reason.name());
      assertThat(wire).isNotEqualTo(com.scalar.db.saga.rpc.SkipReason.SKIP_REASON_UNSPECIFIED);
    }
  }

  @Test
  void toProtoSkipReason_everyWireReason_hasAnApiCounterpart() {
    // Guards drift the other way: a wire skip reason added without an api counterpart fails here.
    for (com.scalar.db.saga.rpc.SkipReason wire : com.scalar.db.saga.rpc.SkipReason.values()) {
      if (wire == com.scalar.db.saga.rpc.SkipReason.SKIP_REASON_UNSPECIFIED
          || wire == com.scalar.db.saga.rpc.SkipReason.UNRECOGNIZED) {
        continue;
      }
      String apiName = wire.name().substring("SKIP_REASON_".length());
      assertThatCode(() -> ResetResult.SkipReason.valueOf(apiName)).doesNotThrowAnyException();
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
  void toSagaQuery_listSagasWithInRangeTimestamp_mapsTheWindow() {
    // A well-formed updated_after within Instant's range maps straight through.
    ListSagasRequest request =
        ListSagasRequest.newBuilder()
            .setUpdatedAfter(Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(250))
            .build();

    SagaQuery query = ProtoMappers.toSagaQuery(request);

    assertThat(query.getUpdatedAfter()).isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 250));
  }

  @Test
  void toSagaQuery_listSagasWithOutOfRangeTimestampGiven_throwsIllegalArgument() {
    // A seconds value past Instant's range is bad client input; it must surface as
    // IllegalArgumentException (mapped to INVALID_ARGUMENT) rather than a DateTimeException that
    // would fall through to INTERNAL.
    ListSagasRequest request =
        ListSagasRequest.newBuilder()
            .setUpdatedAfter(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE).build())
            .build();

    assertThatThrownBy(() -> ProtoMappers.toSagaQuery(request))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void toSagaQuery_bulkResetWithOutOfRangeTimestampGiven_throwsIllegalArgument() {
    // Same guard on the bulk-reset overload, which shares the timestamp conversion.
    ResetEscalatedBulkRequest request =
        ResetEscalatedBulkRequest.newBuilder()
            .setUpdatedBefore(Timestamp.newBuilder().setSeconds(Long.MIN_VALUE).build())
            .build();

    assertThatThrownBy(() -> ProtoMappers.toSagaQuery(request))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void sagaSnapshot_doesNotExposeOwnerIdOnTheWire() {
    // owner_id is a server-internal recovery field, deliberately absent from the wire contract.
    assertThat(com.scalar.db.saga.rpc.SagaSnapshot.getDescriptor().findFieldByName("owner_id"))
        .isNull();
  }

  @Test
  void fromProtoStatus_inverseOfToProtoStatus_forEveryApiStatus() {
    for (SagaStatus status : SagaStatus.values()) {
      assertThat(ProtoMappers.fromProtoStatus(ProtoMappers.toProtoStatus(status)))
          .isEqualTo(status);
    }
  }

  @Test
  void fromProtoStatus_unspecifiedGiven_throwsIllegalArgument() {
    assertThatThrownBy(
            () ->
                ProtoMappers.fromProtoStatus(
                    com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_UNSPECIFIED))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromProtoStatus_unrecognizedGiven_throwsIllegalArgument() {
    assertThatThrownBy(
            () -> ProtoMappers.fromProtoStatus(com.scalar.db.saga.rpc.SagaStatus.UNRECOGNIZED))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void toSagaQuery_listRequestAllFieldsGiven_mapsEachField() {
    // Arrange
    Instant after = Instant.ofEpochSecond(1_700_000_000L, 0);
    Instant before = Instant.ofEpochSecond(1_700_100_000L, 0);
    ListSagasRequest request =
        ListSagasRequest.newBuilder()
            .setStatus(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_ESCALATED)
            .setUpdatedAfter(Timestamp.newBuilder().setSeconds(after.getEpochSecond()).build())
            .setUpdatedBefore(Timestamp.newBuilder().setSeconds(before.getEpochSecond()).build())
            .setPageSize(50)
            .setPageToken("tok")
            .build();

    // Act
    SagaQuery query = ProtoMappers.toSagaQuery(request);

    // Assert
    assertThat(query.getStatus()).isEqualTo(SagaStatus.ESCALATED);
    assertThat(query.getUpdatedAfter()).isEqualTo(after);
    assertThat(query.getUpdatedBefore()).isEqualTo(before);
    assertThat(query.getPageSize()).isEqualTo(50);
    assertThat(query.getPageToken()).isEqualTo("tok");
  }

  @Test
  void toSagaQuery_listRequestNoFieldsGiven_mapsToAnEmptyQuery() {
    // Act
    SagaQuery query = ProtoMappers.toSagaQuery(ListSagasRequest.getDefaultInstance());

    // Assert — every optional field absent maps to an unset api field
    assertThat(query.getStatus()).isNull();
    assertThat(query.getUpdatedAfter()).isNull();
    assertThat(query.getUpdatedBefore()).isNull();
    assertThat(query.getPageToken()).isNull();
  }

  @Test
  void toSagaQuery_bulkRequestGiven_ignoresStatusAndMapsWindowAndPaging() {
    // Arrange
    Instant after = Instant.ofEpochSecond(1_700_000_000L, 0);
    ResetEscalatedBulkRequest request =
        ResetEscalatedBulkRequest.newBuilder()
            .setReason("sweep")
            .setUpdatedAfter(Timestamp.newBuilder().setSeconds(after.getEpochSecond()).build())
            .setPageSize(25)
            .setPageToken("tok")
            .build();

    // Act
    SagaQuery query = ProtoMappers.toSagaQuery(request);

    // Assert — the bulk sweep does not accept a status filter; the engine pins it to ESCALATED
    assertThat(query.getStatus()).isNull();
    assertThat(query.getUpdatedAfter()).isEqualTo(after);
    assertThat(query.getPageSize()).isEqualTo(25);
    assertThat(query.getPageToken()).isEqualTo("tok");
  }

  @Test
  void toProto_resetResultWithSkipAndToken_mapsCountReasonAndToken() {
    // Arrange
    ResetResult result =
        new ResetResult(
            3,
            List.of(new ResetResult.SkippedSaga("s2", ResetResult.SkipReason.CORRUPT_EVENT_STREAM)),
            "next-token");

    // Act
    com.scalar.db.saga.rpc.ResetResult proto = ProtoMappers.toProto(result);

    // Assert
    assertThat(proto.getResetCount()).isEqualTo(3);
    assertThat(proto.getSkippedCount()).isEqualTo(1);
    assertThat(proto.getSkipped(0).getSagaId()).isEqualTo("s2");
    assertThat(proto.getSkipped(0).getReason())
        .isEqualTo(com.scalar.db.saga.rpc.SkipReason.SKIP_REASON_CORRUPT_EVENT_STREAM);
    // detail is unset when the api detail is null (matches the 016 fix — no message leaked)
    assertThat(proto.getSkipped(0).hasDetail()).isFalse();
    assertThat(proto.getNextPageToken()).isEqualTo("next-token");
  }

  @Test
  void toProto_resetResultNoSkipsNoToken_omitsToken() {
    // Act
    com.scalar.db.saga.rpc.ResetResult proto =
        ProtoMappers.toProto(new ResetResult(5, List.of(), null));

    // Assert
    assertThat(proto.getResetCount()).isEqualTo(5);
    assertThat(proto.getSkippedList()).isEmpty();
    assertThat(proto.hasNextPageToken()).isFalse();
  }
}
