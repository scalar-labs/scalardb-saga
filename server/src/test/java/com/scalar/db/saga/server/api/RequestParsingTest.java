package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class RequestParsingTest {

  // --- buildQuery ------------------------------------------------------------

  @Test
  void buildQuery_allParamsGiven_buildsTheQuery() {
    // Arrange
    Instant after = Instant.parse("2026-07-18T10:00:00Z");
    Instant before = Instant.parse("2026-07-18T11:00:00Z");

    // Act
    SagaQuery query =
        RequestParsing.buildQuery(
            params ->
                params
                    .status(SagaStatus.RUNNING)
                    .updatedAfter(after)
                    .updatedBefore(before)
                    .pageSize(50)
                    .pageToken("token-1"));

    // Assert
    assertThat(query.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(query.getUpdatedAfter()).isEqualTo(after);
    assertThat(query.getUpdatedBefore()).isEqualTo(before);
    assertThat(query.getPageSize()).isEqualTo(50);
    assertThat(query.getPageToken()).isEqualTo("token-1");
  }

  @Test
  void buildQuery_noParamsGiven_buildsAnUnfilteredQuery() {
    // Act
    SagaQuery query = RequestParsing.buildQuery(params -> {});

    // Assert
    assertThat(query.getStatus()).isNull();
    assertThat(query.getUpdatedAfter()).isNull();
    assertThat(query.getUpdatedBefore()).isNull();
    assertThat(query.getPageSize()).isEqualTo(SagaQuery.DEFAULT_PAGE_SIZE);
    assertThat(query.getPageToken()).isNull();
  }

  @Test
  void buildQuery_pageSizeAboveTheBound_convertsTheBuilderRejection() {
    // Arrange — the builder validates pageSize from the setter, not from build().
    // Act & Assert
    assertThatThrownBy(() -> RequestParsing.buildQuery(params -> params.pageSize(9999)))
        .isInstanceOf(SagaIllegalArgumentException.class)
        .hasMessageContaining(String.valueOf(SagaQuery.MAX_PAGE_SIZE))
        .hasMessageContaining("9999");
  }

  @Test
  void buildQuery_emptyUpdatedAtWindow_convertsTheBuilderRejection() {
    // Arrange — and this one only from build(), which is why the guarded block spans both.
    Instant after = Instant.parse("2026-07-18T11:00:00Z");
    Instant before = Instant.parse("2026-07-18T10:00:00Z");

    // Act & Assert
    assertThatThrownBy(
            () ->
                RequestParsing.buildQuery(
                    params -> params.updatedAfter(after).updatedBefore(before)))
        .isInstanceOf(SagaIllegalArgumentException.class)
        .hasMessageContaining("2026-07-18T11:00:00Z")
        .hasMessageContaining("2026-07-18T10:00:00Z");
  }

  @Test
  void buildQuery_paramsThrowIllegalArgument_leavesItUnconverted() {
    // Arrange — stands in for a future Integer.parseInt or Enum.valueOf inside a caller's lambda.
    // Converting it would echo wording the builder never authored to the caller as their own fault,
    // and would skip the error mapper's catch-all, which is where such a throw is logged.
    // Collecting
    // the params outside the guarded block is what keeps that from happening, so this is the test
    // that fails if the try is ever widened back over the caller's code.
    IllegalArgumentException raisedByTheCaller = new IllegalArgumentException("not the builder's");

    // Act & Assert
    assertThatThrownBy(
            () ->
                RequestParsing.buildQuery(
                    params -> {
                      throw raisedByTheCaller;
                    }))
        .isSameAs(raisedByTheCaller);
  }

  @Test
  void buildQuery_paramsThrowTypedRejection_leavesItUnconverted() {
    // A caller's own parser reports a malformed field as INVALID_REQUEST; the conversion here must
    // not relabel it as INVALID_ARGUMENT.
    SagaInvalidRequestException raisedByTheCaller =
        new SagaInvalidRequestException("'pageSize' is not an integer");

    // Act & Assert
    assertThatThrownBy(
            () ->
                RequestParsing.buildQuery(
                    params -> {
                      throw raisedByTheCaller;
                    }))
        .isSameAs(raisedByTheCaller);
  }

  // --- parseInstant ----------------------------------------------------------

  @Test
  void parseInstant_validIso8601Given_returnsTheInstant() {
    // Act & Assert
    assertThat(RequestParsing.parseInstant("2026-07-18T10:00:00Z", "updatedAfter"))
        .isEqualTo(Instant.parse("2026-07-18T10:00:00Z"));
  }

  @Test
  void parseInstant_malformedValueGiven_throwsInvalidRequestNamingTheField() {
    // Act & Assert
    assertThatThrownBy(() -> RequestParsing.parseInstant("not-a-date", "updatedAfter"))
        .isInstanceOf(SagaInvalidRequestException.class)
        .hasMessageContaining("updatedAfter");
  }
}
