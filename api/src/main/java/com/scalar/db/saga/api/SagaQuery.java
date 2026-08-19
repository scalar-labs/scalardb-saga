package com.scalar.db.saga.api;

import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * A paginated query over saga state snapshots, used by the Admin API's listing operation.
 *
 * <p>Filtering is limited to what the {@code saga_state} clustering key supports efficiently: an
 * optional {@link SagaStatus} and an optional {@code updated_at} time window. There is deliberately
 * no name filter — {@code saga_name} is not in the clustering key and is not selective, so
 * filtering by it would require an unindexed full scan.
 *
 * <p>Pagination uses an opaque {@link #getPageToken() page token}. Pass {@code null} (or omit it)
 * to start from the beginning; pass the token from the previous {@link SagaPage#getNextPageToken()}
 * to continue. Listing is best-effort under concurrent mutation.
 */
@Immutable
public final class SagaQuery {

  /** Default page size when none is specified. */
  public static final int DEFAULT_PAGE_SIZE = 100;

  /**
   * The maximum <b>requested</b> {@link #getPageSize() page size}. This bounds the requested
   * target, not the exact size of a returned {@link SagaPage}. Because a page never splits a cohort
   * (rows sharing one {@code updated_at}), a returned page may exceed the target — and even this
   * maximum — by up to a full cohort; see {@link Builder#pageSize(int)}.
   */
  public static final int MAX_PAGE_SIZE = 1000;

  private final @Nullable SagaStatus status;
  private final @Nullable Instant updatedAfter;
  private final @Nullable Instant updatedBefore;
  private final int pageSize;
  private final @Nullable String pageToken;

  private SagaQuery(Builder builder) {
    this.status = builder.status;
    this.updatedAfter = builder.updatedAfter;
    this.updatedBefore = builder.updatedBefore;
    this.pageSize = builder.pageSize;
    this.pageToken = builder.pageToken;
  }

  /** Returns a new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** The status to filter by, or {@code null} to list all statuses. */
  public @Nullable SagaStatus getStatus() {
    return status;
  }

  /** Inclusive lower bound on {@code updated_at}, or {@code null} for no lower bound. */
  public @Nullable Instant getUpdatedAfter() {
    return updatedAfter;
  }

  /** Inclusive upper bound on {@code updated_at}, or {@code null} for no upper bound. */
  public @Nullable Instant getUpdatedBefore() {
    return updatedBefore;
  }

  /**
   * The <b>target</b> number of results per page. A returned {@link SagaPage} may exceed this
   * target by up to a full {@code updated_at} cohort, which is not bounded by the target — see
   * {@link Builder#pageSize(int)}.
   */
  public int getPageSize() {
    return pageSize;
  }

  /** The opaque continuation token, or {@code null} to start from the beginning. */
  public @Nullable String getPageToken() {
    return pageToken;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaQuery)) return false;
    SagaQuery that = (SagaQuery) o;
    return pageSize == that.pageSize
        && status == that.status
        && Objects.equals(updatedAfter, that.updatedAfter)
        && Objects.equals(updatedBefore, that.updatedBefore)
        && Objects.equals(pageToken, that.pageToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, updatedAfter, updatedBefore, pageSize, pageToken);
  }

  @Override
  public String toString() {
    return "SagaQuery{"
        + "status="
        + status
        + ", updatedAfter="
        + updatedAfter
        + ", updatedBefore="
        + updatedBefore
        + ", pageSize="
        + pageSize
        + ", pageToken='"
        + pageToken
        + "'}";
  }

  /** Builder for {@link SagaQuery}. */
  public static final class Builder {

    private @Nullable SagaStatus status;
    private @Nullable Instant updatedAfter;
    private @Nullable Instant updatedBefore;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private @Nullable String pageToken;

    private Builder() {}

    /** Sets the status filter (or {@code null} to list all statuses). */
    public Builder status(@Nullable SagaStatus status) {
      this.status = status;
      return this;
    }

    /** Sets the inclusive {@code updated_at} lower bound (or {@code null} for no lower bound). */
    public Builder updatedAfter(@Nullable Instant updatedAfter) {
      this.updatedAfter = updatedAfter;
      return this;
    }

    /** Sets the inclusive {@code updated_at} upper bound (or {@code null} for no upper bound). */
    public Builder updatedBefore(@Nullable Instant updatedBefore) {
      this.updatedBefore = updatedBefore;
      return this;
    }

    /**
     * Sets the <b>target</b> page size. A returned {@link SagaPage} may exceed this target: results
     * are grouped by {@code updated_at} and a page never splits a cohort (rows sharing one
     * timestamp within one internal scan slice), so a cohort straddling the limit is completed
     * rather than cut. A single cohort larger than the target is returned whole as an over-sized
     * page (a mass event spread across slices yields several such pages, not one), so a page may
     * exceed the target — and even {@link SagaQuery#MAX_PAGE_SIZE} — by the full cohort size; it is
     * not an upper bound on the returned item count. Callers should provision memory and response
     * limits for this target plus the largest expected cohort: a page can already hold up to a full
     * target of rows from earlier scan slices when the cohort that overflows it is completed. This
     * bounds the requested target only, in {@code [1, MAX_PAGE_SIZE]}.
     *
     * @param pageSize the target results per page, in {@code [1, MAX_PAGE_SIZE]}
     * @throws IllegalArgumentException if out of range
     */
    public Builder pageSize(int pageSize) {
      if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
        throw new IllegalArgumentException(
            "pageSize must be in [1, " + MAX_PAGE_SIZE + "]: " + pageSize);
      }
      this.pageSize = pageSize;
      return this;
    }

    /** Sets the opaque continuation token. */
    public Builder pageToken(@Nullable String pageToken) {
      this.pageToken = pageToken;
      return this;
    }

    /**
     * Builds the query.
     *
     * @throws IllegalArgumentException if both {@code updatedAfter} and {@code updatedBefore} are
     *     set and {@code updatedAfter} is strictly after {@code updatedBefore} (an empty window).
     *     The bounds are inclusive, so an equal {@code updatedAfter} and {@code updatedBefore} is
     *     allowed and selects that single instant.
     */
    public SagaQuery build() {
      if (updatedAfter != null && updatedBefore != null && updatedAfter.isAfter(updatedBefore)) {
        throw new IllegalArgumentException(
            "updatedAfter must be before or equal to updatedBefore: updatedAfter="
                + updatedAfter
                + ", updatedBefore="
                + updatedBefore);
      }
      return new SagaQuery(this);
    }
  }
}
