package com.scalar.db.saga.api;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * One page of results from a paginated query, plus an opaque token for the next page.
 *
 * <h2>How to paginate</h2>
 *
 * <ul>
 *   <li><b>Drive the loop by the token, not the item count.</b> Keep calling the query with the
 *       previous page's {@link #getNextPageToken()} until it returns {@code null} (equivalently,
 *       until {@link #hasMore()} is {@code false}). Do <b>not</b> stop just because a page returned
 *       fewer items than requested.
 *   <li><b>The item count is approximate.</b> A page may hold slightly more or fewer than the
 *       requested {@code pageSize} (see {@link SagaQuery.Builder#pageSize(int)}), so never build
 *       logic on {@code getItems().size() == pageSize}.
 *   <li><b>The last page may be empty.</b> A non-{@code null} token does not guarantee the next
 *       page has items — the final call can return an empty {@link #getItems()} with a {@code null}
 *       token. The token-driven loop above handles this correctly.
 * </ul>
 *
 * <pre>{@code
 * String token = null;
 * do {
 *   SagaPage<SagaStateSnapshot> page = admin.listSagas(query.pageToken(token).build());
 *   process(page.getItems());
 *   token = page.getNextPageToken();
 * } while (token != null);
 * }</pre>
 *
 * @param <T> the element type
 */
@Immutable
public final class SagaPage<T> {

  private final List<T> items;
  private final @Nullable String nextPageToken;

  /**
   * Creates a page.
   *
   * @param items the results in this page (defensively copied)
   * @param nextPageToken the token to fetch the next page, or {@code null} if this is the last page
   */
  public SagaPage(List<T> items, @Nullable String nextPageToken) {
    Objects.requireNonNull(items, "items must not be null");
    this.items = Collections.unmodifiableList(new java.util.ArrayList<>(items));
    this.nextPageToken = nextPageToken;
  }

  /**
   * The results in this page (unmodifiable). The count is the requested {@code pageSize} at most in
   * the common case, but may be slightly larger when the underlying query completes a group of
   * equal-keyed rows straddling the page boundary rather than splitting it.
   */
  public List<T> getItems() {
    return items;
  }

  /** The token to fetch the next page, or {@code null} if this is the last page. */
  public @Nullable String getNextPageToken() {
    return nextPageToken;
  }

  /** Returns {@code true} if there is a next page to fetch. */
  public boolean hasMore() {
    return nextPageToken != null;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaPage)) return false;
    SagaPage<?> that = (SagaPage<?>) o;
    return items.equals(that.items) && Objects.equals(nextPageToken, that.nextPageToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(items, nextPageToken);
  }

  @Override
  public String toString() {
    return "SagaPage{items=" + items + ", nextPageToken='" + nextPageToken + "'}";
  }
}
