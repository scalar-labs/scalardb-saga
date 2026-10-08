package com.scalar.db.saga.store;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.io.Column;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Counts what Consensus Commit asks of the storage underneath it: calls, the records they write,
 * and the rows and bytes reads return. Built as a dynamic proxy so it follows {@link
 * DistributedStorage} without restating its methods.
 */
final class CountingStorage {

  /** The storage's page size for scans ({@code scalar.db.scan_fetch_size}, default 10). */
  static final int SCAN_FETCH_SIZE = 10;

  private long calls;
  private long scanPages;
  private long recordsWritten;
  private long rowsRead;
  private long bytesRead;

  /** A snapshot of the counters. */
  record Cost(long calls, long scanPages, long recordsWritten, long rowsRead, long bytesRead) {

    /** Round trips: every call, plus each extra page a scan had to fetch. */
    long roundTrips() {
      return calls + scanPages;
    }
  }

  DistributedStorage wrap(DistributedStorage storage) {
    return (DistributedStorage)
        Proxy.newProxyInstance(
            DistributedStorage.class.getClassLoader(),
            new Class<?>[] {DistributedStorage.class},
            handler(storage));
  }

  synchronized void reset() {
    calls = 0;
    scanPages = 0;
    recordsWritten = 0;
    rowsRead = 0;
    bytesRead = 0;
  }

  synchronized Cost cost() {
    return new Cost(calls, scanPages, recordsWritten, rowsRead, bytesRead);
  }

  private InvocationHandler handler(DistributedStorage storage) {
    return (proxy, method, args) -> {
      Object result = invoke(storage, method, args);
      switch (method.getName()) {
        case "get" -> {
          synchronized (this) {
            calls++;
          }
          ((Optional<?>) Objects.requireNonNull(result)).ifPresent(r -> countRow((Result) r));
        }
        case "scan" -> {
          synchronized (this) {
            calls++;
          }
          return countingScanner((Scanner) Objects.requireNonNull(result));
        }
        case "put", "delete", "mutate" -> {
          synchronized (this) {
            calls++;
            recordsWritten += args[0] instanceof List<?> list ? list.size() : 1;
          }
        }
        default -> {}
      }
      return result;
    };
  }

  private Scanner countingScanner(Scanner scanner) {
    long[] rowsInThisScan = {0};
    Runnable onRow =
        () -> {
          rowsInThisScan[0]++;
          // The first page rides on the scan call; every further page is one more round trip.
          if (rowsInThisScan[0] > SCAN_FETCH_SIZE
              && (rowsInThisScan[0] - 1) % SCAN_FETCH_SIZE == 0) {
            synchronized (this) {
              scanPages++;
            }
          }
        };
    return (Scanner)
        Proxy.newProxyInstance(
            Scanner.class.getClassLoader(),
            new Class<?>[] {Scanner.class},
            (proxy, method, args) -> {
              Object result = invoke(scanner, method, args);
              switch (method.getName()) {
                case "one" ->
                    ((Optional<?>) Objects.requireNonNull(result))
                        .ifPresent(
                            r -> {
                              onRow.run();
                              countRow((Result) r);
                            });
                case "all" ->
                    ((List<?>) Objects.requireNonNull(result))
                        .forEach(
                            r -> {
                              onRow.run();
                              countRow((Result) r);
                            });
                case "iterator" -> {
                  @SuppressWarnings("unchecked")
                  Iterator<Result> it = (Iterator<Result>) Objects.requireNonNull(result);
                  return new Iterator<Result>() {
                    @Override
                    public boolean hasNext() {
                      return it.hasNext();
                    }

                    @Override
                    public Result next() {
                      Result r = it.next();
                      onRow.run();
                      countRow(r);
                      return r;
                    }
                  };
                }
                default -> {}
              }
              return result;
            });
  }

  private synchronized void countRow(Result row) {
    rowsRead++;
    for (Column<?> column : row.getColumns().values()) {
      bytesRead += sizeOf(column.getValueAsObject());
    }
  }

  private static long sizeOf(@Nullable Object value) {
    if (value == null) {
      return 0;
    }
    if (value instanceof String s) {
      return s.getBytes(StandardCharsets.UTF_8).length;
    }
    if (value instanceof byte[] bytes) {
      return bytes.length;
    }
    if (value instanceof ByteBuffer buffer) {
      return buffer.remaining();
    }
    if (value instanceof Integer || value instanceof Float) {
      return 4;
    }
    if (value instanceof Boolean) {
      return 1;
    }
    return 8;
  }

  private static @Nullable Object invoke(Object target, Method method, @Nullable Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
