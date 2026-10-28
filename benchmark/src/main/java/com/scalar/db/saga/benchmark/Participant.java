package com.scalar.db.saga.benchmark;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * The fake downstream service the daemon-mode benchmark saga calls: answers {@code POST /step/i}
 * and {@code POST /undo/i} with {@code 200 {}} after an optional delay that emulates participant
 * latency.
 *
 * <p>Like {@link BenchmarkStep}, it keys execution calls by the {@code X-Saga-Id} correlation
 * header the engine propagates, so {@link #duplicateExecutions()} exposes the same
 * re-driven-while-running signature for the server modes that the step exposes embedded.
 */
@ThreadSafe
final class Participant implements AutoCloseable {

  private static final String SAGA_ID_HEADER = "X-Saga-Id";
  private static final byte[] BODY = "{}".getBytes(StandardCharsets.UTF_8);

  private final HttpServer server;
  private final ExecutorService executor;
  private final long delayMillis;
  private final LongAdder stepCalls = new LongAdder();
  private final LongAdder undoCalls = new LongAdder();
  // One entry per (step path, saga id) execution ever seen; step calls beyond these are duplicates.
  private final Set<String> distinctStepCalls = ConcurrentHashMap.newKeySet();
  // Correlation-less calls cannot be deduplicated; distinguish them instead of miscounting.
  private final AtomicLong uncorrelated = new AtomicLong();

  private Participant(HttpServer server, ExecutorService executor, long delayMillis) {
    this.server = server;
    this.executor = executor;
    this.delayMillis = delayMillis;
  }

  /** Starts a participant on an ephemeral loopback port. */
  static Participant start(long delayMillis) throws IOException {
    if (delayMillis < 0) {
      throw new IllegalArgumentException("delayMillis must be >= 0, got " + delayMillis);
    }
    // The IPv4 loopback by address, not by name: no resolver involved, and the bound address is
    // guaranteed to match the 127.0.0.1 base URL handed to the daemon.
    InetAddress loopback = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    HttpServer server = HttpServer.create(new InetSocketAddress(loopback, 0), 0);
    AtomicLong index = new AtomicLong();
    ExecutorService executor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "saga-bench-participant-" + index.incrementAndGet());
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(executor);
    Participant participant = new Participant(server, executor, delayMillis);
    server.createContext("/", participant::handle);
    server.start();
    return participant;
  }

  /** The base URL the daemon should register as the {@code bench} service. */
  String baseUrl() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  /** Total execution ({@code /step/*}) calls. */
  long stepCalls() {
    return stepCalls.sum();
  }

  /** Total compensation ({@code /undo/*}) calls. */
  long undoCalls() {
    return undoCalls.sum();
  }

  /**
   * Execution calls beyond the first per (step, saga): the server-mode analogue of {@link
   * BenchmarkStep#duplicateExecutions()}. Calls without a saga correlation header are excluded
   * (they cannot be attributed) and counted separately.
   */
  long duplicateExecutions() {
    return stepCalls.sum() - uncorrelated.get() - distinctStepCalls.size();
  }

  private void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    if (path.startsWith("/step/")) {
      stepCalls.increment();
      @Nullable String sagaId = exchange.getRequestHeaders().getFirst(SAGA_ID_HEADER);
      if (sagaId == null) {
        uncorrelated.incrementAndGet();
      } else {
        distinctStepCalls.add(path + '|' + sagaId);
      }
    } else if (path.startsWith("/undo/")) {
      undoCalls.increment();
    } else {
      respond(exchange, 404);
      return;
    }
    sleepDelay();
    respond(exchange, 200);
  }

  private void sleepDelay() {
    if (delayMillis <= 0) {
      return;
    }
    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void respond(HttpExchange exchange, int status) throws IOException {
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, BODY.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(BODY);
    }
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
