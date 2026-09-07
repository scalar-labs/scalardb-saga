package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Pins that HTTP handlers run on virtual threads, so a request waiting on its saga costs a parked
 * virtual thread rather than one of {@code http.max_threads} OS threads.
 *
 * <p>Worth asserting directly rather than trusting the configuration: the wiring is a single {@code
 * setVirtualThreadsExecutor} call, and losing it would leave the daemon perfectly healthy — every
 * request still served, every probe still green — while silently restoring the thread ceiling this
 * exists to remove. gRPC has always run its handlers on virtual threads; this covers the HTTP side
 * that {@code todos/076} brought into line.
 */
class HttpVirtualThreadTest {

  // The handler's wait must outlast awaitPeak's, so a loaded runner cannot make them race.
  private static final long HANDLER_RELEASE_TIMEOUT_SECONDS = 30L;
  private static final long PEAK_WAIT_SECONDS = 10L;

  private static SagaServerConfig config(int maxThreads) {
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.HTTP_MAX_THREADS_KEY, Integer.toString(maxThreads));
    props.setProperty(SagaServerConfig.HTTP_MIN_THREADS_KEY, "2");
    return SagaServerConfig.load(props);
  }

  @Test
  @Timeout(60)
  void handler_onTheConfiguredHttpServer_runsOnAVirtualThread() throws Exception {
    // Arrange
    AtomicBoolean virtual = new AtomicBoolean();
    ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
    Javalin app = SagaServer.createHttpServer(config(8), null, virtualThreads);
    app.get("/probe", ctx -> virtual.set(Thread.currentThread().isVirtual()));
    app.start(0);

    // Act
    try {
      send(app, "/probe");
    } finally {
      app.stop();
      virtualThreads.shutdown();
    }

    // Assert
    assertThat(virtual.get()).isTrue();
  }

  @Test
  @Timeout(90)
  void createHttpServer_moreConcurrentBlockingHandlersThanMaxThreads_allRunAtOnce()
      throws Exception {
    // Arrange — a deliberately tiny pool, and far more requests than it has threads. Each request
    // blocks inside the handler, which is what a synchronous saga start does while it waits.
    int maxThreads = 4;
    int requests = 20;
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger concurrent = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    // A handler that timed out instead of being released would make the peak meaningless, so the
    // outcome of the wait is recorded and asserted rather than discarded.
    AtomicBoolean releasedCleanly = new AtomicBoolean(true);

    ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
    Javalin app = SagaServer.createHttpServer(config(maxThreads), null, virtualThreads);
    app.get(
        "/block",
        ctx -> {
          peak.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
          // Comfortably longer than awaitPeak's own budget below: these two waits race, and if a
          // handler gives up first the peak collapses and the test fails for a reason unrelated to
          // what it asserts. @Timeout is the real backstop, so this only has to lose that race.
          if (!release.await(HANDLER_RELEASE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            releasedCleanly.set(false);
          }
          concurrent.decrementAndGet();
        });
    app.start(0);

    // Act — fire them all, let them pile up inside the handler, then release.
    try {
      List<CompletableFuture<HttpResponse<String>>> inFlight = new ArrayList<>();
      HttpClient client = HttpClient.newHttpClient();
      for (int i = 0; i < requests; i++) {
        inFlight.add(
            client.sendAsync(
                HttpRequest.newBuilder(uri(app, "/block")).build(), BodyHandlers.ofString()));
      }
      awaitPeak(peak, requests);
      release.countDown();
      for (CompletableFuture<HttpResponse<String>> f : inFlight) {
        f.get(10, TimeUnit.SECONDS);
      }
    } finally {
      app.stop();
      virtualThreads.shutdown();
    }

    // Assert — on platform threads the peak cannot exceed the pool; here every request sits in the
    // handler at once, so the pool size is no longer the ceiling on concurrent waits.
    assertThat(releasedCleanly.get()).isTrue();
    assertThat(peak.get()).isGreaterThan(maxThreads);
  }

  /** Polls rather than sleeping a fixed interval, so the test is fast and not timing-fragile. */
  private static void awaitPeak(AtomicInteger peak, int target) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PEAK_WAIT_SECONDS);
    while (peak.get() < target && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
  }

  private static URI uri(Javalin app, String path) {
    return URI.create("http://localhost:" + app.port() + path);
  }

  private static void send(Javalin app, String path) throws Exception {
    HttpClient.newHttpClient()
        .send(HttpRequest.newBuilder(uri(app, path)).build(), BodyHandlers.ofString());
  }
}
