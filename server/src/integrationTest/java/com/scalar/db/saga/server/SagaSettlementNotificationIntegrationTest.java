package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Guards the one seam nothing else covers: that the daemon actually installs its {@link
 * SagaWaiterRegistry} on the engine as a settlement listener.
 *
 * <p>Both halves of that seam are tested in isolation — the engine notifies a listener ({@code
 * DefaultSagaOrchestratorTest}), and the transports wake a waiter when the registry fires ({@code
 * SagaResourceStartTest}, {@code SagaServiceImplTest}) — but both of those inject a registry
 * directly. Deleting {@code .settlementListener(waiterRegistry)} from {@link SagaServer} leaves
 * every unit suite green, and the daemon silently degrades from being pushed to polling. This boots
 * a real server through the production constructor so that line is on the path under test.
 *
 * <p><b>How it discriminates.</b> A saga parked on an async step is resumed by a drive carrying no
 * {@code SagaCallback}, so only the registry can wake the waiting request early. Without the wiring
 * the request still answers correctly — the fallback poll sees the outcome — but not before the
 * first poll tick, which is a sixth of the wait bound. The bound here is chosen so that tick is
 * seconds away while the push is milliseconds, and the assertion sits in the gap. A correctness
 * assertion could not tell the two apart; only the timing can.
 */
class SagaSettlementNotificationIntegrationTest extends ServerIntegrationTestSupport {

  private static final String SAGA_NAME = "notify";
  private static final String SECRET = "integration-settlement-secret";

  /**
   * Six polls per window, so the fallback's first tick lands at ~10s while a push answers in well
   * under a second. The gap has to absorb a loaded CI box: this test shares a run with the rest of
   * the suite, and an earlier 20s bound put the tick at 3.3s against a 2s ceiling, which was too
   * tight and failed there while passing alone.
   */
  private static final long SYNC_BOUND_MILLIS = 60_000L;

  /** Half the fallback's first tick, so neither a slow push nor an early tick can be mistaken. */
  private static final long PUSH_CEILING_MILLIS = 5_000L;

  private final AtomicReference<String> capturedCallbackUrl = new AtomicReference<>();
  private final HttpClient http = HttpClient.newHttpClient();

  private static final String DEFINITION =
      withService(
          """
          { "name": "notify", "mode": "SAGA", "steps": [
            { "name": "charge", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge", "async": true },
              "compensation": { "method": "POST", "path": "/charge-undo" } } ] }
          """);

  @Override
  protected void configureParticipant(HttpServer participant) {
    participant.createContext(
        "/charge",
        ex -> {
          capturedCallbackUrl.set(ex.getRequestHeaders().getFirst("X-Saga-Callback-Url"));
          respond(ex, 202, "{}");
        });
    route(participant, "/charge-undo", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
  }

  @Override
  protected void configureProperties(Properties props) {
    int daemonPort = freePort();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, String.valueOf(daemonPort));
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://localhost:" + daemonPort);
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, SECRET);
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, String.valueOf(SYNC_BOUND_MILLIS));
  }

  @Test
  void syncStart_parkedSagaResumesOnThisReplica_isPushedTheOutcomeRatherThanPollingForIt()
      throws Exception {
    // Arrange — fire the participant's callback as soon as the saga is actually parked, so the
    // resume lands while the synchronous start is still waiting. Waiting for WAITING rather than
    // sleeping a fixed time: the callback is rejected outright if the saga has not parked yet, so
    // a sleep would be both a race and a slower test.
    CompletableFuture<Void> callbackFired =
        CompletableFuture.runAsync(
            () -> {
              String callbackUrl = awaitCallbackUrl();
              awaitParked(sagaIdOf(callbackUrl));
              postAbsoluteUnchecked(callbackUrl, "{\"paymentId\":\"P-1\"}");
            });

    // Act — a synchronous start. It parks almost immediately, then waits.
    long startNanos = System.nanoTime();
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    callbackFired.join();

    // Assert — the outcome, and soon enough that only the registry can explain it. Unwired, this
    // same request answers COMPLETED at the first poll tick instead, about ten seconds in.
    assertThat(start.statusCode()).isEqualTo(200);
    assertThat(status(start)).isEqualTo("COMPLETED");
    assertThat(elapsedMillis).isLessThan(PUSH_CEILING_MILLIS);
  }

  /** The callback URL the daemon handed the participant, once the async step has been called. */
  private String awaitCallbackUrl() {
    for (int attempt = 0; attempt < 600; attempt++) {
      String url = capturedCallbackUrl.get();
      if (url != null) {
        return url;
      }
      pause();
    }
    throw new IllegalStateException("participant was never called for the async step");
  }

  /** Blocks until the saga has actually parked, which is when a callback can be accepted. */
  private void awaitParked(String sagaId) {
    for (int attempt = 0; attempt < 600; attempt++) {
      try {
        if ("WAITING".equals(status(get("/sagas/" + sagaId)))) {
          return;
        }
      } catch (Exception e) {
        throw new IllegalStateException("failed reading saga " + sagaId, e);
      }
      pause();
    }
    throw new IllegalStateException("saga " + sagaId + " never parked");
  }

  private static String sagaIdOf(String callbackUrl) {
    String path = URI.create(callbackUrl).getPath();
    return path.split("/")[2];
  }

  private void postAbsoluteUnchecked(String url, String body) {
    try {
      HttpResponse<String> response =
          http.send(
              HttpRequest.newBuilder(URI.create(url))
                  .header("Content-Type", "application/json")
                  .POST(BodyPublishers.ofString(body))
                  .build(),
              BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new IllegalStateException("callback rejected: " + response.statusCode());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static void pause() {
    try {
      Thread.sleep(10L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
