package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins that shutting the server down answers the requests already in flight instead of dropping
 * them.
 *
 * <p>Handlers run on virtual threads, which Jetty's thread pool neither owns nor joins, so stopping
 * the pool contributes nothing to a drain. Without a stop timeout, {@code stop()} returns in
 * milliseconds and an in-flight request's socket dies under it: the caller gets a closed channel
 * rather than a response, and the handler keeps running against a store {@code close()} is about to
 * close. That is what {@code todos/080} recorded, measured. The fix is one {@code setStopTimeout},
 * and losing it would be invisible — every request still served, every probe still green, and only
 * a shutdown under load would show it.
 */
class HttpDrainTest {

  private static String declarativeJson(String name) {
    return "{\"name\":\""
        + name
        + "\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\",\"service\":\"svc\","
        + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
        + "\"compensation\":{\"method\":\"POST\",\"path\":\"/y\"}}]}";
  }

  @Test
  @Timeout(60)
  void close_requestInFlight_answersItAndWaitsForTheHandlerBeforeClosingTheStore(@TempDir Path dir)
      throws Exception {
    // Arrange — a handler that is slow for a reason shutdown cannot short-circuit: it is inside a
    // store read, not a bounded wait. (A bounded wait is deliberately woken by shutdown, so it
    // would no longer exercise the drain.) The async start route calls getStateSnapshot after
    // dispatching, so blocking that read parks the handler mid-request.
    Files.writeString(dir.resolve("saga.json"), declarativeJson("saga"));
    Properties props = new Properties();
    props.setProperty(SagaServerConfig.HOST_KEY, "127.0.0.1");
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, "0");
    props.setProperty(SagaServerConfig.GRPC_ENABLED_KEY, "false");
    props.setProperty(SagaServerConfig.DEFINITIONS_PATH_KEY, dir.toString());
    Path services = Files.createDirectories(dir.resolve("services"));
    Files.writeString(services.resolve("svc.properties"), "base_url=http://127.0.0.1:1\n");
    props.setProperty(SagaServerConfig.SERVICES_PATH_KEY, services.toString());

    CountDownLatch entered = new CountDownLatch(1);
    AtomicLong handlerFinishedAt = new AtomicLong();
    AtomicLong storeClosedAt = new AtomicLong();
    SagaStateSnapshot running =
        new SagaStateSnapshot(
            "s1", "saga", SagaStatus.RUNNING, "owner", "v1", Instant.EPOCH, Instant.EPOCH);

    DefaultSagaOrchestrator orchestrator = mock(DefaultSagaOrchestrator.class);
    lenient().when(orchestrator.httpEndpointRegistrar()).thenReturn(endpoints -> {});
    when(orchestrator.startAsync(eq("saga"), anyMap())).thenReturn("s1");
    when(orchestrator.getStateSnapshot("s1"))
        .thenAnswer(
            invocation -> {
              entered.countDown();
              Thread.sleep(2_000);
              handlerFinishedAt.set(System.nanoTime());
              return running;
            });
    // close() on a mock is a no-op, so record when it happens; that is the ordering the fix exists
    // to guarantee — todos/080's bug was a handler calling a store close() had already shut.
    doAnswer(
            invocation -> {
              storeClosedAt.set(System.nanoTime());
              return null;
            })
        .when(orchestrator)
        .close();

    SagaServer server = new SagaServer(SagaServerConfig.load(props), orchestrator).start();

    // Act
    CompletableFuture<HttpResponse<String>> inFlight =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()
            .sendAsync(
                HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + server.port() + "/sagas?async=true"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString("{\"sagaName\":\"saga\"}"))
                    .build(),
                BodyHandlers.ofString());
    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
    server.close();

    // Assert — the caller got its answer instead of a dropped connection, and the handler had
    // finished before the store was closed underneath it.
    HttpResponse<String> response = inFlight.get(30, TimeUnit.SECONDS);
    assertThat(response.statusCode()).isEqualTo(202);
    assertThat(handlerFinishedAt.get()).isNotZero();
    assertThat(storeClosedAt.get()).isGreaterThan(handlerFinishedAt.get());
  }
}
