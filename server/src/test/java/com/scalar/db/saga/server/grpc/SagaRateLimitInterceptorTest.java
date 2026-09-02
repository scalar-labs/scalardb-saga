package com.scalar.db.saga.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Duration;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceBlockingStub;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.StartSagaRequest;
import com.scalar.db.saga.server.api.RateLimiter;
import com.scalar.db.saga.server.security.SagaAuthRequest;
import com.scalar.db.saga.server.security.SagaAuthenticationException;
import com.scalar.db.saga.server.security.SagaIdentity;
import com.scalar.db.saga.server.security.SagaRole;
import com.scalar.db.saga.server.security.SagaSecurityProvider;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SagaRateLimitInterceptor} over a real in-process gRPC transport, wired behind
 * {@link SagaSecurityInterceptor} exactly as the server wires them (auth first, then rate limit).
 * The write (saga-start) methods are throttled per authenticated principal, reads are not, and an
 * over-limit call is closed with {@code RESOURCE_EXHAUSTED}. Reaching the throttle also proves auth
 * ran first — the limiter keys on the resolved principal.
 */
class SagaRateLimitInterceptorTest {

  private static final Metadata.Key<String> ROLE_HEADER =
      Metadata.Key.of("x-test-role", Metadata.ASCII_STRING_MARSHALLER);

  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<Server> servers = new ArrayList<>();
  private @Nullable ManagedChannel channel;

  private void startServer(int limit) throws IOException {
    SagaOrchestrator orchestrator = mock(SagaOrchestrator.class);
    when(orchestrator.getStateSnapshot(any())).thenReturn(snapshot("s-1", SagaStatus.RUNNING));
    when(orchestrator.startAsync(anyString(), anyMap())).thenReturn("gen-1");
    String name = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(
                ServerInterceptors.interceptForward(
                    new SagaServiceImpl(
                        orchestrator,
                        cap -> Math.min(60_000L, cap),
                        new java.util.concurrent.CompletableFuture<>()),
                    new SagaSecurityInterceptor(new WriteProvider()),
                    new SagaRateLimitInterceptor(new RateLimiter(limit, 60_000L))))
            .build()
            .start();
    servers.add(server);
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    channels.add(channel);
  }

  @AfterEach
  void tearDown() {
    channels.forEach(ManagedChannel::shutdownNow);
    servers.forEach(Server::shutdownNow);
  }

  @Test
  void startSaga_overLimit_isResourceExhausted() throws IOException {
    // Arrange — limit 2 saga-starts per principal
    startServer(2);

    // Act / Assert — the first two starts pass, the third is throttled
    assertThat(start().getSagaId()).isEqualTo("s-1");
    assertThat(start().getSagaId()).isEqualTo("s-1");
    StatusRuntimeException error = catchThrowableOfType(StatusRuntimeException.class, this::start);
    assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    // The refusal carries the code, so the client SDK classifies it as retryable rate limiting
    // instead of falling to its unrecognized-error catch-all.
    assertThat(ErrorInfos.errorInfo(error).getReason())
        .isEqualTo(SagaErrorCode.RATE_LIMIT_EXCEEDED.code());
    // And the standard RetryInfo detail carries the advisory wait — positive, at most the
    // limiter's window (60s here) — so a machine can back off precisely instead of guessing.
    Duration delay = ErrorInfos.retryInfo(error).getRetryDelay();
    long delayMillis = delay.getSeconds() * 1000 + delay.getNanos() / 1_000_000;
    assertThat(delayMillis).isPositive().isLessThanOrEqualTo(60_000L);
  }

  @Test
  void getSaga_notLimited_evenBeyondLimit() throws IOException {
    // Arrange — a limit of 1, but reads must never be throttled
    startServer(1);

    // Act / Assert
    for (int i = 0; i < 5; i++) {
      assertThat(get().getSagaId()).isEqualTo("s-1");
    }
  }

  private SagaSnapshot start() {
    return stub()
        .startSaga(StartSagaRequest.newBuilder().setName("transfer").setAsync(true).build());
  }

  private SagaSnapshot get() {
    return stub().getSaga(GetSagaRequest.newBuilder().setSagaId("s-1").build());
  }

  private SagaServiceBlockingStub stub() {
    ManagedChannel started = Objects.requireNonNull(channel, "server not started");
    Metadata metadata = new Metadata();
    metadata.put(ROLE_HEADER, "write");
    return SagaServiceGrpc.newBlockingStub(started)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  private static SagaStateSnapshot snapshot(String sagaId, SagaStatus status) {
    Instant now = Instant.ofEpochSecond(1_700_000_000L);
    return new SagaStateSnapshot(sagaId, "transfer", status, "owner-1", "v1", now, now);
  }

  /** A stub provider mapping the {@code x-test-role=write} header to a single WRITE identity. */
  private static final class WriteProvider implements SagaSecurityProvider {
    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      if (!request.header("x-test-role").orElse("").equals("write")) {
        throw new SagaAuthenticationException("missing write role");
      }
      return SagaIdentity.of("writer", Set.of(SagaRole.WRITE));
    }

    @Override
    public String name() {
      return "write-stub";
    }
  }
}
