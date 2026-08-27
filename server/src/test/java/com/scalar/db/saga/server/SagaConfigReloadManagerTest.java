package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.scalar.db.saga.definition.SagaDefinition;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

class SagaConfigReloadManagerTest {

  @TempDir Path servicesDir;
  @TempDir Path definitionsDir;
  @TempDir Path secretsDir;

  private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

  private ReloadConfig reloadConfig(long intervalSeconds) {
    return new ReloadConfig(
        servicesDir,
        intervalSeconds,
        secretsDir,
        List.of(),
        Clock.fixed(Instant.parse("2026-08-22T12:00:00Z"), ZoneOffset.UTC));
  }

  /** A store that accepts everything and reports each registration as the serving version. */
  private static DefinitionStore acceptingStore() {
    return new DefinitionStore() {
      private final Map<String, SagaDefinition> serving = new HashMap<>();

      @Override
      public void register(SagaDefinition definition) {
        serving.put(definition.getName(), definition);
      }

      @Override
      public @Nullable SagaDefinition latest(String sagaName) {
        return serving.get(sagaName);
      }

      @Override
      public boolean isRegistered(String sagaName, String version) {
        SagaDefinition latest = serving.get(sagaName);
        return latest != null && version.equals(latest.getVersion());
      }
    };
  }

  private ConfigReconciler reconciler(ReloadConfig config) {
    return new ConfigReconciler(
        config, definitionsDir, false, services -> {}, acceptingStore(), names -> {});
  }

  @Test
  void start_schedulesFixedDelayPassesAtTheConfiguredInterval() {
    // Arrange
    ReloadConfig config = reloadConfig(30);
    SagaConfigReloadManager manager =
        new SagaConfigReloadManager(reconciler(config), config, scheduler);

    // Act
    manager.start();

    // Assert — first run one full interval after start: the boot pass already applied the
    // current configuration synchronously
    verify(scheduler)
        .scheduleWithFixedDelay(any(Runnable.class), eq(30L), eq(30L), eq(TimeUnit.SECONDS));
  }

  @Test
  void start_scheduledTaskRunsThePass() throws IOException {
    // Arrange — a valid candidate set the captured task should apply
    Files.writeString(servicesDir.resolve("svc.properties"), "base_url=http://svc:1\n");
    Files.writeString(
        definitionsDir.resolve("saga.json"),
        "{\"name\":\"s\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"a\",\"service\":\"svc\","
            + "\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
            + "\"compensation\":{\"method\":\"POST\",\"path\":\"/y\"}}]}");
    ReloadConfig config = reloadConfig(30);
    ConfigReconciler reconciler = reconciler(config);
    SagaConfigReloadManager manager = new SagaConfigReloadManager(reconciler, config, scheduler);
    manager.start();
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(task.capture(), anyLong(), anyLong(), any(TimeUnit.class));

    // Act
    task.getValue().run();

    // Assert — the pass ran and applied
    assertThat(reconciler.appliedDefinitionCount()).isEqualTo(1);
  }

  @Test
  void start_passThrowsError_scheduledTaskContainsIt() throws IOException {
    // Arrange — capture the periodic task and make the pass blow up with an Error. Only a catch
    // on Throwable contains it; a Throwable escaping a scheduleWithFixedDelay task cancels all
    // its future executions, silently stopping configuration reload for the rest of the process.
    // A service file makes the diff non-empty, so the pass genuinely reaches the failing seam.
    Files.writeString(servicesDir.resolve("svc.properties"), "base_url=http://svc:1\n");
    ReloadConfig config = reloadConfig(30);
    ConfigReconciler reconciler =
        new ConfigReconciler(
            config,
            definitionsDir,
            false,
            services -> {
              throw new Error("registrar blew up");
            },
            acceptingStore(),
            names -> {});
    SagaConfigReloadManager manager = new SagaConfigReloadManager(reconciler, config, scheduler);
    manager.start();
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(task.capture(), anyLong(), anyLong(), any(TimeUnit.class));

    // Act & Assert
    assertThatCode(() -> task.getValue().run()).doesNotThrowAnyException();
  }

  @Test
  void stop_shutsDownAndAwaitsTheInFlightPass() throws InterruptedException {
    // Arrange
    ReloadConfig config = reloadConfig(30);
    when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);
    when(scheduler.isTerminated()).thenReturn(true);
    SagaConfigReloadManager manager =
        new SagaConfigReloadManager(reconciler(config), config, scheduler);

    // Act
    manager.stop(System.nanoTime() + TimeUnit.SECONDS.toNanos(5));

    // Assert — graceful shutdown first (awaiting the in-flight pass), then the hard stop
    verify(scheduler).shutdown();
    verify(scheduler).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    verify(scheduler).shutdownNow();
  }

  @Test
  void stop_passStillRunningAtDeadline_warnsThatItIsInterrupted() throws InterruptedException {
    // Arrange — the wait expires with the pass still running. The drain is best effort, so the
    // shutdown proceeds; a silent interrupt would leave a failed registration unexplained.
    ReloadConfig config = reloadConfig(30);
    when(scheduler.awaitTermination(anyLong(), any())).thenReturn(false);
    when(scheduler.isTerminated()).thenReturn(false);
    SagaConfigReloadManager manager =
        new SagaConfigReloadManager(reconciler(config), config, scheduler);

    // Act & Assert
    try (LogCapture logs = LogCapture.of(SagaConfigReloadManager.class)) {
      manager.stop(System.nanoTime() + TimeUnit.SECONDS.toNanos(5));

      assertThat(logs.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("still running");
              });
    }
    verify(scheduler).shutdownNow();
  }

  @Test
  void stop_neverStarted_completesWithoutError() {
    // Arrange — the constructor-failure and start-failure cleanup paths stop a manager that was
    // never started
    ReloadConfig config = reloadConfig(30);
    when(scheduler.isTerminated()).thenReturn(true);
    SagaConfigReloadManager manager =
        new SagaConfigReloadManager(reconciler(config), config, scheduler);

    // Act & Assert
    assertThatCode(() -> manager.stop(System.nanoTime())).doesNotThrowAnyException();
  }
}
