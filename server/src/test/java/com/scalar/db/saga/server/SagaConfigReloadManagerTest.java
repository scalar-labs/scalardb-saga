package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  private ConfigReloadPass pass(ReloadConfig config) {
    return new ConfigReloadPass(
        config, definitionsDir, false, Map.of(), () -> services -> {}, definition -> {});
  }

  @Test
  void start_schedulesFixedDelayPassesAtTheConfiguredInterval() {
    // Arrange
    ReloadConfig config = reloadConfig(30);
    SagaConfigReloadManager manager = new SagaConfigReloadManager(pass(config), config, scheduler);

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
    ConfigReloadPass pass = pass(config);
    SagaConfigReloadManager manager = new SagaConfigReloadManager(pass, config, scheduler);
    manager.start();
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(task.capture(), anyLong(), anyLong(), any(TimeUnit.class));

    // Act
    task.getValue().run();

    // Assert — the pass ran and applied
    assertThat(pass.appliedDefinitionCount()).isEqualTo(1);
  }

  @Test
  void start_passThrowsError_scheduledTaskContainsIt() throws IOException {
    // Arrange — capture the periodic task and make the pass blow up with an Error. Only a catch
    // on Throwable contains it; a Throwable escaping a scheduleWithFixedDelay task cancels all
    // its future executions, silently stopping configuration reload for the rest of the process.
    // A service file makes the diff non-empty, so the pass genuinely reaches the failing seam.
    Files.writeString(servicesDir.resolve("svc.properties"), "base_url=http://svc:1\n");
    ReloadConfig config = reloadConfig(30);
    ConfigReloadPass pass =
        new ConfigReloadPass(
            config,
            definitionsDir,
            false,
            Map.of(),
            () -> {
              throw new Error("registrar blew up");
            },
            definition -> {});
    SagaConfigReloadManager manager = new SagaConfigReloadManager(pass, config, scheduler);
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
    SagaConfigReloadManager manager = new SagaConfigReloadManager(pass(config), config, scheduler);

    // Act
    manager.stop(System.nanoTime() + TimeUnit.SECONDS.toNanos(5));

    // Assert — graceful shutdown first (awaiting the in-flight pass), then the hard stop
    verify(scheduler).shutdown();
    verify(scheduler).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    verify(scheduler).shutdownNow();
  }

  @Test
  void stop_neverStarted_completesWithoutError() {
    // Arrange — the constructor-failure and start-failure cleanup paths stop a manager that was
    // never started
    ReloadConfig config = reloadConfig(30);
    SagaConfigReloadManager manager = new SagaConfigReloadManager(pass(config), config, scheduler);

    // Act & Assert
    assertThatCode(() -> manager.stop(System.nanoTime())).doesNotThrowAnyException();
  }
}
