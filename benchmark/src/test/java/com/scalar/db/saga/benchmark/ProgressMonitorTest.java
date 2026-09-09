package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class ProgressMonitorTest {

  @Test
  public void constructor_zeroIntervalGiven_throwsException() {
    assertThatThrownBy(
            () ->
                new ProgressMonitor(
                    () -> new ProgressMonitor.Progress(0, 0, 0, 0, 0), 0, line -> {}))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void formatLine_progressGiven_rendersCounters() {
    // Arrange
    ProgressMonitor.Progress progress = new ProgressMonitor.Progress(100, 80, 20, 1_500, 30);

    // Act
    String line = ProgressMonitor.formatLine(5_000, progress, 0);

    // Assert
    assertThat(line)
        .contains("issued=100")
        .contains("resolved=80")
        .contains("inFlight=20")
        .contains("oldestInFlight=1.5s")
        .contains("backlog=30");
    assertThat(line).doesNotContain("NO PROGRESS");
  }

  @Test
  public void formatLine_stalled_appendsWarning() {
    // Arrange
    ProgressMonitor.Progress progress = new ProgressMonitor.Progress(100, 80, 20, 65_000, 500);

    // Act
    String line = ProgressMonitor.formatLine(70_000, progress, 15_000);

    // Assert
    assertThat(line).contains("** NO PROGRESS for 15.0s **");
  }

  @Test
  public void start_running_emitsLinesUntilClosed() throws Exception {
    // Arrange
    List<String> lines = new CopyOnWriteArrayList<>();
    ProgressMonitor monitor =
        new ProgressMonitor(() -> new ProgressMonitor.Progress(1, 1, 0, 0, 0), 10, lines::add);

    // Act
    monitor.start();
    long deadline = System.nanoTime() + 2_000_000_000L;
    while (lines.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    monitor.close();

    // Assert
    assertThat(lines).isNotEmpty();
  }
}
