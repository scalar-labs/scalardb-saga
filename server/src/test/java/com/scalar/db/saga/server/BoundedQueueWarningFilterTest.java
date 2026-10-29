package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Runs through the shipped {@code logback.xml}, the only Logback configuration on the test
 * classpath, so these also prove the filter is registered there.
 */
class BoundedQueueWarningFilterTest {

  @Test
  public void decide_boundedQueueWarning_isDropped() {
    try (LogCapture logs = LogCapture.of(QueuedThreadPool.class)) {
      // Act
      new QueuedThreadPool(8, 1, 60_000, new BlockingArrayQueue<>(4));

      // Assert
      assertThat(logs.events()).isEmpty();
    }
  }

  @Test
  public void decide_otherThreadPoolWarning_isLogged() {
    try (LogCapture logs = LogCapture.of(QueuedThreadPool.class)) {
      // Act
      LoggerFactory.getLogger(QueuedThreadPool.class).warn("Job failed");

      // Assert
      assertThat(logs.events()).extracting(ILoggingEvent::getMessage).containsExactly("Job failed");
    }
  }

  @Test
  public void decide_sameMessageFromOtherLogger_isLogged() {
    try (LogCapture logs = LogCapture.of(BoundedQueueWarningFilterTest.class)) {
      // Act
      LoggerFactory.getLogger(BoundedQueueWarningFilterTest.class)
          .warn("Detected thread pool queue");

      // Assert
      assertThat(logs.events()).hasSize(1);
    }
  }
}
