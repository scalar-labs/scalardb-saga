package com.scalar.db.saga.server;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * Captures what a logger emits during a test, as a try-with-resources scope:
 *
 * <pre>{@code
 * try (LogCapture logs = LogCapture.of(TlsMaterial.class)) {
 *   // act
 *   assertThat(logs.events())...;
 * }
 * }</pre>
 *
 * One shared fixture instead of a per-suite ListAppender field, {@code @AfterEach} detach, and
 * logger cast — the shape this replaces had grown several diverging copies.
 */
public final class LogCapture implements AutoCloseable {

  private final Logger logger;
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  private LogCapture(Logger logger) {
    this.logger = logger;
    appender.start();
    logger.addAppender(appender);
  }

  /** Captures the logger named after {@code loggerClass}. */
  public static LogCapture of(Class<?> loggerClass) {
    return new LogCapture((Logger) LoggerFactory.getLogger(loggerClass));
  }

  /** Captures the root logger — everything the process logs while the capture is open. */
  public static LogCapture ofRoot() {
    return new LogCapture((Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME));
  }

  /** The events captured so far, live: grows as the logger emits. */
  public List<ILoggingEvent> events() {
    return appender.list;
  }

  @Override
  public void close() {
    logger.detachAppender(appender);
  }
}
