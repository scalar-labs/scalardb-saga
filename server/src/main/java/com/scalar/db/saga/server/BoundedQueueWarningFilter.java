package com.scalar.db.saga.server;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jspecify.annotations.Nullable;
import org.slf4j.Marker;

/**
 * Drops the WARN Jetty's {@link QueuedThreadPool} logs at construction whenever its job queue is
 * bounded. The daemon bounds it on purpose ({@code scalar.db.saga.server.http.max_queued_requests})
 * and Jetty honours the bound by rejecting work once threads and queue are full, so the line is
 * noise an operator cannot act on. Matching the message rather than raising the logger's level
 * keeps the pool's other warnings, such as a job that failed or one left unrun at stop.
 *
 * <p>Public only because {@code logback.xml} instantiates it by reflection.
 */
public final class BoundedQueueWarningFilter extends TurboFilter {

  private static final String LOGGER_NAME = QueuedThreadPool.class.getName();
  private static final String MESSAGE_PREFIX = "Detected thread pool queue";

  @Override
  public FilterReply decide(
      @Nullable Marker marker,
      Logger logger,
      Level level,
      @Nullable String format,
      Object @Nullable [] params,
      @Nullable Throwable t) {
    if (format != null
        && format.startsWith(MESSAGE_PREFIX)
        && logger.getName().equals(LOGGER_NAME)) {
      return FilterReply.DENY;
    }
    return FilterReply.NEUTRAL;
  }
}
