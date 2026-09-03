package com.scalar.db.saga.engine;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

class AdmissionControllerTest {

  /** A clock the test moves by hand, so interval behavior is exercised without sleeping. */
  private final AtomicLong nanos = new AtomicLong(1_000_000L);

  private AdmissionController controller(int cap) {
    return new AdmissionController(cap, nanos::get);
  }

  private void advance(long amount) {
    nanos.addAndGet(amount);
  }

  @Nested
  class Permits {

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void constructor_capNotPositiveGiven_throwsIllegalArgumentException(int cap) {
      // Off is expressed by having no controller at all, not by a controller with a zero cap: a
      // zero-cap semaphore would refuse every start.
      assertThatThrownBy(() -> new AdmissionController(cap))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acquire_belowTheCap_returnsALease() {
      // Act
      AdmissionController.PermitLease lease = controller(2).acquire();

      // Assert
      assertThat(lease).isNotNull();
    }

    @Test
    void acquire_atTheCap_returnsNull() {
      // Arrange
      AdmissionController controller = controller(1);
      controller.acquire();

      // Act & Assert
      assertThat(controller.acquire()).isNull();
    }

    @Test
    void acquire_afterTheLeaseIsReleased_admitsAgain() {
      // Arrange
      AdmissionController controller = controller(1);
      AdmissionController.PermitLease lease = controller.acquire();
      assertThat(controller.acquire()).isNull();

      // Act
      requireNonNull(lease).release();

      // Assert
      assertThat(controller.acquire()).isNotNull();
      assertThat(controller.availablePermits()).isZero();
    }

    @Test
    void release_calledTwice_returnsOnlyOnePermit() {
      // A permit returned twice raises the cap silently and permanently, which is worse than
      // leaking one: the daemon would admit more than the operator allowed and never say so.
      // Arrange
      AdmissionController controller = controller(2);
      AdmissionController.PermitLease lease = requireNonNull(controller.acquire());

      // Act
      lease.release();
      lease.release();

      // Assert — strict equality: a symmetric leak and over-release would cancel out under a
      // "greater than" assertion.
      assertThat(controller.availablePermits()).isEqualTo(2);
    }

    @Test
    void release_everyLeaseReleased_restoresTheFullCap() {
      // Arrange
      AdmissionController controller = controller(3);
      AdmissionController.PermitLease first = requireNonNull(controller.acquire());
      AdmissionController.PermitLease second = requireNonNull(controller.acquire());

      // Act
      first.release();
      second.release();

      // Assert
      assertThat(controller.availablePermits()).isEqualTo(3);
    }

    @Test
    void maxConcurrent_returnsTheConfiguredCap() {
      assertThat(controller(7).maxConcurrent()).isEqualTo(7);
    }
  }

  @Nested
  class RejectionSummary {

    private Logger controllerLogger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void attachLogCapture() {
      controllerLogger = (Logger) LoggerFactory.getLogger(AdmissionController.class);
      appender = new ListAppender<>();
      appender.start();
      controllerLogger.addAppender(appender);
    }

    @AfterEach
    void detachLogCapture() {
      controllerLogger.detachAppender(appender);
      appender.stop();
    }

    private List<String> summaries() {
      return appender.list.stream()
          .filter(event -> event.getLevel() == Level.WARN)
          .map(ILoggingEvent::getFormattedMessage)
          .toList();
    }

    /** Fills the cap so every later acquire is refused. */
    private AdmissionController saturated(int cap) {
      AdmissionController controller = controller(cap);
      for (int i = 0; i < cap; i++) {
        controller.acquire();
      }
      return controller;
    }

    @Test
    void acquire_rejectionsWithinOneInterval_logsNothing() {
      // The storm is the moment a daemon can least afford a line per event.
      // Arrange
      AdmissionController controller = saturated(1);

      // Act
      for (int i = 0; i < 100; i++) {
        controller.acquire();
      }

      // Assert
      assertThat(summaries()).isEmpty();
    }

    @Test
    void acquire_rejectionsSpanningAnInterval_logsOneSummaryNamingTheCount() {
      // Arrange
      AdmissionController controller = saturated(1);
      for (int i = 0; i < 5; i++) {
        controller.acquire();
      }

      // Act — the rejection that crosses the boundary is the one that reports.
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS);
      controller.acquire();

      // Assert
      assertThat(summaries())
          .singleElement()
          .asString()
          .contains("6 start(s) rejected")
          .contains("6 total since start");
    }

    @Test
    void acquire_secondInterval_reportsOnlyItsOwnDelta() {
      // Counters are cumulative, so each interval has to subtract what was already reported;
      // reporting the running total every time would make a quiet interval look like a storm.
      // Arrange
      AdmissionController controller = saturated(1);
      controller.acquire();
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS);
      controller.acquire();

      // Act
      controller.acquire();
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS);
      controller.acquire();

      // Assert
      assertThat(summaries()).hasSize(2);
      assertThat(summaries().get(1))
          .contains("2 start(s) rejected")
          .contains("4 total since start");
    }

    @Test
    void acquire_manyThreadsRejectedAcrossOneBoundary_logsExactlyOneSummary()
        throws InterruptedException {
      // The election has to hold under the only conditions it matters in: every rejector arriving
      // at once, just after the interval expired.
      // Arrange
      AdmissionController controller = saturated(1);
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS);
      int threads = 64;
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);

      // Act
      try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
        for (int i = 0; i < threads; i++) {
          pool.execute(
              () -> {
                try {
                  start.await();
                  controller.acquire();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  done.countDown();
                }
              });
        }
        start.countDown();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
      }

      // Assert
      assertThat(summaries()).hasSize(1);
    }

    @Test
    void acquire_summaryLine_namesTheCapAndTheFreePermits() {
      // The line has to stand on its own in a log an operator greps at 3am.
      // Arrange
      AdmissionController controller = saturated(2);

      // Act
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS);
      controller.acquire();

      // Assert
      assertThat(summaries())
          .singleElement()
          .asString()
          .contains("Admission cap 2")
          .contains("0 permits free");
    }
  }
}
