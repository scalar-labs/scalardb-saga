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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    /** The periodic summaries only, excluding the line that announces a storm's onset. */
    private List<String> intervalSummaries() {
      return summaries().stream().filter(line -> line.contains("rejected in the last")).toList();
    }

    private List<String> summaries() {
      return appender.list.stream()
          .filter(event -> event.getLevel() == Level.WARN)
          .map(ILoggingEvent::getFormattedMessage)
          .toList();
    }

    /**
     * Keeps a storm running across an interval boundary: a refusal partway through so the storm
     * never looks quiet, then one past the boundary, which is the one that reports. Spacing
     * refusals a whole interval apart instead would make each a fresh storm, and each would
     * announce its own onset rather than summarizing.
     */
    private void refuseAcrossAnIntervalBoundary(AdmissionController controller) {
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS / 2);
      controller.acquire();
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS / 2 + 1);
      controller.acquire();
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
    void acquire_firstRejectionAfterAQuietSpell_saysSoAtOnce() {
      // Without this the cap can refuse work for a whole interval in silence: the per-refusal lines
      // are DEBUG and the image logs at INFO, so a burst shorter than the summary period leaves the
      // operator with nothing but their callers' 503s.
      // Arrange
      AdmissionController controller = saturated(1);

      // Act
      controller.acquire();

      // Assert
      assertThat(summaries())
          .singleElement()
          .asString()
          .contains("reached: starts are being refused");
    }

    @Test
    void acquire_furtherRejectionsWithinTheInterval_addNothing() {
      // The storm is the moment a daemon can least afford a line per event, so after the onset the
      // interval summary is the only thing that speaks.
      // Arrange
      AdmissionController controller = saturated(1);
      controller.acquire();

      // Act
      for (int i = 0; i < 100; i++) {
        controller.acquire();
      }

      // Assert
      assertThat(summaries()).hasSize(1);
    }

    @Test
    void acquire_stormResumingAfterAQuietSpell_announcesItselfAgain() {
      // A second burst an hour later is news again, not a continuation.
      // Arrange
      AdmissionController controller = saturated(1);
      controller.acquire();

      // Act — quiet for longer than an interval, then refusals resume.
      advance(AdmissionController.SUMMARY_INTERVAL_NANOS * 2);
      controller.acquire();

      // Assert
      assertThat(summaries()).hasSize(2);
      assertThat(summaries().get(1)).contains("reached: starts are being refused");
    }

    @Test
    void acquire_rejectionsSpanningAnInterval_logsOneSummaryNamingTheCount() {
      // Arrange — the onset line claims the first refusal, so the interval reports the rest.
      AdmissionController controller = saturated(1);
      for (int i = 0; i < 5; i++) {
        controller.acquire();
      }

      // Act — the refusal that crosses the boundary is the one that reports.
      refuseAcrossAnIntervalBoundary(controller);

      // Assert
      assertThat(intervalSummaries())
          .singleElement()
          .asString()
          .contains("6 start(s) rejected")
          .contains("7 total since start");
    }

    @Test
    void acquire_secondInterval_reportsOnlyItsOwnDelta() {
      // Counters are cumulative, so each interval subtracts what was already reported; repeating
      // the running total would make a quiet interval look like a storm.
      // Arrange — one continuous storm, so the periodic summary is what speaks after the onset.
      AdmissionController controller = saturated(1);
      controller.acquire();
      refuseAcrossAnIntervalBoundary(controller);

      // Act
      refuseAcrossAnIntervalBoundary(controller);

      // Assert
      assertThat(intervalSummaries()).hasSize(2);
      assertThat(intervalSummaries().get(1))
          .contains("2 start(s) rejected")
          .contains("5 total since start");
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
    void acquire_stormSpanningTwoIntervals_reportsAPositiveDeltaEachWithGrowingTotals() {
      // One storm, two boundaries, one line each: the delta an operator reads is what arrived in
      // that interval, and the cumulative figure beside it only ever climbs. This says nothing
      // about a writer overtaken by a later interval; that guard is driven directly in
      // claimDelta_totalLowerThanAlreadyReportedGiven_leavesTheMarkWhereItWas.
      // Arrange
      AdmissionController controller = saturated(1);

      // Act — one continuous storm crossing two interval boundaries.
      controller.acquire();
      refuseAcrossAnIntervalBoundary(controller);
      refuseAcrossAnIntervalBoundary(controller);

      // Assert — every reported delta is positive, and the running totals only ever grow.
      assertThat(intervalSummaries()).hasSize(2);
      assertThat(intervalSummaries()).noneMatch(line -> line.contains("-"));
      assertThat(totalIn(intervalSummaries().get(0)))
          .isLessThan(totalIn(intervalSummaries().get(1)));
    }

    @Test
    void claimDelta_totalLowerThanAlreadyReportedGiven_leavesTheMarkWhereItWas() {
      // The one case threads cannot stage: a writer that read its total, lost the processor for a
      // whole interval, and returned after a successor reported more. Nothing can hold a thread
      // between reading the total and claiming it, so the claim is driven directly. Were the mark
      // to follow the late writer down, the interval after would count 10 through 20 a second
      // time.
      // Arrange
      AdmissionController controller = controller(1);
      controller.claimDelta(20);

      // Act
      long overtaken = controller.claimDelta(10);

      // Assert
      assertThat(overtaken).isZero();
      assertThat(controller.claimDelta(25)).isEqualTo(5);
    }

    /** The "N total since start" figure from a summary line. */
    private long totalIn(String summary) {
      // Matched rather than sliced: the line's own "start(s)" would capture the wrong bracket.
      Matcher matcher = Pattern.compile("\\((\\d+) total since start").matcher(summary);
      assertThat(matcher.find()).isTrue();
      return Long.parseLong(matcher.group(1));
    }

    @Test
    void acquire_summaryLine_namesTheCapAndTheFreePermits() {
      // The line has to stand on its own in a log an operator greps at 3am.
      // Arrange
      AdmissionController controller = saturated(2);
      controller.acquire();

      // Act
      refuseAcrossAnIntervalBoundary(controller);

      // Assert
      assertThat(intervalSummaries())
          .singleElement()
          .asString()
          .contains("Admission cap 2")
          .contains("0 permits free");
    }
  }
}
