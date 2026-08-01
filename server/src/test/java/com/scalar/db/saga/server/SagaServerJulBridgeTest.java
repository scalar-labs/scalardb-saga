package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.read.ListAppender;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Covers the path that keeps gRPC's {@code java.util.logging} output inside the daemon's Logback
 * configuration: {@link SagaServer#installJulToSlf4jBridge()}, which routes the records, {@link
 * SagaServer#main}, which is the sole caller and therefore the only reason the routing happens at
 * all, and the {@code LevelChangePropagator} in the shipped {@code logback.xml}, which is what
 * makes routing them affordable.
 *
 * <p>Lives in its own class because it mutates JVM-wide state; the root JUL logger's handlers and
 * level, the JUL test loggers' levels, and the root Logback logger's level and appenders. All are
 * captured and restored per test so nothing leaks into the rest of the suite.
 */
class SagaServerJulBridgeTest {

  private ch.qos.logback.classic.Logger rootLogger;
  private ListAppender<ILoggingEvent> appender;
  private List<Handler> originalJulHandlers;
  private java.util.logging.Level originalJulRootLevel;
  private Level originalLogbackRootLevel;
  private @Nullable LoggerContext propagationContext;

  // Fields, not locals: SpotBugs flags LG_LOST_LOGGER_DUE_TO_WEAK_REFERENCE because LogManager
  // holds Loggers weakly, so a level set on a local can be lost to garbage collection before the
  // logging calls run.
  private java.util.logging.Logger levelTestLogger;
  private java.util.logging.Logger propagationTestLogger;

  @BeforeEach
  void setUp() {
    java.util.logging.Logger julRoot = LogManager.getLogManager().getLogger("");
    originalJulHandlers = Arrays.asList(julRoot.getHandlers().clone());
    originalJulRootLevel = julRoot.getLevel();
    rootLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    originalLogbackRootLevel = rootLogger.getLevel();
    // The assertions read events off the appender below, so the root level is a live input to them.
    // Pinned rather than inherited, so an exported SCALAR_DB_SAGA_LOG_LEVEL cannot filter the
    // records out from under the tests.
    rootLogger.setLevel(Level.TRACE);
    appender = new ListAppender<>();
    appender.start();
    rootLogger.addAppender(appender);
    levelTestLogger = java.util.logging.Logger.getLogger("io.grpc.test.levels");
    propagationTestLogger = java.util.logging.Logger.getLogger("io.grpc.test.propagation");
  }

  @AfterEach
  void tearDown() {
    if (propagationContext != null) {
      propagationContext.stop();
      propagationContext = null;
    }
    rootLogger.setLevel(originalLogbackRootLevel);
    rootLogger.detachAppender(appender);
    appender.stop();
    SLF4JBridgeHandler.uninstall();
    java.util.logging.Logger julRoot = LogManager.getLogManager().getLogger("");
    for (Handler handler : julRoot.getHandlers()) {
      julRoot.removeHandler(handler);
    }
    originalJulHandlers.forEach(julRoot::addHandler);
    // Configuring the shipped logback.xml propagates the Logback root level onto the JUL root
    // logger, overwriting whatever the JVM started with.
    if (originalJulRootLevel != null) {
      julRoot.setLevel(originalJulRootLevel);
    }
    levelTestLogger.setLevel(null);
    propagationTestLogger.setLevel(null);
  }

  @Test
  public void installJulToSlf4jBridge_julLoggerUsed_routesTheRecordToSlf4j() {
    // Arrange
    SagaServer.installJulToSlf4jBridge();

    // Act — how gRPC emits its logs: java.util.logging, not SLF4J.
    java.util.logging.Logger.getLogger("io.grpc.netty.NettyServerTransport")
        .warning("transport failed");

    // Assert
    assertThat(appender.list)
        .extracting(ILoggingEvent::getLoggerName, ILoggingEvent::getFormattedMessage)
        .containsExactly(tuple("io.grpc.netty.NettyServerTransport", "transport failed"));
  }

  @Test
  public void installJulToSlf4jBridge_julLoggerUsed_mapsJulLevelsOntoSlf4jLevels() {
    // Arrange
    SagaServer.installJulToSlf4jBridge();
    levelTestLogger.setLevel(java.util.logging.Level.ALL);

    // Act
    levelTestLogger.severe("severe");
    levelTestLogger.warning("warning");
    levelTestLogger.info("info");

    // Assert — a JUL SEVERE must not arrive as anything less than an SLF4J ERROR, or an operator
    // filtering on ERROR would miss gRPC failures entirely.
    assertThat(appender.list)
        .extracting(ILoggingEvent::getLevel)
        .containsExactly(Level.ERROR, Level.WARN, Level.INFO);
  }

  @Test
  public void installJulToSlf4jBridge_defaultConsoleHandlerPresent_removesItToAvoidDoubleLogging() {
    // Arrange — a handler standing in for JUL's default ConsoleHandler.
    java.util.logging.Logger julRoot = LogManager.getLogManager().getLogger("");
    julRoot.addHandler(new java.util.logging.ConsoleHandler());

    // Act
    SagaServer.installJulToSlf4jBridge();

    // Assert — only the bridge is left, so a record is not also printed in JUL's own format.
    assertThat(julRoot.getHandlers()).hasSize(1);
    assertThat(julRoot.getHandlers()[0]).isInstanceOf(SLF4JBridgeHandler.class);
    assertThat(SLF4JBridgeHandler.isInstalled()).isTrue();
  }

  @Test
  public void installJulToSlf4jBridge_calledTwice_installsOnlyOneBridge() {
    // Arrange — main() runs once, but a double install would double every gRPC log line.
    SagaServer.installJulToSlf4jBridge();
    SagaServer.installJulToSlf4jBridge();

    // Act
    java.util.logging.Logger.getLogger("io.grpc.test.idempotence").warning("once");

    // Assert
    assertThat(appender.list).hasSize(1);
  }

  @Test
  public void main_noArgumentsGiven_installsTheBridgeBeforeFailing() {
    // Arrange — a handler standing in for JUL's default ConsoleHandler. Without one already on the
    // root logger, an assertion that the bridge is installed could pass on a JVM that simply had no
    // handlers to begin with, rather than because main removed them and installed the bridge.
    java.util.logging.Logger julRoot = LogManager.getLogManager().getLogger("");
    julRoot.addHandler(new java.util.logging.ConsoleHandler());

    // Act — main installs the bridge before it validates its arguments, so the usage error reaches
    // the wiring without a properties file or a started server. That ordering is the thing being
    // pinned: validating first would leave this failing, and would also mean the entry point can
    // exit before the daemon's logging is configured.
    assertThatThrownBy(() -> SagaServer.main(new String[0]))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert — the entry point installs the handler, not merely the method it is supposed to call.
    // Every other test here calls installJulToSlf4jBridge() itself, so deleting the call from main
    // would restore unbridged gRPC logging with the whole class still green.
    assertThat(julRoot.getHandlers()).hasSize(1);
    assertThat(julRoot.getHandlers()[0]).isInstanceOf(SLF4JBridgeHandler.class);
    assertThat(SLF4JBridgeHandler.isInstalled()).isTrue();
  }

  @Test
  public void logbackConfiguration_withRootLevelRaised_stopsJulFromBuildingTheRecord()
      throws JoranException {
    // Arrange
    propagationContext = configureFromShippedLogbackXml();

    // Act
    propagationContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.WARN);

    // Assert — this is what keeps the bridge affordable. JUL declines the call itself, so no
    // LogRecord is built for a line Logback would only discard. Drop the LevelChangePropagator
    // from logback.xml and JUL keeps its own INFO default, paying that cost on every disabled
    // statement gRPC makes.
    assertThat(propagationTestLogger.isLoggable(java.util.logging.Level.INFO)).isFalse();
  }

  @Test
  public void logbackConfiguration_withRootLevelLowered_enablesTheMatchingJulLevel()
      throws JoranException {
    // Arrange
    propagationContext = configureFromShippedLogbackXml();

    // Act — an operator turning verbosity up through SCALAR_DB_SAGA_LOG_LEVEL.
    propagationContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.DEBUG);

    // Assert — DEBUG has to arrive as JUL FINE, or gRPC detail stays unreachable however the
    // daemon is configured.
    assertThat(propagationTestLogger.isLoggable(java.util.logging.Level.FINE)).isTrue();
  }

  /**
   * Configures a private {@link LoggerContext} from the daemon's shipped {@code logback.xml}.
   *
   * <p>Private rather than the suite's own context so these tests do not disturb its appenders, and
   * loaded explicitly by URL so the assertions stay pinned to the shipped file even if a {@code
   * logback-test.xml} is later added to this source set. Either way the {@code
   * LevelChangePropagator} it installs writes through to the JVM-wide {@code LogManager}, which is
   * the state these tests observe.
   */
  private LoggerContext configureFromShippedLogbackXml() throws JoranException {
    URL shipped =
        Objects.requireNonNull(
            SagaServer.class.getResource("/logback.xml"),
            "the daemon's shipped logback.xml is not on the test classpath");
    LoggerContext context = new LoggerContext();
    context.setName("shipped-logback-under-test");
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    configurator.doConfigure(shipped);
    return context;
  }
}
