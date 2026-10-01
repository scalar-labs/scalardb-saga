package com.scalar.db.saga.server;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import javax.net.ssl.X509ExtendedKeyManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsReloaderTest {

  // Generated once per class (keytool is a subprocess); every test copies the boot pair into its
  // own live files and rotates or corrupts those.
  @TempDir static Path materialDir;
  private static TlsTestCerts.PemPair boot;
  private static TlsTestCerts.PemPair rotated;

  @TempDir Path dir;
  private Path liveCert;
  private Path liveKey;

  @BeforeAll
  static void generateMaterial() {
    boot = TlsTestCerts.generateRsa(materialDir, "boot");
    rotated = TlsTestCerts.generateRsa(materialDir, "rotated");
  }

  @BeforeEach
  void copyBootPair() throws IOException {
    liveCert = Files.copy(boot.certChainPath(), dir.resolve("tls.crt"));
    liveKey = Files.copy(boot.privateKeyPath(), dir.resolve("tls.key"));
  }

  private TlsReloader reloader(Clock clock) {
    return new TlsReloader(liveCert, liveKey, clock, TlsMaterial.load(liveCert, liveKey));
  }

  private TlsReloader reloader() {
    return reloader(Clock.systemUTC());
  }

  /** The certificate a handshake would be served now, asked the way JSSE asks. */
  private static X509Certificate served(TlsReloader reloader) {
    X509ExtendedKeyManager keyManager = reloader.keyManager();
    return keyManager.getCertificateChain(keyManager.chooseEngineServerAlias("RSA", null, null))[0];
  }

  private void rotateTo(TlsTestCerts.PemPair pair) throws IOException {
    Files.copy(pair.certChainPath(), liveCert, REPLACE_EXISTING);
    Files.copy(pair.privateKeyPath(), liveKey, REPLACE_EXISTING);
  }

  private static List<ILoggingEvent> atLevel(LogCapture logs, Level level) {
    return logs.events().stream().filter(event -> event.getLevel() == level).toList();
  }

  @Test
  void constructor_bootMaterialGiven_keyManagerServesItsCertificate() {
    // Act
    TlsReloader reloader = reloader();

    // Assert
    assertThat(served(reloader)).isEqualTo(boot.certificate());
  }

  @Test
  void constructor_expiredCert_warnsWithDateOnly() {
    // Arrange: a clock past notAfter makes the (10-year) test certificate expired. The server
    // still starts — rotation may land a fresh file — but the warning must say so, dates only.
    Clock afterExpiry =
        Clock.fixed(
            boot.certificate().getNotAfter().toInstant().plus(Duration.ofDays(1)), ZoneOffset.UTC);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader(afterExpiry);

      // Assert
      assertThat(logs.events())
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("expired");
                assertThat(event.getFormattedMessage()).doesNotContain("BEGIN");
                assertThat(event.getFormattedMessage()).doesNotContain(dir.toString());
              });
    }
  }

  @Test
  void constructor_notYetValidCert_warnsWithDateOnly() {
    // Arrange
    Clock beforeValidity =
        Clock.fixed(
            boot.certificate().getNotBefore().toInstant().minus(Duration.ofDays(1)),
            ZoneOffset.UTC);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader(beforeValidity);

      // Assert
      assertThat(logs.events())
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("not valid until");
              });
    }
  }

  @Test
  void constructor_certWithinValidity_logsNothing() {
    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader();

      // Assert
      assertThat(logs.events()).isEmpty();
    }
  }

  @Test
  void run_filesUnchanged_keepsServingAndLogsNothing() {
    // Arrange
    TlsReloader reloader = reloader();

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();
      reloader.run();

      // Assert
      assertThat(logs.events()).isEmpty();
    }
    assertThat(served(reloader)).isEqualTo(boot.certificate());
  }

  @Test
  void run_filesRewrittenWithIdenticalMaterial_isNotARotation() throws IOException {
    // Arrange — the file changes on disk (a re-mount, or the prose openssl writes around a block)
    // but the material does not. A byte comparison would announce a rotation that never happened.
    TlsReloader reloader = reloader();
    Files.writeString(liveCert, "subject=CN=localhost\n" + Files.readString(liveCert));

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();

      // Assert
      assertThat(logs.events()).isEmpty();
    }
  }

  @Test
  void run_rotatedPairWritten_publishesItAndLogsRotation() throws IOException {
    // Arrange
    TlsReloader reloader = reloader();
    rotateTo(rotated);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();

      // Assert
      assertThat(logs.events())
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage()).contains("rotated");
                assertThat(event.getFormattedMessage()).doesNotContain(dir.toString());
              });
    }
    assertThat(served(reloader)).isEqualTo(rotated.certificate());
  }

  @Test
  void run_afterRotation_previousAliasStillAnswers() throws IOException {
    // Arrange — a JSSE handshake asks for the alias, the key and the chain in three separate
    // calls; one that straddles a rotation must get the previous generation for all three, or it
    // pairs a certificate with the wrong key. This pins the grpc-util behaviour relied on.
    TlsReloader reloader = reloader();
    X509ExtendedKeyManager keyManager = reloader.keyManager();
    String previousAlias = keyManager.chooseEngineServerAlias("RSA", null, null);
    rotateTo(rotated);

    // Act
    reloader.run();

    // Assert
    assertThat(keyManager.chooseEngineServerAlias("RSA", null, null)).isNotEqualTo(previousAlias);
    assertThat(keyManager.getCertificateChain(previousAlias)[0]).isEqualTo(boot.certificate());
    assertThat(keyManager.getPrivateKey(previousAlias)).isNotNull();
  }

  @Test
  void run_keyFromDifferentIssuanceWritten_rejectsAndKeepsServing() throws IOException {
    // Arrange — the torn rotation: a kubelet symlink flip between the two reads yields the old
    // certificate beside the new key. The pair fails the key-match check and nothing is
    // published; the next pass, with both files new, succeeds on its own.
    TlsReloader reloader = reloader();
    Files.copy(rotated.privateKeyPath(), liveKey, REPLACE_EXISTING);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();

      // Assert
      assertThat(logs.events())
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("rejected");
                assertThat(event.getFormattedMessage())
                    .contains(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
                assertThat(event.getFormattedMessage()).doesNotContain("-----");
                assertThat(event.getFormattedMessage()).doesNotContain(dir.toString());
              });
    }
    assertThat(served(reloader)).isEqualTo(boot.certificate());

    // Act — the flip completes
    Files.copy(rotated.certChainPath(), liveCert, REPLACE_EXISTING);
    reloader.run();

    // Assert
    assertThat(served(reloader)).isEqualTo(rotated.certificate());
  }

  @Test
  void run_sameRejectionRepeated_warnsOnce() throws IOException {
    // Arrange
    TlsReloader reloader = reloader();
    Files.copy(rotated.privateKeyPath(), liveKey, REPLACE_EXISTING);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();
      reloader.run();
      reloader.run();

      // Assert — one WARN for the state change; the repeats go to DEBUG
      assertThat(atLevel(logs, Level.WARN)).hasSize(1);
    }
  }

  @Test
  void run_certFileEmptied_rejectsAndKeepsServing() throws IOException {
    // Arrange — the zero-length read a resolution racing the old directory's removal can observe
    TlsReloader reloader = reloader();
    Files.write(liveCert, new byte[0]);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();

      // Assert
      assertThat(atLevel(logs, Level.WARN))
          .singleElement()
          .satisfies(
              event ->
                  assertThat(event.getFormattedMessage())
                      .contains(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY));
    }
    assertThat(served(reloader)).isEqualTo(boot.certificate());
  }

  @Test
  void run_certFileRemoved_rejectsAndKeepsServing() throws IOException {
    // Arrange
    TlsReloader reloader = reloader();
    Files.delete(liveCert);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();

      // Assert
      assertThat(atLevel(logs, Level.WARN)).hasSize(1);
    }
    assertThat(served(reloader)).isEqualTo(boot.certificate());
  }

  @Test
  void run_malformedPemWritten_rejectsAndKeepsServing() throws IOException {
    // Arrange
    TlsReloader reloader = reloader();
    Files.writeString(liveCert, "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n");

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      reloader.run();

      // Assert
      assertThat(atLevel(logs, Level.WARN)).hasSize(1);
    }
    assertThat(served(reloader)).isEqualTo(boot.certificate());
  }

  @Test
  void run_rejectionsThenValidPair_logsRecoveryWithTheirCountAndWarnsAgainLater()
      throws IOException {
    // Arrange — two rejected passes, then the rotation completes
    TlsReloader reloader = reloader();
    Files.copy(rotated.privateKeyPath(), liveKey, REPLACE_EXISTING);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      reloader.run();
      reloader.run();
      Files.copy(rotated.certChainPath(), liveCert, REPLACE_EXISTING);

      // Act
      reloader.run();

      // Assert
      assertThat(atLevel(logs, Level.INFO))
          .anySatisfy(
              event -> assertThat(event.getFormattedMessage()).contains("recovered after 2"));

      // Act — a fresh rejection after recovery is a new state change, not a repeat
      Files.copy(boot.privateKeyPath(), liveKey, REPLACE_EXISTING);
      reloader.run();

      // Assert
      assertThat(atLevel(logs, Level.WARN)).hasSize(2);
    }
    assertThat(served(reloader)).isEqualTo(rotated.certificate());
  }

  @Test
  void run_certificateAgesIntoItsLastQuarter_warnsOnceFromThePassNotEveryPass() {
    // Arrange — built with half the certificate's life left, so the constructor has nothing to
    // say; what is under test is the pass noticing the certificate ageing past the threshold
    X509Certificate leaf = boot.certificate();
    Instant notBefore = leaf.getNotBefore().toInstant();
    Instant notAfter = leaf.getNotAfter().toInstant();
    Duration lifetime = Duration.between(notBefore, notAfter);
    SteppingClock clock = new SteppingClock(notAfter.minus(lifetime.dividedBy(2)));

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      TlsReloader reloader = reloader(clock);
      reloader.run();
      assertThat(logs.events()).isEmpty();

      // Act — time passes to a fifth of the lifetime left, past any issuer's renewal point, then
      // two passes
      clock.advance(lifetime.dividedBy(2).minus(lifetime.dividedBy(5)));
      reloader.run();
      reloader.run();

      // Assert — one WARN, from the first pass past the threshold
      assertThat(atLevel(logs, Level.WARN))
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getFormattedMessage()).contains("last quarter");
                assertThat(event.getFormattedMessage()).contains(notAfter.toString());
              });
    }
  }

  @Test
  void run_certificateAgesIntoItsLastQuarterWhileRejected_warnsFromTheRejectedPass()
      throws IOException {
    // Arrange — the rotation is stuck on a key from another issuance, so every pass is rejected
    // and only the rejection path is left to notice the serving certificate ageing
    X509Certificate leaf = boot.certificate();
    Instant notBefore = leaf.getNotBefore().toInstant();
    Instant notAfter = leaf.getNotAfter().toInstant();
    Duration lifetime = Duration.between(notBefore, notAfter);
    SteppingClock clock = new SteppingClock(notAfter.minus(lifetime.dividedBy(2)));
    TlsReloader reloader = reloader(clock);
    Files.copy(rotated.privateKeyPath(), liveKey, REPLACE_EXISTING);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      reloader.run();
      assertThat(atLevel(logs, Level.WARN))
          .singleElement()
          .satisfies(event -> assertThat(event.getFormattedMessage()).contains("rejected"));

      // Act — time passes to a fifth of the lifetime left, then a pass that is rejected again
      clock.advance(lifetime.dividedBy(2).minus(lifetime.dividedBy(5)));
      reloader.run();

      // Assert — the repeat rejection stays quiet; the ageing certificate does not
      assertThat(atLevel(logs, Level.WARN))
          .hasSize(2)
          .last()
          .satisfies(event -> assertThat(event.getFormattedMessage()).contains("last quarter"));
    }
    assertThat(served(reloader)).isEqualTo(boot.certificate());
  }

  /** A clock a test can step, for a certificate that has to age between two passes. */
  private static final class SteppingClock extends Clock {
    private Instant instant;

    SteppingClock(Instant start) {
      this.instant = start;
    }

    void advance(Duration amount) {
      instant = instant.plus(amount);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
