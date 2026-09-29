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
  void run_lastQuarterOfValidity_warnsOnceNotEveryPass() {
    // Arrange — a fifth of the lifetime left: past the point any issuer would have renewed
    X509Certificate leaf = boot.certificate();
    Instant notBefore = leaf.getNotBefore().toInstant();
    Instant notAfter = leaf.getNotAfter().toInstant();
    Clock lateInLife =
        Clock.fixed(
            notAfter.minus(Duration.between(notBefore, notAfter).dividedBy(5)), ZoneOffset.UTC);

    try (LogCapture logs = LogCapture.of(TlsReloader.class)) {
      // Act
      TlsReloader reloader = reloader(lateInLife);
      reloader.run();
      reloader.run();

      // Assert
      assertThat(atLevel(logs, Level.WARN))
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getFormattedMessage()).contains("last quarter");
                assertThat(event.getFormattedMessage()).contains(notAfter.toString());
              });
    }
  }
}
