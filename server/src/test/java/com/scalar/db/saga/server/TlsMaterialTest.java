package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import ch.qos.logback.classic.Level;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsMaterialTest {

  // Generated once per class: keytool is a subprocess, and every test derives its inputs from
  // these three pairs (or writes its own malformed files) rather than generating fresh ones.
  @TempDir static Path materialDir;

  private static TlsTestCerts.PemPair rsa;
  private static TlsTestCerts.PemPair otherRsa;
  private static TlsTestCerts.PemPair ec;

  @TempDir Path dir;

  @BeforeAll
  static void generateMaterial() {
    rsa = TlsTestCerts.generateRsa(materialDir, "rsa");
    otherRsa = TlsTestCerts.generateRsa(materialDir, "other-rsa");
    ec = TlsTestCerts.generateEc(materialDir, "ec");
  }

  @Test
  void load_validRsaPemGiven_parsesChainAndKey() {
    // Act
    TlsMaterial material =
        TlsMaterial.load(rsa.certChainPath(), rsa.privateKeyPath(), Clock.systemUTC());

    // Assert
    assertThat(material.certChain()).containsExactly(rsa.certificate());
    assertThat(material.privateKey().getAlgorithm()).isEqualTo("RSA");
  }

  @Test
  void load_validEcPemGiven_parsesKey() {
    TlsMaterial material =
        TlsMaterial.load(ec.certChainPath(), ec.privateKeyPath(), Clock.systemUTC());

    assertThat(material.certChain()).containsExactly(ec.certificate());
    assertThat(material.privateKey().getAlgorithm()).isEqualTo("EC");
  }

  @Test
  void load_chainWithMultipleCertsGiven_preservesFileOrder() throws IOException {
    // Arrange: the parser does not validate issuer linkage, so a second unrelated certificate
    // stands in for an intermediate; the leaf (first block) is what the key must match.
    Path chain = dir.resolve("chain.crt");
    Files.writeString(
        chain, Files.readString(rsa.certChainPath()) + Files.readString(otherRsa.certChainPath()));

    // Act
    TlsMaterial material = TlsMaterial.load(chain, rsa.privateKeyPath(), Clock.systemUTC());

    // Assert
    assertThat(material.certChain()).containsExactly(rsa.certificate(), otherRsa.certificate());
  }

  @Test
  void load_proseBetweenCertBlocksGiven_stillParses() throws IOException {
    // openssl prepends subject=/issuer= comment lines between blocks; block extraction must not
    // trip over them.
    Path chain = dir.resolve("prose.crt");
    Files.writeString(
        chain,
        "subject=CN=localhost\nissuer=CN=localhost\n" + Files.readString(rsa.certChainPath()));

    TlsMaterial material = TlsMaterial.load(chain, rsa.privateKeyPath(), Clock.systemUTC());

    assertThat(material.certChain()).containsExactly(rsa.certificate());
  }

  @Test
  void load_certFileMissing_throwsNamingKeyButNotPathValue() {
    // The key, never the configured value: a path value can be a mis-pasted secret (a ${file:...}
    // reference on a path key resolves to the referenced content), so no message may echo it.
    Path missing = dir.resolve("nope.crt");

    assertThatThrownBy(() -> TlsMaterial.load(missing, rsa.privateKeyPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining("does not exist")
        .hasMessageNotContaining("nope.crt")
        .hasNoCause();
  }

  @Test
  void load_keyFileMissing_throwsNamingKeyButNotPathValue() {
    Path missing = dir.resolve("nope.key");

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), missing, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("does not exist")
        .hasMessageNotContaining("nope.key")
        .hasNoCause();
  }

  @Test
  void load_keyFileUnreadable_throwsPermissionsHintDistinctFromMissing() throws IOException {
    // POSIX permissions are the mechanism under test, and root reads 0000 files regardless — on
    // either environment the scenario cannot be arranged, so skip rather than fail or pass-wrong.
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    assumeTrue(!"root".equals(System.getProperty("user.name")));
    // The #1 real-world trip: a root-owned Secret mounted 0600 exists but is invisible to the
    // server's non-root uid. "Does not exist" would send the operator hunting a present mount.
    Path unreadable = dir.resolve("unreadable.key");
    Files.copy(rsa.privateKeyPath(), unreadable);
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), unreadable, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("not readable")
        .hasMessageContaining("permissions")
        .hasNoCause();
  }

  @Test
  void load_binaryDerCertGiven_throwsPemConversionHint() throws Exception {
    Path der = dir.resolve("cert.der");
    Files.write(der, rsa.certificate().getEncoded());

    assertThatThrownBy(() -> TlsMaterial.load(der, rsa.privateKeyPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining("DER")
        .hasNoCause();
  }

  @Test
  void load_certFileWithoutCertificateBlock_throws() throws IOException {
    Path notPem = dir.resolve("not-pem.crt");
    Files.writeString(notPem, "hello, this is not a certificate\n");

    assertThatThrownBy(() -> TlsMaterial.load(notPem, rsa.privateKeyPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining("no CERTIFICATE block")
        .hasNoCause();
  }

  @Test
  void load_keyMaterialInCertPath_throwsSwappedPathsHint() {
    assertThatThrownBy(
            () -> TlsMaterial.load(rsa.privateKeyPath(), rsa.privateKeyPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining("swapped")
        .hasNoCause();
  }

  @Test
  void load_certificateInKeyPath_throwsSwappedPathsHint() {
    assertThatThrownBy(
            () -> TlsMaterial.load(rsa.certChainPath(), rsa.certChainPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("swapped")
        .hasNoCause();
  }

  @Test
  void load_corruptCertBlock_throwsWithoutEchoingContent() throws IOException {
    // Valid base64, not a certificate. The message must not leak the block's contents.
    Path corrupt = dir.resolve("corrupt.crt");
    Files.writeString(corrupt, "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n");

    assertThatThrownBy(() -> TlsMaterial.load(corrupt, rsa.privateKeyPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining("does not parse")
        .hasMessageNotContaining("AAAA")
        .hasNoCause();
  }

  @Test
  void load_pkcs1KeyGiven_throwsWithIssuerSideGuidance() throws IOException {
    // The parser rejects by label before decoding, so the body can be dummy base64. PKCS#1 is
    // what cert-manager and Vault emit by default, so the message carries their one-line fixes.
    Path pkcs1 = dir.resolve("pkcs1.key");
    Files.writeString(
        pkcs1, "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n");

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), pkcs1, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("PKCS#1")
        .hasMessageContaining("cert-manager")
        .hasMessageContaining("openssl pkcs8 -topk8 -nocrypt")
        .hasNoCause();
  }

  @Test
  void load_sec1EcKeyGiven_throwsWithConversionGuidance() throws IOException {
    Path sec1 = dir.resolve("sec1.key");
    Files.writeString(sec1, "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n");

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), sec1, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("SEC1")
        .hasMessageContaining("openssl pkcs8 -topk8 -nocrypt")
        .hasNoCause();
  }

  @Test
  void load_encryptedKeyGiven_throwsUnencryptedOnlyGuidance() throws IOException {
    Path encrypted = dir.resolve("encrypted.key");
    Files.writeString(
        encrypted,
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n");

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), encrypted, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("encrypted")
        .hasMessageContaining("unencrypted PKCS#8")
        .hasNoCause();
  }

  @Test
  void load_ed25519KeyGiven_throwsUnsupportedAlgorithm() throws Exception {
    // Valid PKCS#8, unsupported algorithm: Netty's own parser (no BC) tries only RSA/DSA/EC, so
    // accepting Ed25519 here would pass validation and then fail inside the gRPC stack.
    KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
    Path ed25519 = dir.resolve("ed25519.key");
    Files.writeString(
        ed25519,
        TlsTestCerts.pem("PRIVATE KEY", generator.generateKeyPair().getPrivate().getEncoded()));

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), ed25519, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("neither RSA nor EC")
        .hasNoCause();
  }

  @Test
  void load_corruptPkcs8DerGiven_throwsWithoutBlamingAlgorithmChoiceAlone() throws IOException {
    // Valid base64 under the PKCS#8 label, but not valid DER: the same catch-all fires as for an
    // unsupported algorithm, and the message must own both readings — an operator with a corrupt
    // file must not be sent off to change algorithms.
    Path corrupt = dir.resolve("corrupt-pkcs8.key");
    Files.writeString(corrupt, "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n");

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), corrupt, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("corrupt")
        .hasNoCause();
  }

  @Test
  void load_rfc1421EncryptedLegacyKeyGiven_throwsEncryptedGuidanceNotGenericNoBlock()
      throws IOException {
    // A PKCS#1 key encrypted the RFC 1421 way carries Proc-Type/DEK-Info headers inside the
    // block, so the base64-only block pattern rightly refuses to match it — but the operator
    // deserves the encrypted-key guidance, not a generic "no PRIVATE KEY block".
    Path encrypted = dir.resolve("rfc1421.key");
    Files.writeString(
        encrypted,
        """
        -----BEGIN RSA PRIVATE KEY-----
        Proc-Type: 4,ENCRYPTED
        DEK-Info: AES-128-CBC,0123456789ABCDEF0123456789ABCDEF

        AAAA
        -----END RSA PRIVATE KEY-----
        """);

    assertThatThrownBy(() -> TlsMaterial.load(rsa.certChainPath(), encrypted, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("encrypted legacy")
        .hasMessageContaining("openssl pkcs8 -topk8 -nocrypt")
        .hasNoCause();
  }

  @Test
  void load_keyFromDifferentIssuanceGiven_throwsMismatchNamingBothPaths() {
    // Same algorithm, wrong pair — the renewed-cert-with-stale-key case, which neither Jetty nor
    // Netty catches at startup.
    assertThatThrownBy(
            () ->
                TlsMaterial.load(rsa.certChainPath(), otherRsa.privateKeyPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY)
        .hasMessageContaining(SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY)
        .hasMessageContaining("does not match")
        .hasNoCause();
  }

  @Test
  void load_ecKeyWithRsaCertGiven_throwsMismatch() {
    assertThatThrownBy(
            () -> TlsMaterial.load(rsa.certChainPath(), ec.privateKeyPath(), Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not match")
        .hasNoCause();
  }

  @Test
  void load_expiredCert_warnsWithDateOnly() {
    // Arrange: a clock past notAfter makes the (10-year) test certificate expired. The server
    // still starts — rotation may land a fresh file — but the warning must say so, dates only.
    Clock afterExpiry =
        Clock.fixed(
            rsa.certificate().getNotAfter().toInstant().plus(Duration.ofDays(1)), ZoneOffset.UTC);

    try (LogCapture logs = LogCapture.of(TlsMaterial.class)) {
      // Act
      TlsMaterial.load(rsa.certChainPath(), rsa.privateKeyPath(), afterExpiry);

      // Assert
      assertThat(logs.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("expired");
                assertThat(event.getFormattedMessage()).doesNotContain("BEGIN");
              });
    }
  }

  @Test
  void load_notYetValidCert_warnsWithDateOnly() {
    Clock beforeValidity =
        Clock.fixed(
            rsa.certificate().getNotBefore().toInstant().minus(Duration.ofDays(1)), ZoneOffset.UTC);

    try (LogCapture logs = LogCapture.of(TlsMaterial.class)) {
      TlsMaterial.load(rsa.certChainPath(), rsa.privateKeyPath(), beforeValidity);

      assertThat(logs.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("not valid until");
              });
    }
  }

  @Test
  void load_certWithinValidity_logsNoWarning() {
    try (LogCapture logs = LogCapture.of(TlsMaterial.class)) {
      TlsMaterial.load(rsa.certChainPath(), rsa.privateKeyPath(), Clock.systemUTC());

      assertThat(logs.events()).isEmpty();
    }
  }

  @Test
  void certChainPemStream_reparsedByCertificateFactory_yieldsTheValidatedChain() throws Exception {
    TlsMaterial material =
        TlsMaterial.load(rsa.certChainPath(), rsa.privateKeyPath(), Clock.systemUTC());

    List<Certificate> reparsed =
        new ArrayList<>(
            CertificateFactory.getInstance("X.509")
                .generateCertificates(material.certChainPemStream()));

    assertThat(reparsed).containsExactly(rsa.certificate());
  }

  @Test
  void privateKeyPemStream_carriesTheValidatedKeyAsUnencryptedPkcs8() throws Exception {
    TlsMaterial material =
        TlsMaterial.load(rsa.certChainPath(), rsa.privateKeyPath(), Clock.systemUTC());

    String pem =
        new String(material.privateKeyPemStream().readAllBytes(), StandardCharsets.US_ASCII);

    // Exactly the validated key's PKCS#8 encoding, under the PKCS#8 label — what makes the gRPC
    // transport serve vetted bytes rather than whatever the files hold at server-build time.
    assertThat(pem).startsWith("-----BEGIN PRIVATE KEY-----");
    String body =
        pem.replace("-----BEGIN PRIVATE KEY-----", "")
            .replace("-----END PRIVATE KEY-----", "")
            .replaceAll("\\s", "");
    assertThat(Base64.getDecoder().decode(body)).isEqualTo(material.privateKey().getEncoded());
  }

  @Test
  void keyStore_passwordGiven_holdsKeyAndChainUnderOneEntry() throws Exception {
    TlsMaterial material =
        TlsMaterial.load(rsa.certChainPath(), rsa.privateKeyPath(), Clock.systemUTC());
    char[] password = "throwaway".toCharArray();

    KeyStore keyStore = material.keyStore(password);

    assertThat(keyStore.getKey("scalardb-saga-tls", password)).isEqualTo(material.privateKey());
    assertThat(keyStore.getCertificateChain("scalardb-saga-tls"))
        .containsExactly(rsa.certificate());
  }

  @Test
  void load_anyFailure_neverEchoesPemContent() throws IOException {
    // The sweep the redaction doctrine asks for: run the whole failure matrix and assert no
    // message carries a PEM marker or base64 key material. Individual tests pin each message's
    // useful half; this pins the absent half.
    Path pkcs1 = dir.resolve("sweep-pkcs1.key");
    Files.writeString(
        pkcs1, "-----BEGIN RSA PRIVATE KEY-----\nMIIEow==\n-----END RSA PRIVATE KEY-----\n");
    List<ThrowingCallable> cases =
        List.of(
            () -> TlsMaterial.load(dir.resolve("no.crt"), rsa.privateKeyPath(), Clock.systemUTC()),
            () -> TlsMaterial.load(rsa.privateKeyPath(), rsa.privateKeyPath(), Clock.systemUTC()),
            () -> TlsMaterial.load(rsa.certChainPath(), rsa.certChainPath(), Clock.systemUTC()),
            () -> TlsMaterial.load(rsa.certChainPath(), pkcs1, Clock.systemUTC()),
            () ->
                TlsMaterial.load(
                    rsa.certChainPath(), otherRsa.privateKeyPath(), Clock.systemUTC()));

    for (ThrowingCallable failing : cases) {
      // Messages may name PEM labels as static guidance ("a block labeled BEGIN PRIVATE KEY");
      // what they must never carry is content — block delimiters, base64 key material — or the
      // configured path values themselves, which can be mis-pasted secrets. Every input in the
      // list above lives under one of the two temp dirs, so their absence proves no message
      // echoed any path.
      assertThatThrownBy(failing)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageNotContaining("-----")
          .hasMessageNotContaining("MII")
          .hasMessageNotContaining(dir.toString())
          .hasMessageNotContaining(materialDir.toString())
          .hasNoCause();
    }
  }
}
