package com.scalar.db.saga.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Generates throwaway TLS material for tests: a self-signed certificate (SAN {@code localhost} and
 * {@code 127.0.0.1}) plus its unencrypted PKCS#8 key, written as PEM files. Generated at test time
 * rather than checked in, so nothing expires in CI and no private key lives in the repository.
 *
 * <p>Generation shells out to the JDK's own {@code keytool}, deliberately adding no crypto
 * dependency: BouncyCastle on a test classpath would silently widen Netty's PEM parsing (Netty
 * accepts PKCS#1 keys only when BC is present), and the TLS suites exist to exercise the production
 * parse path — the one without it.
 */
public final class TlsTestCerts {

  /** The generated PEM files, plus the parsed certificate for tests that assert against it. */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification =
          "Test-only value carrier; X509Certificate is not mutated by anything holding it")
  public record PemPair(Path certChainPath, Path privateKeyPath, X509Certificate certificate) {}

  private static final String ALIAS = "tls";
  private static final String PASSWORD = "changeit";

  private TlsTestCerts() {}

  /** Generates an RSA-2048 pair under {@code dir}, file names derived from {@code baseName}. */
  public static PemPair generateRsa(Path dir, String baseName) {
    return generate(dir, baseName, "RSA", "2048");
  }

  /** Generates a P-256 EC pair under {@code dir}, file names derived from {@code baseName}. */
  public static PemPair generateEc(Path dir, String baseName) {
    return generate(dir, baseName, "EC", "256");
  }

  /** Wraps DER bytes in a PEM block with the given label, 64-column MIME line wrapping. */
  public static String pem(String label, byte[] der) {
    String base64 =
        Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der);
    return "-----BEGIN " + label + "-----\n" + base64 + "\n-----END " + label + "-----\n";
  }

  @SuppressFBWarnings(
      value = "HARD_CODE_PASSWORD",
      justification =
          "The keystore exists only for the seconds between keytool writing it and this method"
              + " exporting PEM from it, in a test temp dir; the password protects nothing")
  private static PemPair generate(Path dir, String baseName, String keyAlg, String keySize) {
    Path keystorePath = dir.resolve(baseName + ".p12");
    runKeytool(
        List.of(
            keytoolBinary(),
            "-genkeypair",
            "-alias",
            ALIAS,
            "-keyalg",
            keyAlg,
            "-keysize",
            keySize,
            "-dname",
            "CN=localhost",
            "-validity",
            "3650",
            "-ext",
            "SAN=dns:localhost,ip:127.0.0.1",
            "-keystore",
            keystorePath.toString(),
            "-storetype",
            "PKCS12",
            "-storepass",
            PASSWORD,
            "-keypass",
            PASSWORD));
    try {
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      try (InputStream in = Files.newInputStream(keystorePath)) {
        keyStore.load(in, PASSWORD.toCharArray());
      }
      PrivateKey key = (PrivateKey) keyStore.getKey(ALIAS, PASSWORD.toCharArray());
      X509Certificate certificate = (X509Certificate) keyStore.getCertificate(ALIAS);
      // PrivateKey.getEncoded() is PKCS#8 DER for the JDK's RSA and EC implementations, so the
      // PEM below is exactly the "BEGIN PRIVATE KEY" form the server requires.
      Path certPath = dir.resolve(baseName + ".crt");
      Path keyPath = dir.resolve(baseName + ".key");
      Files.writeString(certPath, pem("CERTIFICATE", certificate.getEncoded()));
      Files.writeString(keyPath, pem("PRIVATE KEY", key.getEncoded()));
      return new PemPair(certPath, keyPath, certificate);
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException("Failed to export the generated test material as PEM", e);
    }
  }

  private static String keytoolBinary() {
    return Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
  }

  @SuppressFBWarnings(
      value = "COMMAND_INJECTION",
      justification =
          "The argv is compile-time constants plus JUnit temp-dir paths; nothing an external"
              + " party influences reaches it, and this runs only in tests")
  private static void runKeytool(List<String> command) {
    try {
      Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
      // waitFor first, so the timeout is real: readAllBytes blocks to EOF, i.e. process exit,
      // which would make a timeout checked after it unreachable. Waiting before draining cannot
      // deadlock here — keytool's output is far below the OS pipe buffer.
      if (!process.waitFor(60, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new IllegalStateException("keytool did not finish within 60 seconds");
      }
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (process.exitValue() != 0) {
        throw new IllegalStateException("keytool failed: " + output);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to run keytool", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for keytool", e);
    }
  }
}
