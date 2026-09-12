package com.scalar.db.saga.server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server's TLS key material: the PEM certificate chain and private key named by {@code
 * tls.cert_chain_path} and {@code tls.private_key_path}, loaded and validated once at startup,
 * before either transport binds. Both transports consume this one component — Jetty through {@link
 * #keyStore}, gRPC through the validated material re-encoded as PEM ({@link #certChainPemStream}
 * and {@link #privateKeyPemStream}) — so both serve exactly the bytes validated here and cannot
 * diverge from each other or from what was vetted, no matter what happens to the files after
 * validation. Its acceptance set is deliberately a strict subset of what either stack parses
 * (unencrypted PKCS#8, RSA or EC), which keeps behavior independent of the classpath: BouncyCastle
 * appearing transitively would widen Netty's parser, but never what already passed here.
 *
 * <p>A future certificate reload starts here but does not end here: re-running {@link #load}
 * re-validates the same paths, but neither listener would notice a swapped result on its own —
 * Jetty serves from an {@code SslContextFactory} built once in {@code SagaServer.tlsConnector}
 * (reload must retain that factory and call its {@code reload(...)}), and gRPC pins the SslContext
 * built from this material at server-build time (reload needs a delegating key manager, e.g. grpc's
 * {@code AdvancedTlsX509KeyManager}, or a server rebuild). Until that wiring exists, certificate
 * rotation is a restart.
 *
 * <p>Every failure here is a startup error an operator must act on, so each failure class gets its
 * own message naming the config key — and never the configured value, not even the path. A path
 * <em>value</em> is not provably a path: any {@code scalar.db.saga.*} value may arrive through a
 * secret reference or an inline paste, so a {@code ${file:...}} reference mis-placed on a path key
 * delivers the referenced secret (potentially this very private key) as the "path", and echoing it
 * would write the secret to the log. The operator resolves key to path in their own configuration
 * file. Key material makes the usual redaction rule absolute: the parse exceptions embed raw input,
 * so none are propagated as causes. Certificate <em>metadata</em> (validity dates) is the
 * deliberate exception — the certificate is public material presented to every client on handshake,
 * so the expiry warning may name its dates.
 */
final class TlsMaterial {

  private static final Logger logger = LoggerFactory.getLogger(TlsMaterial.class);

  // PEM block extraction rather than whole-file parsing, so prose between blocks (openssl's
  // subject= and issuer= comment lines) cannot trip the underlying parsers.
  private static final Pattern CERT_BLOCK =
      Pattern.compile("-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\\s]*)-----END CERTIFICATE-----");
  private static final Pattern KEY_BLOCK =
      Pattern.compile(
          "-----BEGIN ([A-Z0-9 ]*PRIVATE KEY)-----([A-Za-z0-9+/=\\s]*)-----END \\1-----");

  private static final String PKCS8_LABEL = "PRIVATE KEY";
  private static final String ENCRYPTED_LABEL = "ENCRYPTED PRIVATE KEY";
  private static final String PKCS1_LABEL = "RSA PRIVATE KEY";
  private static final String SEC1_LABEL = "EC PRIVATE KEY";

  // Deliberately no path fields: this object is the validated material, not its provenance. The
  // authoritative path holder is SagaServerConfig, which is what a future reload re-reads — and
  // the redaction rule forbids echoing path values anywhere, so not even diagnostics want them.
  private final List<X509Certificate> certChain;
  private final PrivateKey privateKey;
  // The validated material re-encoded as PEM, computed once: what the gRPC builder consumes, so
  // Netty never re-reads the files (see certChainPemStream).
  private final byte[] certChainPem;
  private final byte[] privateKeyPem;

  private TlsMaterial(List<X509Certificate> certChain, PrivateKey privateKey) {
    this.certChain = certChain;
    this.privateKey = privateKey;
    this.certChainPem = pemEncode("CERTIFICATE", certificateDers(certChain));
    this.privateKeyPem = pemEncode("PRIVATE KEY", List.of(privateKey.getEncoded()));
  }

  /**
   * Loads and validates the material: both files readable, the chain parses with at least one
   * certificate, the key parses as unencrypted PKCS#8 RSA or EC, and the key is the one the leaf
   * certificate was issued for. Warns (dates only) when the leaf is outside its validity window,
   * judged against {@code clock} so tests can pin time.
   *
   * @throws IllegalArgumentException naming the config key — never file content or the configured
   *     value — on any failure
   */
  static TlsMaterial load(Path certChainPath, Path privateKeyPath, Clock clock) {
    String certPem = readPemText(certChainPath, SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY);
    String keyPem = readPemText(privateKeyPath, SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
    List<X509Certificate> chain = parseCertChain(certPem);
    PrivateKey key = parsePrivateKey(keyPem);
    requireKeyMatchesLeaf(key, chain.get(0));
    warnIfOutsideValidity(chain.get(0), clock);
    return new TlsMaterial(List.copyOf(chain), key);
  }

  /** The parsed chain, leaf first, in file order. Never empty. */
  List<X509Certificate> certChain() {
    return certChain;
  }

  PrivateKey privateKey() {
    return privateKey;
  }

  /**
   * The validated chain re-encoded as PEM, as a fresh stream — what the gRPC builder consumes.
   * Handing over re-encoded bytes rather than the file paths is what guarantees gRPC serves exactly
   * the validated material: the files can change between validation and server build (a rotation
   * landing mid-boot), and Netty would otherwise read them a second time.
   */
  InputStream certChainPemStream() {
    return new ByteArrayInputStream(certChainPem);
  }

  /** The validated key re-encoded as unencrypted PKCS#8 PEM, as a fresh stream. */
  InputStream privateKeyPemStream() {
    return new ByteArrayInputStream(privateKeyPem);
  }

  /**
   * Builds an in-memory PKCS12 keystore holding the key and chain under one entry, protected by
   * {@code password} — for Jetty, whose {@code SslContextFactory} initializes its key manager with
   * the keystore password, so the caller must hand it the same throwaway password it passes here.
   * Nothing is written to disk.
   */
  KeyStore keyStore(char[] password) {
    try {
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      keyStore.load(null, null);
      keyStore.setKeyEntry(
          "scalardb-saga-tls", privateKey, password, certChain.toArray(new X509Certificate[0]));
      return keyStore;
    } catch (IOException | GeneralSecurityException e) {
      // Assembling an in-memory PKCS12 from already-validated material does not fail for
      // config-attributable reasons; if it does, something is wrong with the runtime itself.
      throw new IllegalStateException("Failed to assemble the in-memory TLS keystore", e);
    }
  }

  private static List<byte[]> certificateDers(List<X509Certificate> chain) {
    List<byte[]> ders = new ArrayList<>();
    for (X509Certificate certificate : chain) {
      try {
        ders.add(certificate.getEncoded());
      } catch (CertificateEncodingException e) {
        // Re-encoding an already-parsed certificate does not fail for config-attributable
        // reasons; if it does, something is wrong with the runtime itself.
        throw new IllegalStateException("Failed to re-encode the validated certificate chain", e);
      }
    }
    return ders;
  }

  private static byte[] pemEncode(String label, List<byte[]> ders) {
    StringBuilder pem = new StringBuilder();
    Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
    for (byte[] der : ders) {
      pem.append("-----BEGIN ")
          .append(label)
          .append("-----\n")
          .append(encoder.encodeToString(der))
          .append("\n-----END ")
          .append(label)
          .append("-----\n");
    }
    return pem.toString().getBytes(StandardCharsets.US_ASCII);
  }

  /**
   * Builds the standard failure for a TLS file problem: names the config key and what is wrong with
   * the file it names — never the configured value (see the class javadoc for why the path is not
   * echoed). Every message shape below flows through here so the value-free rule is enforced in one
   * place.
   */
  private static IllegalArgumentException badFile(String key, String detail) {
    return new IllegalArgumentException("'" + key + "' names a file " + detail);
  }

  /**
   * Reads a PEM file as text, mapping each I/O failure class to its own actionable message. The
   * unreadable case is deliberately distinct from the missing one: a Kubernetes Secret mounted with
   * root-owned {@code 0600} permissions is invisible to the server's non-root uid, and "no such
   * file" would send the operator hunting for a mount that is present.
   */
  private static String readPemText(Path path, String key) {
    try {
      return Files.readString(path, StandardCharsets.UTF_8);
    } catch (NoSuchFileException e) {
      throw badFile(key, "that does not exist.");
    } catch (AccessDeniedException e) {
      throw badFile(
          key,
          "that exists but is not readable by this process. Check the file's permissions against"
              + " the server's uid — a Secret mounted root-owned with mode 0600 is the usual"
              + " cause; mount it with a mode the server's non-root uid can read, e.g. 0444.");
    } catch (IOException e) {
      // Includes CharacterCodingException: a binary file cannot be UTF-8-decoded, and the most
      // likely binary here is DER. The cause stays off: its message can embed raw input.
      throw badFile(
          key,
          "that could not be read as text. If the file is binary DER, convert it to PEM (openssl"
              + " x509 -inform der for a certificate; openssl pkcs8 -topk8 -nocrypt for a key).");
    }
  }

  private static List<X509Certificate> parseCertChain(String pem) {
    CertificateFactory factory;
    try {
      factory = CertificateFactory.getInstance("X.509");
    } catch (CertificateException e) {
      throw new IllegalStateException("The JVM offers no X.509 certificate factory", e);
    }
    List<X509Certificate> chain = new ArrayList<>();
    Matcher matcher = CERT_BLOCK.matcher(pem);
    while (matcher.find()) {
      byte[] der = decodeBase64(matcher.group(1), SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY);
      try {
        chain.add((X509Certificate) factory.generateCertificate(new ByteArrayInputStream(der)));
      } catch (CertificateException e) {
        throw badFile(
            SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY,
            "whose CERTIFICATE block does not parse as an X.509 certificate.");
      }
    }
    if (chain.isEmpty()) {
      if (KEY_BLOCK.matcher(pem).find()) {
        throw badFile(
            SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY,
            "holding private-key material, not a certificate chain. Did the two tls.* path values"
                + " get swapped?");
      }
      throw badFile(
          SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY,
          "with no CERTIFICATE block. The file must be a PEM certificate chain, leaf first.");
    }
    return chain;
  }

  private static PrivateKey parsePrivateKey(String pem) {
    Matcher matcher = KEY_BLOCK.matcher(pem);
    if (!matcher.find()) {
      if (CERT_BLOCK.matcher(pem).find()) {
        throw badFile(
            SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
            "holding a certificate, not a private key. Did the two tls.* path values get"
                + " swapped?");
      }
      // A PKCS#1 key encrypted the RFC 1421 way carries Proc-Type/DEK-Info headers inside the
      // block, which the block pattern (base64-only body) rightly refuses to match — but the
      // operator deserves the encrypted-key guidance, not a generic "no block" complaint.
      if (pem.contains("Proc-Type") && pem.contains("ENCRYPTED")) {
        throw badFile(
            SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
            "holding an encrypted legacy private key (RFC 1421 encryption headers). Only"
                + " unencrypted PKCS#8 keys are supported — protect the file with mount"
                + " permissions instead, and decrypt and convert it with: openssl pkcs8 -topk8"
                + " -nocrypt");
      }
      throw badFile(
          SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
          "with no PRIVATE KEY block. The file must be an unencrypted PKCS#8 PEM key (a block"
              + " labeled BEGIN PRIVATE KEY).");
    }
    String label = matcher.group(1);
    switch (label) {
      case PKCS8_LABEL -> {}
      case ENCRYPTED_LABEL ->
          throw badFile(
              SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
              "holding an encrypted private key. Only unencrypted PKCS#8 keys are supported —"
                  + " protect the file with mount permissions instead, and decrypt it with:"
                  + " openssl pkcs8 -topk8 -nocrypt");
      case PKCS1_LABEL ->
          throw badFile(
              SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
              "holding a legacy PKCS#1 key (BEGIN RSA PRIVATE KEY); only PKCS#8 (BEGIN PRIVATE"
                  + " KEY) is supported. cert-manager emits PKCS#1 unless the Certificate sets"
                  + " spec.privateKey.encoding: PKCS8; Vault needs private_key_format=pkcs8; or"
                  + " convert with: openssl pkcs8 -topk8 -nocrypt");
      case SEC1_LABEL ->
          throw badFile(
              SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
              "holding a legacy SEC1 EC key (BEGIN EC PRIVATE KEY); only PKCS#8 (BEGIN PRIVATE"
                  + " KEY) is supported. Convert with: openssl pkcs8 -topk8 -nocrypt");
      default ->
          throw badFile(
              SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
              "whose PRIVATE KEY block is not one this server supports. Supply an unencrypted"
                  + " PKCS#8 key (a block labeled BEGIN PRIVATE KEY).");
    }
    byte[] der = decodeBase64(matcher.group(2), SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
    for (String algorithm : List.of("RSA", "EC")) {
      try {
        return KeyFactory.getInstance(algorithm).generatePrivate(spec);
      } catch (GeneralSecurityException e) {
        // Try the next algorithm; the spec carries its own algorithm ID, so exactly one can
        // succeed.
      }
    }
    // Reached both by an unsupported algorithm and by corrupt DER (valid base64 that is not a
    // key); the message must not send an operator with a corrupt file off to change algorithms.
    throw badFile(
        SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY,
        "holding a PKCS#8 key that parses as neither RSA nor EC: the block is corrupt, or its"
            + " algorithm (e.g. Ed25519) is not supported.");
  }

  private static byte[] decodeBase64(String body, String key) {
    try {
      // The MIME decoder tolerates line wraps and both line-ending styles.
      return Base64.getMimeDecoder().decode(body);
    } catch (IllegalArgumentException e) {
      throw badFile(key, "containing a PEM block whose contents are not valid base64.");
    }
  }

  /**
   * Verifies the private key is the one the leaf certificate was issued for, by signing a probe
   * with the key and verifying it with the leaf's public key. Neither Jetty nor Netty performs this
   * check at startup; without it a mismatch surfaces only as handshake failures on every
   * connection, after the ports are already serving.
   */
  private static void requireKeyMatchesLeaf(PrivateKey key, X509Certificate leaf) {
    boolean matches;
    if (!key.getAlgorithm().equals(leaf.getPublicKey().getAlgorithm())) {
      matches = false;
    } else {
      String algorithm = key.getAlgorithm().equals("RSA") ? "SHA256withRSA" : "SHA256withECDSA";
      byte[] probe = "scalardb-saga tls key-match probe".getBytes(StandardCharsets.UTF_8);
      try {
        Signature signer = Signature.getInstance(algorithm);
        signer.initSign(key);
        signer.update(probe);
        byte[] signature = signer.sign();
        Signature verifier = Signature.getInstance(algorithm);
        verifier.initVerify(leaf.getPublicKey());
        verifier.update(probe);
        matches = verifier.verify(signature);
      } catch (GeneralSecurityException e) {
        // A key that cannot sign, or a public key that cannot verify, is as unusable as a clean
        // mismatch; the cause stays off like every other parse exception here.
        matches = false;
      }
    }
    if (!matches) {
      throw new IllegalArgumentException(
          "The private key named by '"
              + SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY
              + "' does not match the leaf certificate named by '"
              + SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY
              + "'. The key must be the one the leaf certificate was issued for; the usual cause"
              + " is a renewed certificate mounted alongside a stale key, or the two keys naming"
              + " material from different issuances.");
    }
  }

  /**
   * Warns when the leaf certificate is outside its validity window. The server still starts —
   * cert-manager-style rotation can land a fresh file before real traffic arrives, and refusing to
   * boot would turn a monitoring problem into an outage — but every client will reject the
   * handshake until the material is replaced, so say so at startup. Dates only: validity is public
   * handshake material, but nothing else from the certificate is echoed.
   */
  private static void warnIfOutsideValidity(X509Certificate leaf, Clock clock) {
    Instant now = clock.instant();
    Instant notBefore = leaf.getNotBefore().toInstant();
    Instant notAfter = leaf.getNotAfter().toInstant();
    if (now.isBefore(notBefore)) {
      logger.warn(
          "The TLS certificate named by '{}' is not valid until {}; clients will reject the"
              + " handshake until then.",
          SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY,
          notBefore);
    } else if (now.isAfter(notAfter)) {
      logger.warn(
          "The TLS certificate named by '{}' expired at {}; clients will reject the handshake"
              + " until it is replaced.",
          SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY,
          notAfter);
    }
  }
}
