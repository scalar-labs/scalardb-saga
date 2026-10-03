package com.scalar.db.saga.server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The server's TLS key material: the PEM certificate chain and private key named by {@code
 * tls.cert_chain_path} and {@code tls.private_key_path}, loaded and validated at startup, before
 * either transport binds, and again on every reload pass (see {@link TlsReloader}). The validated
 * chain and key are published to one key manager that both transports serve from, so both present
 * exactly the bytes validated here and cannot diverge from each other or from what was vetted, no
 * matter what happens to the files afterwards. Its acceptance set is deliberately a strict subset
 * of what either stack parses (unencrypted PKCS#8, RSA or EC), which keeps behavior independent of
 * the classpath: BouncyCastle appearing transitively would widen Netty's parser, but never what
 * already passed here.
 *
 * <p>Every failure here is a configuration error an operator must act on — fatal at startup, a
 * logged rejection on reload — so each failure class gets its own message naming the config key,
 * and never the configured value, not even the path. A path <em>value</em> is not provably a path:
 * any {@code scalar.db.saga.*} value may arrive through a secret reference or an inline paste, so a
 * {@code ${file:...}} reference mis-placed on a path key delivers the referenced secret
 * (potentially this very private key) as the "path", and echoing it would write the secret to the
 * log. The operator resolves key to path in their own configuration file. Key material makes the
 * usual redaction rule absolute: the parse exceptions embed raw input, so none are propagated as
 * causes. Certificate <em>metadata</em> (validity dates) is the deliberate exception — the
 * certificate is public material presented to every client on handshake, so {@link TlsReloader}'s
 * validity warnings may name its dates.
 */
final class TlsMaterial {

  // PEM block extraction rather than whole-file parsing, so prose between blocks (openssl's
  // subject= and issuer= comment lines) cannot trip the underlying parsers.
  private static final Pattern CERT_BLOCK =
      Pattern.compile("-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\\s]*)-----END CERTIFICATE-----");
  private static final Pattern CERT_BEGIN = Pattern.compile("-----BEGIN CERTIFICATE-----");
  private static final Pattern KEY_BLOCK =
      Pattern.compile(
          "-----BEGIN ([A-Z0-9 ]*PRIVATE KEY)-----([A-Za-z0-9+/=\\s]*)-----END \\1-----");

  private static final String PKCS8_LABEL = "PRIVATE KEY";
  private static final String ENCRYPTED_LABEL = "ENCRYPTED PRIVATE KEY";
  private static final String PKCS1_LABEL = "RSA PRIVATE KEY";
  private static final String SEC1_LABEL = "EC PRIVATE KEY";

  // Deliberately no path fields: this object is the validated material, not its provenance. The
  // authoritative path holder is SagaServerConfig, which is what the reload pass re-reads — and
  // the redaction rule forbids echoing path values anywhere, so not even diagnostics want them.
  private final List<X509Certificate> certChain;
  private final PrivateKey privateKey;

  private TlsMaterial(List<X509Certificate> certChain, PrivateKey privateKey) {
    this.certChain = certChain;
    this.privateKey = privateKey;
  }

  /**
   * Loads and validates the material: both files readable, the chain parses with at least one
   * certificate, the key parses as unencrypted PKCS#8 RSA or EC, and the key is the one the leaf
   * certificate was issued for. Validity dates are not checked here: the leaf's window is the
   * reload pass's concern, watched continuously rather than judged once.
   *
   * @throws IllegalArgumentException naming the config key — never file content or the configured
   *     value — on any failure
   */
  static TlsMaterial load(Path certChainPath, Path privateKeyPath) {
    String certPem = readPemText(certChainPath, SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY);
    String keyPem = readPemText(privateKeyPath, SagaServerConfig.TLS_PRIVATE_KEY_PATH_KEY);
    List<X509Certificate> chain = parseCertChain(certPem);
    PrivateKey key = parsePrivateKey(keyPem);
    requireKeyMatchesLeaf(key, chain.get(0));
    return new TlsMaterial(List.copyOf(chain), key);
  }

  /** The parsed chain, leaf first, in file order. Never empty. */
  List<X509Certificate> certChain() {
    return certChain;
  }

  PrivateKey privateKey() {
    return privateKey;
  }

  /** The leaf certificate: first in the chain, the one the key was issued for. */
  X509Certificate leaf() {
    return certChain.get(0);
  }

  /**
   * Whether {@code other} carries the same chain and key, compared by DER encoding — what the
   * reload pass asks to tell a re-read of unchanged files, or a reformatted copy of them, from a
   * rotation.
   */
  boolean sameMaterialAs(TlsMaterial other) {
    return certChain.equals(other.certChain)
        && MessageDigest.isEqual(privateKey.getEncoded(), other.privateKey.getEncoded());
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
    // A BEGIN the loop skipped is a block with no END: the shape a non-atomic writer leaves when
    // the file is read mid-write, with the leaf complete and an intermediate cut short. Publishing
    // what did parse would serve a chain missing its intermediates until the next pass; rejecting
    // it costs one interval instead. A kubelet symlink flip never produces this; a plain copy can.
    if (CERT_BEGIN.matcher(pem).results().count() != chain.size()) {
      throw badFile(
          SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY,
          "containing a CERTIFICATE block that is not terminated. If a rotation is in progress the"
              + " next reload pass picks up the complete file; otherwise the file is truncated.");
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
}
