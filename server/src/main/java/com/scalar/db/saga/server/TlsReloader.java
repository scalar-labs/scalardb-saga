package com.scalar.db.saga.server;

import io.grpc.util.AdvancedTlsX509KeyManager;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import javax.net.ssl.X509ExtendedKeyManager;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one key manager both transports serve TLS from, and the pass that keeps it current. The
 * constructor seeds it with the material validated at boot; each pass re-reads and re-validates the
 * two configured PEM files, publishes the pair when it changed, and leaves the serving material
 * alone when it did not validate. {@link SagaConfigReloadManager} runs the pass on the
 * configuration reload interval, after both transports have bound.
 *
 * <p>Publishing is one call on the key manager and reaches both transports at once, so they can
 * never serve different generations. A key manager is consulted only during a handshake, so a
 * publish reaches new connections and leaves established ones untouched; grpc-util's implementation
 * also keeps the previous generation answering for one rotation, so a handshake straddling a swap
 * cannot pair one generation's certificate with another's key. The transports never see a partial
 * state either: the pass parses and validates the complete candidate pair before it touches the key
 * manager, and a candidate that fails — unreadable, empty, malformed, a certificate and key from
 * different issuances, or the torn snapshot a kubelet symlink flip yields between the two reads —
 * is rejected whole and retried next pass. A rotation lands well before expiry, so a rejected pass
 * has days of runway; failing the process instead would turn a monitoring problem into an outage.
 *
 * <p>Nothing here is served over the wire, so the pass reports through the log, once per change of
 * state, so that a stuck rotation neither drowns the log nor goes unnoticed: INFO when a rotation
 * is published, WARN on a rejection and DEBUG while the same rejection repeats, INFO when a
 * rotation recovers, and WARN when the serving certificate is expired, not yet valid, or into the
 * last quarter of its validity with no replacement — the sign that an issuer's renewal has not
 * landed, measured against the certificate's own lifetime so a one-day and a one-year certificate
 * alert at the same point past their due renewal. The alertable condition is the conjunction:
 * rejections while expiry approaches. Validity dates are the one certificate detail echoed, being
 * public handshake material; PEM content and configured path values never are (see {@link
 * TlsMaterial}).
 *
 * <p>The pass state belongs to the reload scheduler's single thread; {@link #run} is {@code
 * synchronized} as a belt, so the synchronous test seam cannot interleave with a scheduled pass.
 * The key manager publishes through a volatile write and is safe to hand to the transports.
 */
final class TlsReloader {

  private static final Logger logger = LoggerFactory.getLogger(TlsReloader.class);

  private final Path certChainPath;
  private final Path privateKeyPath;
  private final Clock clock;
  private final AdvancedTlsX509KeyManager keyManager = new AdvancedTlsX509KeyManager();
  private TlsMaterial serving;
  // Rejections since the last pass that validated; what the recovery line reports.
  private int rejectedPasses;
  // The last rejection reason and validity notice logged, so a repeat goes to DEBUG (or nowhere)
  // instead of repeating a WARN every interval.
  private @Nullable String lastRejection;
  private @Nullable String lastValidityNotice;

  TlsReloader(Path certChainPath, Path privateKeyPath, Clock clock, TlsMaterial boot) {
    this.certChainPath = certChainPath;
    this.privateKeyPath = privateKeyPath;
    this.clock = clock;
    this.serving = boot;
    publish(boot);
    noticeValidity();
  }

  /** The key manager both transports serve from; consulted on every handshake. */
  X509ExtendedKeyManager keyManager() {
    return keyManager;
  }

  /** One pass: re-read and re-validate, publish on change, keep serving on rejection. */
  synchronized void run() {
    TlsMaterial candidate;
    try {
      candidate = TlsMaterial.load(certChainPath, privateKeyPath);
    } catch (IllegalArgumentException e) {
      // TlsMaterial's failures always carry a message; requireNonNull bridges the JDK's nullable
      // getMessage signature.
      reject(Objects.requireNonNull(e.getMessage()));
      return;
    }
    if (!candidate.sameMaterialAs(serving)) {
      publish(candidate);
      serving = candidate;
      logger.info(
          "TLS certificate rotated; new connections are served the certificate valid until {}",
          notAfter(candidate));
    }
    if (rejectedPasses > 0) {
      logger.info("TLS reload recovered after {} rejected pass(es)", rejectedPasses);
      rejectedPasses = 0;
      lastRejection = null;
    }
    noticeValidity();
  }

  private void publish(TlsMaterial material) {
    keyManager.updateIdentityCredentials(
        material.certChain().toArray(new X509Certificate[0]), material.privateKey());
  }

  private void reject(String reason) {
    rejectedPasses++;
    if (reason.equals(lastRejection)) {
      logger.debug("TLS reload still rejected ({} passes): {}", rejectedPasses, reason);
    } else {
      logger.warn(
          "TLS reload rejected; the certificate valid until {} keeps serving: {}",
          notAfter(serving),
          reason);
      lastRejection = reason;
    }
    // The serving certificate keeps ageing while rejections continue; this is where "reloads
    // failing while expiry approaches" becomes visible.
    noticeValidity();
  }

  /**
   * Warns, once per change of state, when the serving certificate is outside its validity window or
   * into the last quarter of it. The server keeps serving either way: rotation may land a fresh
   * file before real traffic arrives, and refusing to serve would turn a monitoring problem into an
   * outage — but every client will reject the handshake of an expired certificate, so say so.
   */
  private void noticeValidity() {
    X509Certificate leaf = serving.leaf();
    Instant now = clock.instant();
    Instant notBefore = leaf.getNotBefore().toInstant();
    Instant notAfter = leaf.getNotAfter().toInstant();
    Duration lifetime = Duration.between(notBefore, notAfter);
    String notice;
    if (now.isBefore(notBefore)) {
      notice =
          "is not valid until " + notBefore + "; clients will reject the handshake until then.";
    } else if (!now.isBefore(notAfter)) {
      notice =
          "expired at " + notAfter + "; clients will reject the handshake until it is replaced.";
    } else if (Duration.between(now, notAfter).compareTo(lifetime.dividedBy(4)) < 0) {
      notice =
          "expires at "
              + notAfter
              + " and is into the last quarter of its validity with no replacement in sight."
              + " Check the issuer and the mounted files.";
    } else {
      notice = null;
    }
    if (!Objects.equals(notice, lastValidityNotice)) {
      if (notice != null) {
        logger.warn(
            "The TLS certificate named by '{}' {}",
            SagaServerConfig.TLS_CERT_CHAIN_PATH_KEY,
            notice);
      }
      lastValidityNotice = notice;
    }
  }

  private static Instant notAfter(TlsMaterial material) {
    return material.leaf().getNotAfter().toInstant();
  }
}
