package com.scalar.db.saga.server;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CA certificates trusted on outbound HTTPS, participant calls and JWKS fetches alike, and the
 * pass that keeps them current. The trust is the JVM's default trust store plus the PEM bundle
 * named by {@code egress.ca_cert_path}, never a replacement for it, so a public participant keeps
 * working when a private one is added.
 *
 * <p>Both outbound stacks hold one {@link SSLContext} whose trust manager forwards to a delegate
 * the reload pass swaps, so a rotated bundle reaches every new connection without a restart. That
 * matters most during a CA rotation: the new CA has to be trusted before participants move to
 * certificates it issued, and a restart-only design would force every replica to restart inside
 * that window. A swap also invalidates the context's cached client sessions: a resumed handshake
 * never consults the trust manager, so without that a removed CA would keep vouching for hosts
 * already reached until their sessions expired. Each later pass repeats that sweep for sessions
 * created up to the swap, which catches one stored by a handshake already in flight at the swap.
 * Connections already open keep the trust they were verified under. The delegate is the JDK's own
 * trust manager over a key store of both certificate sets, so chain validation and hostname
 * verification are the platform's own.
 *
 * <p>Like {@link TlsReloader}, a pass parses and validates the complete bundle before anything is
 * swapped, and a bundle that fails is rejected whole: the previous trust stays in place and the
 * rejection is logged once per change of state, WARN first and DEBUG while it repeats. Failures
 * name the config key, never the configured value or file content (see {@link TlsMaterial}).
 *
 * <p>The pass state belongs to the reload scheduler's single thread; {@link #run} is {@code
 * synchronized} as a belt, as in {@link TlsReloader}. The delegate is published through a volatile
 * write and read on every handshake.
 */
final class EgressTrust {

  private static final Logger logger = LoggerFactory.getLogger(EgressTrust.class);

  private final Path bundlePath;
  private final List<X509Certificate> jvmDefaults;
  private final SwappableTrustManager trustManager;
  private final SSLContext sslContext;
  private List<X509Certificate> trusted;
  private int rejectedPasses;
  // When the delegate was last swapped, in epoch millis; 0 until the first swap. Every pass drops
  // cached sessions created up to then, not only the pass that swapped: a handshake already under
  // way at the swap was verified under the old trust and can store its session after that sweep,
  // since TLS 1.3 stores it only when the server's session ticket arrives.
  private long swappedAtMillis;
  private @Nullable String lastRejection;

  /**
   * Loads and validates the bundle.
   *
   * @throws IllegalArgumentException naming {@code egress.ca_cert_path} if the bundle is unreadable
   *     or holds anything but complete certificates
   */
  EgressTrust(Path bundlePath) {
    this.bundlePath = bundlePath;
    this.jvmDefaults = jvmDefaultIssuers();
    this.trusted = load(bundlePath);
    this.trustManager = new SwappableTrustManager(trustManagerFor(jvmDefaults, trusted));
    this.sslContext = sslContextOver(trustManager);
  }

  /** The context both outbound stacks build their connections from. */
  SSLContext sslContext() {
    return sslContext;
  }

  /** One pass: re-read and re-validate, swap on change, keep the current trust on rejection. */
  synchronized void run() {
    if (swappedAtMillis > 0) {
      invalidateSessionsCreatedBy(swappedAtMillis);
    }
    List<X509Certificate> candidate;
    try {
      candidate = load(bundlePath);
    } catch (IllegalArgumentException e) {
      reject(Objects.requireNonNull(e.getMessage()));
      return;
    }
    if (!candidate.equals(trusted)) {
      trustManager.delegate = trustManagerFor(jvmDefaults, candidate);
      swappedAtMillis = System.currentTimeMillis();
      invalidateSessionsCreatedBy(swappedAtMillis);
      trusted = candidate;
      logger.info(
          "Outbound CA bundle named by '{}' reloaded; new connections trust its {} certificate(s)"
              + " in addition to the JVM's",
          SagaServerConfig.EGRESS_CA_CERT_PATH_KEY,
          candidate.size());
    }
    if (rejectedPasses > 0) {
      logger.info("Outbound CA bundle reload recovered after {} rejected pass(es)", rejectedPasses);
      rejectedPasses = 0;
      lastRejection = null;
    }
  }

  /**
   * Drops the cached client sessions created at or before {@code cutoffMillis}, so connecting to
   * those hosts again takes a full handshake under the current trust.
   */
  private void invalidateSessionsCreatedBy(long cutoffMillis) {
    SSLSessionContext sessions = sslContext.getClientSessionContext();
    for (Enumeration<byte[]> ids = sessions.getIds(); ids.hasMoreElements(); ) {
      SSLSession session = sessions.getSession(ids.nextElement());
      if (session != null && session.getCreationTime() <= cutoffMillis) {
        session.invalidate();
      }
    }
  }

  private void reject(String reason) {
    rejectedPasses++;
    if (reason.equals(lastRejection)) {
      logger.debug(
          "Outbound CA bundle reload still rejected ({} passes): {}", rejectedPasses, reason);
    } else {
      logger.warn(
          "Outbound CA bundle reload rejected; the {} certificate(s) loaded before stay trusted: {}",
          trusted.size(),
          reason);
      lastRejection = reason;
    }
  }

  private static List<X509Certificate> load(Path bundlePath) {
    return TlsMaterial.loadCertificates(bundlePath, SagaServerConfig.EGRESS_CA_CERT_PATH_KEY);
  }

  /**
   * The JVM's default trust anchors, honoring {@code javax.net.ssl.trustStore} if it is set.
   * Visible for testing.
   */
  static List<X509Certificate> jvmDefaultIssuers() {
    try {
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init((KeyStore) null);
      for (TrustManager manager : factory.getTrustManagers()) {
        if (manager instanceof X509TrustManager x509) {
          return List.of(x509.getAcceptedIssuers());
        }
      }
      throw new IllegalStateException("The JVM's default trust manager is not an X.509 one");
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("The JVM's default trust store could not be read", e);
    }
  }

  /** A JDK trust manager anchored on both certificate sets. Visible for testing. */
  static X509ExtendedTrustManager trustManagerFor(
      List<X509Certificate> jvmDefaults, List<X509Certificate> bundle) {
    try {
      KeyStore anchors = KeyStore.getInstance(KeyStore.getDefaultType());
      anchors.load(null, null);
      for (int i = 0; i < jvmDefaults.size(); i++) {
        anchors.setCertificateEntry("jvm-" + i, jvmDefaults.get(i));
      }
      for (int i = 0; i < bundle.size(); i++) {
        anchors.setCertificateEntry("egress-" + i, bundle.get(i));
      }
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(anchors);
      for (TrustManager manager : factory.getTrustManagers()) {
        if (manager instanceof X509ExtendedTrustManager extended) {
          return extended;
        }
      }
      throw new IllegalStateException("The JVM offers no X.509 trust manager");
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("Could not build the outbound trust store", e);
    }
  }

  private static SSLContext sslContextOver(TrustManager trustManager) {
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, new TrustManager[] {trustManager}, null);
      return context;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Could not initialize the outbound TLS context", e);
    }
  }

  /** Forwards every check to the current delegate, which the reload pass replaces whole. */
  private static final class SwappableTrustManager extends X509ExtendedTrustManager {

    private volatile X509ExtendedTrustManager delegate;

    SwappableTrustManager(X509ExtendedTrustManager delegate) {
      this.delegate = delegate;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      delegate.checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      delegate.checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      delegate.checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      delegate.checkServerTrusted(chain, authType, engine);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      delegate.checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return delegate.getAcceptedIssuers();
    }
  }
}
