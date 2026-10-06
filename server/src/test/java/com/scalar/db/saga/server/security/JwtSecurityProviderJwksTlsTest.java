package com.scalar.db.saga.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.scalar.db.saga.server.TlsTestCerts;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The JWKS fetch connects through the socket factory it is given: with one trusting the JWKS host's
 * private CA a token validates, and without one the fetch fails the handshake. The factory here
 * trusts the self-signed certificate directly; that the daemon's factory carries its egress bundle
 * is {@code EgressTrustTest}'s concern.
 */
class JwtSecurityProviderJwksTlsTest {

  private static final String ISSUER = "https://issuer.example";
  private static final String AUDIENCE = "saga-daemon";

  @TempDir Path dir;
  private RSAKey signingKey;
  private TlsTestCerts.PemPair jwksCert;
  private HttpsServer jwksServer;
  private Properties props;

  @BeforeEach
  void startJwksServer() throws Exception {
    signingKey = new RSAKeyGenerator(2048).keyID("tls-key").generate();
    jwksCert = TlsTestCerts.generateRsa(dir, "jwks");
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    char[] password = new char[0];
    keyStore.setKeyEntry(
        "jwks",
        privateKey(jwksCert.privateKeyPath()),
        password,
        new Certificate[] {jwksCert.certificate()});
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keyStore, password);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers.getKeyManagers(), null, null);
    byte[] jwks = new JWKSet(signingKey.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
    jwksServer = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
    jwksServer.setHttpsConfigurator(new HttpsConfigurator(context));
    jwksServer.createContext(
        "/jwks.json",
        exchange -> {
          exchange.sendResponseHeaders(200, jwks.length);
          exchange.getResponseBody().write(jwks);
          exchange.close();
        });
    jwksServer.start();
    props = new Properties();
    props.setProperty(
        JwtConfig.JWKS_URL_KEY,
        "https://localhost:" + jwksServer.getAddress().getPort() + "/jwks.json");
    props.setProperty(JwtConfig.ISSUER_KEY, ISSUER);
    props.setProperty(JwtConfig.AUDIENCE_KEY, AUDIENCE);
  }

  @AfterEach
  void stopJwksServer() {
    jwksServer.stop(0);
  }

  @Test
  void create_socketFactoryTrustingTheJwksHostGiven_validatesTokens() throws Exception {
    // Arrange
    JwtSecurityProvider provider = JwtSecurityProvider.create(props, trustingJwksHost());

    // Act
    SagaIdentity identity = provider.authenticate(bearer(token()));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice");
  }

  @Test
  void create_noSocketFactoryGiven_cannotReachThePrivateCaJwksHost() throws Exception {
    // Arrange
    JwtSecurityProvider provider = JwtSecurityProvider.create(props, null);
    String token = token();

    // Act & Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthUnavailableException.class);
  }

  private SSLSocketFactory trustingJwksHost() throws Exception {
    KeyStore anchors = KeyStore.getInstance("PKCS12");
    anchors.load(null, null);
    anchors.setCertificateEntry("jwks", jwksCert.certificate());
    TrustManagerFactory trust =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trust.init(anchors);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, trust.getTrustManagers(), null);
    return context.getSocketFactory();
  }

  private String token() throws Exception {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject("alice")
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .expirationTime(Date.from(Instant.now().plusSeconds(300)))
            .build();
    SignedJWT jwt =
        new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("tls-key").build(), claims);
    jwt.sign(new RSASSASigner(signingKey));
    return jwt.serialize();
  }

  private static PrivateKey privateKey(Path pem) throws Exception {
    String body =
        Files.readString(pem)
            .replace("-----BEGIN PRIVATE KEY-----", "")
            .replace("-----END PRIVATE KEY-----", "");
    return KeyFactory.getInstance("RSA")
        .generatePrivate(new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(body)));
  }

  private static SagaAuthRequest bearer(String token) {
    return SagaAuthRequest.fromHeaders(
        "GET /sagas/x", null, Map.of("Authorization", "Bearer " + token));
  }
}
