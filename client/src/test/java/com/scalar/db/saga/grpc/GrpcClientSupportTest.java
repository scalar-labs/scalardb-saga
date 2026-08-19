package com.scalar.db.saga.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GrpcClientSupportTest {

  @Test
  void tcnativeAvailable_shadedTransportOnClasspath_returnsTrue() {
    // Arrange — nothing to set up: the module's own runtime dependency set is the fixture. The
    // shipped transport is grpc-netty-shaded, which relocates Netty and bundles tcnative, so the
    // probe must find the shaded OpenSsl class and report its bundled native as loaded. A probe
    // that only knows the unshaded name returns false here — the regression this test pins down.

    // Act
    boolean available = GrpcClientSupport.tcnativeAvailable();

    // Assert
    assertThat(available).isTrue();
  }
}
