package com.scalar.db.saga.daemon.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.daemon.api.RateLimitHandler;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.daemon.security.SagaSecurityHandler;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Cross-transport policy parity: for every saga operation the REST and gRPC transports must resolve
 * the same required role and the same rate-limit treatment, even though REST keys on the HTTP verb
 * ({@link SagaSecurityHandler#requiredRoleFor}, {@link RateLimitHandler#isRateLimited}) and gRPC
 * keys on the RPC method name ({@link SagaSecurityInterceptor#requiredRoleFor}, {@link
 * SagaRateLimitInterceptor#isRateLimited}). The two mappings are maintained independently — aligned
 * only by "Keep in sync" comments today — so this test turns that comment discipline into a CI
 * gate: a future divergence fails the build rather than silently drifting.
 *
 * <p>Each operation also pins its expected role and rate-limit treatment, so the two transports
 * cannot pass by agreeing on the <em>wrong</em> value. Extend {@link #OPERATIONS} when an operation
 * is added to the REST/RPC surface.
 *
 * <p>Lives in the {@code grpc} test package so it can read the two package-private gRPC resolvers
 * directly; the two REST-side resolvers are public. When the switches are eventually replaced by a
 * shared {@code operation -> role} policy (see the admin-API plan), this test survives as the
 * regression guard that the two per-transport vocabulary mappings still agree.
 */
class TransportPolicyParityTest {

  /** A saga operation, how each transport names it, and its expected policy. */
  private record Operation(
      String name,
      String restVerb,
      String grpcMethod,
      SagaRole expectedRole,
      boolean expectedRateLimited) {}

  // The known saga operations. Starting a saga is one gRPC method (StartSaga) but two REST verbs
  // (POST /sagas to create, PUT /sagas/{id} for a versioned start), so both verbs are listed
  // against StartSaga to exercise each.
  private static final List<Operation> OPERATIONS =
      List.of(
          new Operation("start (create)", "POST", "StartSaga", SagaRole.WRITE, true),
          new Operation("start (versioned)", "PUT", "StartSaga", SagaRole.WRITE, true),
          new Operation("get", "GET", "GetSaga", SagaRole.READ, false),
          new Operation("await", "GET", "AwaitSaga", SagaRole.READ, false));

  @Test
  void requiredRole_matchesExpectedAndAgreesAcrossTransports() {
    for (Operation op : OPERATIONS) {
      // Arrange / Act
      SagaRole rest = SagaSecurityHandler.requiredRoleFor(op.restVerb());
      SagaRole grpc = SagaSecurityInterceptor.requiredRoleFor(op.grpcMethod());

      // Assert — each transport resolves the expected role (so they cannot agree on a wrong value).
      assertThat(rest)
          .as("REST role for '%s' (%s)", op.name(), op.restVerb())
          .isEqualTo(op.expectedRole());
      assertThat(grpc)
          .as("gRPC role for '%s' (%s)", op.name(), op.grpcMethod())
          .isEqualTo(op.expectedRole());
    }
  }

  @Test
  void rateLimitTreatment_matchesExpectedAndAgreesAcrossTransports() {
    for (Operation op : OPERATIONS) {
      // Arrange / Act
      boolean rest = RateLimitHandler.isRateLimited(op.restVerb());
      boolean grpc = SagaRateLimitInterceptor.isRateLimited(op.grpcMethod());

      // Assert — each transport applies the expected rate-limit treatment.
      assertThat(rest)
          .as("REST rate-limit for '%s' (%s)", op.name(), op.restVerb())
          .isEqualTo(op.expectedRateLimited());
      assertThat(grpc)
          .as("gRPC rate-limit for '%s' (%s)", op.name(), op.grpcMethod())
          .isEqualTo(op.expectedRateLimited());
    }
  }
}
