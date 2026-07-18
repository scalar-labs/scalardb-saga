package com.scalar.db.saga.daemon.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.daemon.api.CallbackResource;
import com.scalar.db.saga.daemon.api.HealthResource;
import com.scalar.db.saga.daemon.api.SagaAdminResource;
import com.scalar.db.saga.daemon.api.SagaResource;
import com.scalar.db.saga.daemon.security.SagaOperation;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import io.grpc.MethodDescriptor;
import io.javalin.Javalin;
import io.javalin.router.Endpoint;
import io.javalin.security.RouteRole;
import java.time.Clock;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * The permanent cross-transport guard on the daemon's access policy.
 *
 * <p>Its shape follows from where the risk actually lives. Both transports now resolve their policy
 * from {@link SagaOperation}, so asserting that they agree with each other would be true by
 * construction and would prove nothing. What can still go wrong is the <b>mapping</b>: a route or
 * an RPC exposed without an operation, or under the wrong one. So this test asserts three things
 * instead:
 *
 * <ol>
 *   <li>the policy table itself — every operation's required role and rate-limit treatment, stated
 *       literally, so changing the policy has to be a deliberate edit here;
 *   <li>gRPC completeness — every method on the generated service descriptor maps to the expected
 *       operation, and nothing is mapped that is not a real method;
 *   <li>REST completeness — every registered route carries the expected operation, and none is
 *       untagged.
 * </ol>
 *
 * <p>(2) and (3) enumerate the real registrations rather than a hand-written list, which is what
 * makes "added an endpoint, forgot the policy" a build failure. That matters most for the
 * fail-closed resolver: an untagged route is refused at runtime, so without this test the first
 * sign of a missing tag would be a dead endpoint in production.
 */
class TransportPolicyParityTest {

  /** The expected policy for every operation. Stated literally; not derived from the enum. */
  private static final Map<SagaOperation, ExpectedPolicy> EXPECTED_POLICY =
      new EnumMap<>(
          Map.ofEntries(
              Map.entry(SagaOperation.HEALTH, new ExpectedPolicy(null, false)),
              Map.entry(SagaOperation.CALLBACK, new ExpectedPolicy(null, false)),
              Map.entry(SagaOperation.START_SAGA, new ExpectedPolicy(SagaRole.WRITE, true)),
              Map.entry(SagaOperation.GET_SAGA, new ExpectedPolicy(SagaRole.READ, false)),
              Map.entry(SagaOperation.AWAIT_SAGA, new ExpectedPolicy(SagaRole.READ, false)),
              Map.entry(SagaOperation.LIST_SAGAS, new ExpectedPolicy(SagaRole.ADMIN, true)),
              Map.entry(SagaOperation.GET_SAGA_DETAIL, new ExpectedPolicy(SagaRole.READ, false)),
              Map.entry(SagaOperation.RECOVER_SAGA, new ExpectedPolicy(SagaRole.ADMIN, true)),
              Map.entry(SagaOperation.FORCE_COMPLETE, new ExpectedPolicy(SagaRole.ADMIN, true)),
              Map.entry(SagaOperation.RESET_ESCALATED, new ExpectedPolicy(SagaRole.ADMIN, true))));

  /** The expected operation for every gRPC method (across both services), by bare method name. */
  private static final Map<String, SagaOperation> EXPECTED_GRPC_METHODS =
      Map.ofEntries(
          Map.entry("StartSaga", SagaOperation.START_SAGA),
          Map.entry("GetSaga", SagaOperation.GET_SAGA),
          Map.entry("AwaitSaga", SagaOperation.AWAIT_SAGA),
          Map.entry("GetSagaDetail", SagaOperation.GET_SAGA_DETAIL),
          Map.entry("ListSagas", SagaOperation.LIST_SAGAS),
          Map.entry("RecoverSaga", SagaOperation.RECOVER_SAGA),
          Map.entry("ForceComplete", SagaOperation.FORCE_COMPLETE),
          Map.entry("ResetEscalated", SagaOperation.RESET_ESCALATED),
          Map.entry("ResetEscalatedBulk", SagaOperation.RESET_ESCALATED));

  /** The expected operation for every REST route, by {@code "METHOD path"}. */
  private static final Map<String, SagaOperation> EXPECTED_REST_ROUTES =
      Map.ofEntries(
          Map.entry("GET /health", SagaOperation.HEALTH),
          Map.entry("POST /sagas/{id}/steps/{stepName}/complete", SagaOperation.CALLBACK),
          Map.entry("POST /sagas", SagaOperation.START_SAGA),
          Map.entry("PUT /sagas/{id}", SagaOperation.START_SAGA),
          Map.entry("GET /sagas/{id}", SagaOperation.GET_SAGA),
          Map.entry("GET /sagas", SagaOperation.LIST_SAGAS),
          Map.entry("GET /sagas/{id}/detail", SagaOperation.GET_SAGA_DETAIL),
          Map.entry("POST /sagas/{id}/recover", SagaOperation.RECOVER_SAGA),
          Map.entry("POST /sagas/{id}/force-complete", SagaOperation.FORCE_COMPLETE),
          Map.entry("POST /sagas/{id}/reset", SagaOperation.RESET_ESCALATED),
          Map.entry("POST /admin/reset-escalated", SagaOperation.RESET_ESCALATED));

  private record ExpectedPolicy(@Nullable SagaRole role, boolean limited) {}

  @Test
  void policyTable_matchesExpected() {
    // Assert — the table covers every operation, so a new one cannot be added without a decision
    // being recorded here
    assertThat(EXPECTED_POLICY.keySet())
        .as("every SagaOperation must have an expected policy")
        .containsExactlyInAnyOrder(SagaOperation.values());

    EXPECTED_POLICY.forEach(
        (operation, expected) -> {
          assertThat(operation.requiredRole())
              .as("required role for %s", operation)
              .isEqualTo(expected.role());
          assertThat(operation.rateLimited())
              .as("rate-limit treatment for %s", operation)
              .isEqualTo(expected.limited());
        });
  }

  @Test
  void grpcMethods_allMapToTheExpectedOperation() {
    // Arrange — the generated descriptors are the source of truth for what is actually exposed,
    // over
    // both the saga service and the admin service
    Set<String> exposed = new LinkedHashSet<>();
    for (MethodDescriptor<?, ?> method : SagaServiceGrpc.getServiceDescriptor().getMethods()) {
      exposed.add(method.getBareMethodName());
    }
    for (MethodDescriptor<?, ?> method : AdminServiceGrpc.getServiceDescriptor().getMethods()) {
      exposed.add(method.getBareMethodName());
    }

    // Assert — every exposed method resolves to the expected operation
    assertThat(exposed)
        .as("every exposed gRPC method must have an expected operation")
        .containsExactlyInAnyOrderElementsOf(EXPECTED_GRPC_METHODS.keySet());
    for (String bareMethodName : exposed) {
      assertThat(GrpcOperations.fromBareMethodName(bareMethodName))
          .as("operation for gRPC method '%s'", bareMethodName)
          .isEqualTo(EXPECTED_GRPC_METHODS.get(bareMethodName));
    }

    // Assert — nothing is mapped that is not a real method (a stale mapping hides a rename)
    assertThat(GrpcOperations.mappedMethodNames())
        .as("no mapped gRPC method may be absent from the service descriptor")
        .containsExactlyInAnyOrderElementsOf(exposed);
  }

  @Test
  void grpcMethod_unmappedGiven_resolvesToNoOperation() {
    // Assert — an unmapped or absent method has no policy, so its interceptor refuses it rather
    // than defaulting it to a role
    assertThat(GrpcOperations.fromBareMethodName("NoSuchMethod")).isNull();
    assertThat(GrpcOperations.fromBareMethodName(null)).isNull();
  }

  @Test
  void restRoutes_allCarryTheExpectedOperation() {
    // Arrange — register the real routes, exactly as SagaServer does
    Javalin app = Javalin.create();
    DefaultSagaOrchestrator orchestrator = mock(DefaultSagaOrchestrator.class);
    HealthResource.register(app);
    SagaResource.register(app, mock(SagaOrchestrator.class), 0L);
    SagaAdminResource.register(app, orchestrator, 0L);
    CallbackResource.register(app, orchestrator, "test-secret", 0L, Clock.systemUTC());

    // Act — enumerate what was actually registered, skipping the before/after handlers
    Map<String, SagaOperation> registered = new HashMap<>();
    Set<String> untagged = new LinkedHashSet<>();
    for (Endpoint endpoint : registeredHttpEndpoints(app)) {
      String route = endpoint.getMethod() + " " + endpoint.getPath();
      SagaOperation operation = operationOf(endpoint.getRoles());
      if (operation == null) {
        untagged.add(route);
      } else {
        registered.put(route, operation);
      }
    }

    // Assert — no route may be registered without an operation. Such a route is refused at runtime
    // (fail closed), so this assertion is what turns a forgotten tag into a build failure rather
    // than a dead endpoint discovered in production.
    assertThat(untagged).as("every registered REST route must declare a SagaOperation").isEmpty();
    assertThat(registered)
        .as("every REST route must carry the expected operation")
        .containsExactlyInAnyOrderEntriesOf(EXPECTED_REST_ROUTES);
  }

  /**
   * The app's registered HTTP-verb endpoints, excluding the before/after filter handlers. Reads the
   * router straight off the config rather than through {@code javalinServlet()}, whose concrete
   * type depends on which server backs the app.
   */
  private static Iterable<Endpoint> registeredHttpEndpoints(Javalin app) {
    Set<Endpoint> endpoints = new LinkedHashSet<>();
    app.unsafeConfig()
        .pvt
        .internalRouter
        .allHttpHandlers()
        .forEach(
            parsed -> {
              Endpoint endpoint = parsed.getEndpoint();
              if (endpoint.getMethod().isHttpMethod()) {
                endpoints.add(endpoint);
              }
            });
    return endpoints;
  }

  private static @Nullable SagaOperation operationOf(Set<RouteRole> roles) {
    for (RouteRole role : roles) {
      if (role instanceof SagaOperation operation) {
        return operation;
      }
    }
    return null;
  }
}
