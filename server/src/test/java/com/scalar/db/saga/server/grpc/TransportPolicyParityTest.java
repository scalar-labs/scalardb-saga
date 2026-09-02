package com.scalar.db.saga.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.server.api.CallbackResource;
import com.scalar.db.saga.server.api.HealthResource;
import com.scalar.db.saga.server.api.SagaAdminResource;
import com.scalar.db.saga.server.api.SagaResource;
import com.scalar.db.saga.server.security.SagaOperation;
import com.scalar.db.saga.server.security.SagaRole;
import io.grpc.MethodDescriptor;
import io.javalin.Javalin;
import io.javalin.router.Endpoint;
import io.javalin.security.RouteRole;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
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
              Map.entry(SagaOperation.LIST_SAGAS, new ExpectedPolicy(SagaRole.ADMIN, false)),
              Map.entry(SagaOperation.GET_SAGA_DETAIL, new ExpectedPolicy(SagaRole.READ, false)),
              Map.entry(SagaOperation.RECOVER_SAGA, new ExpectedPolicy(SagaRole.ADMIN, false)),
              Map.entry(SagaOperation.FORCE_COMPLETE, new ExpectedPolicy(SagaRole.ADMIN, false)),
              Map.entry(SagaOperation.RESET_ESCALATED, new ExpectedPolicy(SagaRole.ADMIN, false))));

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
          Map.entry("HEAD /health", SagaOperation.HEALTH),
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
    SagaResource.register(
        app, mock(SagaOrchestrator.class), 0L, new java.util.concurrent.CompletableFuture<>());
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

  /**
   * No code path in {@link SagaResource} may call a {@link SagaOrchestrator} {@code start}
   * overload.
   *
   * <p>Asserted structurally, by reading the compiled class's constant pool, rather than by driving
   * the routes: a behavioural check only covers the routes a test happens to exercise, so a newly
   * added start route would pass it while reintroducing the defect. This is the guard {@code
   * docs/plans/2026-08-30-001-...-plan.md} asked for and {@code todos/082} recorded as missing.
   *
   * <p>Why it matters: the synchronous {@code start} overloads run the entire saga on the calling
   * thread, which on this layer is a request thread. That was the P1 in {@code todos/076} — ~200
   * concurrent slow sagas exhausted the Jetty pool. Every REST start must go through {@code
   * startAsync} with a bound. {@code start} remains correct API for embedded callers, which is why
   * it still exists and why this guard is scoped to this one class.
   */
  @Test
  void sagaResource_containsNoReferenceToASynchronousStartOverload() throws Exception {
    // Arrange — the compiled form of the class, including its lambdas (route handlers are compiled
    // into SagaResource$$Lambda bodies as private synthetic methods of the same class file).
    byte[] classFile;
    try (InputStream in =
        SagaResource.class.getResourceAsStream(SagaResource.class.getSimpleName() + ".class")) {
      classFile = Objects.requireNonNull(in, "SagaResource.class not found").readAllBytes();
    }

    // Act — collect every method name this class file references on SagaOrchestrator.
    Set<String> orchestratorCalls = referencedMethodsOn(classFile, SagaOrchestrator.class);

    // Assert — startAsync is expected; start is the regression.
    assertThat(orchestratorCalls)
        .as(
            "SagaResource must not call SagaOrchestrator.start(...) — it runs the saga on the "
                + "request thread. Use startAsync with a bound (todos/076).")
        .doesNotContain("start");
    assertThat(orchestratorCalls)
        .as("sanity: the scan must actually be seeing the orchestrator calls it inspects")
        .contains("startAsync");
  }

  /**
   * Returns the method names {@code classFile} references on {@code owner}, read from its constant
   * pool. Lambdas do not hide a call from this: a lambda body compiles to a synthetic method of the
   * same class, so its calls land in the same constant pool.
   *
   * <p>The pool has to be walked in full rather than scanned for a string, because entries are
   * variable-length and {@code Long}/{@code Double} occupy two slots each — a naive scan
   * desynchronises. Only {@code Methodref} (10) and {@code InterfaceMethodref} (11) encode a call;
   * each points at a {@code Class} entry for the owner and a {@code NameAndType} entry for the
   * method, both of which resolve to {@code Utf8} entries.
   */
  private static Set<String> referencedMethodsOn(byte[] classFile, Class<?> owner)
      throws IOException {
    Map<Integer, String> utf8 = new HashMap<>();
    Map<Integer, Integer> classNameIndex = new HashMap<>();
    Map<Integer, int[]> memberRefs = new HashMap<>(); // -> {classIndex, nameAndTypeIndex}
    Map<Integer, Integer> nameIndexOfNameAndType = new HashMap<>();

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(classFile))) {
      in.readInt(); // magic
      in.readUnsignedShort(); // minor
      in.readUnsignedShort(); // major
      int count = in.readUnsignedShort();
      for (int i = 1; i < count; i++) {
        int tag = in.readUnsignedByte();
        switch (tag) {
          case 1 -> utf8.put(i, in.readUTF());
          case 7, 8, 16, 19, 20 -> {
            int index = in.readUnsignedShort();
            if (tag == 7) {
              classNameIndex.put(i, index);
            }
          }
          case 9, 10, 11, 17, 18 -> {
            int first = in.readUnsignedShort();
            int second = in.readUnsignedShort();
            if (tag == 10 || tag == 11) {
              memberRefs.put(i, new int[] {first, second});
            }
          }
          case 12 -> {
            nameIndexOfNameAndType.put(i, in.readUnsignedShort());
            in.readUnsignedShort(); // descriptor
          }
          case 3, 4 -> in.readInt();
          case 5, 6 -> {
            in.readLong();
            i++; // long and double take two constant-pool slots
          }
          case 15 -> {
            in.readUnsignedByte();
            in.readUnsignedShort();
          }
          default -> throw new IOException("unknown constant pool tag " + tag + " at " + i);
        }
      }
    }

    String ownerInternalName = owner.getName().replace('.', '/');
    Set<String> methods = new LinkedHashSet<>();
    for (int[] ref : memberRefs.values()) {
      String refOwner = utf8.get(classNameIndex.get(ref[0]));
      if (ownerInternalName.equals(refOwner)) {
        methods.add(utf8.get(nameIndexOfNameAndType.get(ref[1])));
      }
    }
    return methods;
  }
}
