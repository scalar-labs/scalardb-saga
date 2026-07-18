package com.scalar.db.saga.daemon.grpc;

import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.OperatorContext;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.InterventionRequest;
import com.scalar.db.saga.rpc.ListSagasRequest;
import com.scalar.db.saga.rpc.ListSagasResponse;
import com.scalar.db.saga.rpc.ResetEscalatedBulkRequest;
import com.scalar.db.saga.rpc.ResetResult;
import com.scalar.db.saga.rpc.SagaSnapshot;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import net.jcip.annotations.ThreadSafe;

/**
 * The gRPC rendering of the saga operational control plane — the wire parallel of {@link
 * com.scalar.db.saga.daemon.api.SagaAdminResource} (REST). Stateless except for the injected {@link
 * DefaultSagaOrchestrator} (the same instance the REST routes and the saga service use).
 *
 * <p><b>Operator identity is server-injected.</b> Every mutation is attributed to the authenticated
 * caller, read from the gRPC {@link Context} where {@link SagaSecurityInterceptor} put it — never
 * from a request field. This service is meaningful only when interceptor-wrapped: a bare {@code
 * addService} would leave every destructive RPC unauthenticated with no identity on the context, so
 * a mutation with no resolved identity is refused (a wiring bug, surfaced as {@code INTERNAL}), not
 * served anonymously. Listing needs no operator, so it uses the orchestrator's embedded admin view.
 *
 * <p><b>Bounded drive.</b> A single-saga {@code RecoverSaga}/{@code ResetEscalated} drives the saga
 * inline. The drive is bounded by the daemon's admin drive deadline, further tightened by the
 * remaining call deadline (mirroring {@code SagaServiceImpl.computeBoundMillis}): past the bound
 * the durable transition is already recorded and the response carries the saga's current (possibly
 * still-running) state.
 */
@ThreadSafe
public final class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {

  /**
   * Slack subtracted from the call deadline when bounding the drive, so the server returns the
   * snapshot before gRPC cancels the call.
   */
  private static final long DEADLINE_SLACK_MILLIS = 100L;

  private final DefaultSagaOrchestrator orchestrator;
  private final long adminDriveDeadlineMillis;

  /**
   * @param orchestrator the orchestrator whose admin control plane the RPCs drive
   * @param adminDriveDeadlineMillis the daemon's standing bound on a single-saga inline drive
   *     (positive); tightened per call by the remaining gRPC deadline
   */
  public AdminServiceImpl(DefaultSagaOrchestrator orchestrator, long adminDriveDeadlineMillis) {
    this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator must not be null");
    this.adminDriveDeadlineMillis = adminDriveDeadlineMillis;
  }

  @Override
  public void listSagas(ListSagasRequest request, StreamObserver<ListSagasResponse> observer) {
    try {
      // A read: no operator, no drive, so the embedded admin view serves it.
      observer.onNext(
          ProtoMappers.toProto(
              orchestrator.adminService().listSagas(ProtoMappers.toSagaQuery(request))));
      observer.onCompleted();
    } catch (RuntimeException e) {
      observer.onError(GrpcErrorMapper.toStatusRuntimeException(e));
    }
  }

  @Override
  public void recoverSaga(InterventionRequest request, StreamObserver<SagaSnapshot> observer) {
    respondWith(observer, admin -> admin.recoverSaga(request.getSagaId(), request.getReason()));
  }

  @Override
  public void forceComplete(InterventionRequest request, StreamObserver<SagaSnapshot> observer) {
    respondWith(observer, admin -> admin.forceComplete(request.getSagaId(), request.getReason()));
  }

  @Override
  public void resetEscalated(InterventionRequest request, StreamObserver<SagaSnapshot> observer) {
    respondWith(observer, admin -> admin.resetEscalated(request.getSagaId(), request.getReason()));
  }

  @Override
  public void resetEscalatedBulk(
      ResetEscalatedBulkRequest request, StreamObserver<ResetResult> observer) {
    try {
      SagaAdminService admin = admin();
      observer.onNext(
          ProtoMappers.toProto(
              admin.resetEscalated(ProtoMappers.toSagaQuery(request), request.getReason())));
      observer.onCompleted();
    } catch (RuntimeException e) {
      observer.onError(GrpcErrorMapper.toStatusRuntimeException(e));
    }
  }

  /**
   * Runs a single-saga admin call against the per-request admin view and responds with the snapshot
   * it returns, mapping any failure to a gRPC status — the shared build/respond/map flow the
   * snapshot-returning mutations share.
   */
  private void respondWith(
      StreamObserver<SagaSnapshot> observer, Function<SagaAdminService, SagaStateSnapshot> call) {
    try {
      observer.onNext(ProtoMappers.toProto(call.apply(admin())));
      observer.onCompleted();
    } catch (RuntimeException e) {
      observer.onError(GrpcErrorMapper.toStatusRuntimeException(e));
    }
  }

  /**
   * Builds the per-call admin view attributed to the authenticated caller, with the drive bound.
   */
  private SagaAdminService admin() {
    return orchestrator.adminService(operatorContext(), driveDeadlineMillis());
  }

  private OperatorContext operatorContext() {
    SagaIdentity identity = SagaSecurityInterceptor.IDENTITY.get();
    if (identity == null) {
      // No identity on the context: the admin service was reached without the security interceptor.
      // Refuse rather than attribute an intervention to nobody; surfaced as INTERNAL (a wiring
      // bug).
      throw new IllegalStateException(
          "no authenticated identity on an admin RPC; the service was not interceptor-wrapped");
    }
    return identity::principal;
  }

  /**
   * The drive bound for this call: the daemon's standing admin deadline, tightened by the remaining
   * gRPC call deadline (minus slack) when the caller set one — so a slow saga returns a snapshot
   * before the client's deadline fires, rather than the client seeing {@code DEADLINE_EXCEEDED}.
   */
  private long driveDeadlineMillis() {
    long bound = adminDriveDeadlineMillis;
    Deadline deadline = Context.current().getDeadline();
    if (deadline != null) {
      // Floor at 1ms, not 0. This mirrors SagaServiceImpl.computeBoundMillis, but 0 means opposite
      // things at the two call sites: there it feeds an await where 0 is "return immediately"; here
      // it feeds DefaultSagaAdminService where 0 or less means "unbounded, drive on the calling
      // thread." A tight or already-expired client deadline would otherwise clamp remaining to 0,
      // flipping the drive to unbounded on the gRPC request thread. Keeping it at least 1 keeps the
      // drive bounded; it times out fast and returns the current state.
      long remaining =
          Math.max(1L, deadline.timeRemaining(TimeUnit.MILLISECONDS) - DEADLINE_SLACK_MILLIS);
      bound = Math.min(bound, remaining);
    }
    return bound;
  }
}
