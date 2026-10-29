/**
 * The Java 8 client SDK for a ScalarDB Saga server, speaking gRPC.
 *
 * <p>Two entry points, one per audience. {@link com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient}
 * is the application client: it implements {@link com.scalar.db.saga.api.SagaOrchestrator}, so code
 * that starts and inspects sagas runs unchanged against the embedded orchestrator or a remote
 * server. {@link com.scalar.db.saga.grpc.GrpcSagaAdminClient} is the operator client: it implements
 * {@link com.scalar.db.saga.api.SagaAdminService} and needs the {@code saga:admin} role.
 *
 * <p>Both are built from a target address, optionally with TLS, a private CA, an authority override
 * and a default deadline; each holds one channel and is meant to be created once, shared, and
 * closed. Failures arrive as the same {@code com.scalar.db.saga.exception} types and {@link
 * com.scalar.db.saga.exception.SagaErrorCode}s the embedded engine throws, plus the transport-level
 * ones that only exist over a network.
 */
@NullMarked
package com.scalar.db.saga.grpc;

import org.jspecify.annotations.NullMarked;
