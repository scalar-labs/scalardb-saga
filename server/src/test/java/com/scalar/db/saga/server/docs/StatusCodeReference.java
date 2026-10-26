package com.scalar.db.saga.server.docs;

import com.scalar.db.saga.exception.SagaErrorCode;
import io.grpc.Status;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Where each {@link SagaErrorCode} reaches a user, for the generated status-code page. The wire
 * statuses are the ones a real route or RPC answers with, which is narrower than what the error
 * mappers could assign: no route registers a definition, for example, so the definition codes have
 * none even though the mappers would answer 400 for them. {@code ErrorMapperTest} and {@code
 * GrpcErrorMapperTest} check that every status listed here is one the mapper produces for that
 * code, so a mapper change cannot leave the page claiming a status the server no longer sends.
 */
public final class StatusCodeReference {

  /**
   * One code's entry.
   *
   * @param http the HTTP statuses the REST API answers with, empty when REST never returns the code
   * @param grpc the gRPC statuses the gRPC API answers with, empty when gRPC never returns the code
   * @param note a sentence on where else, or under which condition, the code appears; required when
   *     neither transport returns it
   */
  public record Entry(List<Integer> http, List<Status.Code> grpc, @Nullable String note) {
    public Entry {
      http = List.copyOf(http);
      grpc = List.copyOf(grpc);
    }
  }

  /** Codes nothing produces yet. Their numbers are taken, but no user can meet them. */
  public static final Set<SagaErrorCode> RESERVED =
      EnumSet.of(
          SagaErrorCode.STEP_TIMEOUT,
          SagaErrorCode.STEP_USER_FAILURE,
          // The client SDK reports an unreachable server as SERVICE_UNAVAILABLE instead.
          SagaErrorCode.SERVER_UNREACHABLE);

  // These codes fail the pass in validation, before anything is applied, so the server never
  // serves part of it.
  private static final String REJECTS_CONFIGURATION =
      " the server logs this code and rejects the whole configuration: at startup the server does"
          + " not start, and on a reload it keeps serving its previous configuration.";

  private static final String DEFINITION_LOAD =
      "Not returned by the API. When loading its definitions,"
          + REJECTS_CONFIGURATION
          + " An embedded engine throws it when the definition is registered.";

  private static final String CLIENT_SDK = "Not sent by the server. The Java client SDK raises it.";

  public static final Map<SagaErrorCode, Entry> ENTRIES = build();

  private static Map<SagaErrorCode, Entry> build() {
    Map<SagaErrorCode, Entry> m = new EnumMap<>(SagaErrorCode.class);
    m.put(
        SagaErrorCode.INVALID_REQUEST,
        wire(
            List.of(400, 413),
            Status.Code.INVALID_ARGUMENT,
            "The participant callback routes answer `413` for an oversized body."));
    m.put(
        SagaErrorCode.INVALID_ARGUMENT,
        wire(
            400,
            Status.Code.INVALID_ARGUMENT,
            "The Java client SDK also raises it before sending a request it can check itself."));
    m.put(SagaErrorCode.INVALID_DEFINITION, other(DEFINITION_LOAD));
    m.put(SagaErrorCode.MALFORMED_DEFINITION, other(DEFINITION_LOAD));
    m.put(
        SagaErrorCode.UNREADABLE_DEFINITION_SOURCE,
        other(
            "Embedded mode only. The engine throws it when a definition is read from a file or a"
                + " classpath resource."));
    m.put(
        SagaErrorCode.INVALID_STEP_CLASS,
        other(
            "Embedded mode only. The engine throws it when a definition with a class-based step is"
                + " registered."));
    m.put(
        SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER,
        other(
            "Not returned by the API. When a definition has a class-based step,"
                + REJECTS_CONFIGURATION));
    m.put(
        SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED,
        other(
            "Embedded mode only. A custom step resolver throws it when it cannot find the HTTP"
                + " endpoint it asks for."));
    // An operator's recover or reset drives the saga on the request. After a restart that can be
    // the first plan build for the saga's definition version, which fails here when the version
    // names a service the server no longer has.
    m.put(
        SagaErrorCode.INVALID_STEP_DEFINITION,
        wire(
            400,
            Status.Code.INVALID_ARGUMENT,
            "Returned only by an operator's recover or reset, when the saga's definition version"
                + " names a service the server no longer has. Otherwise, when loading its"
                + " definitions,"
                + REJECTS_CONFIGURATION
                + " An embedded engine throws it when the definition is registered."));
    m.put(SagaErrorCode.UNAUTHENTICATED, wire(401, Status.Code.UNAUTHENTICATED, null));
    m.put(SagaErrorCode.PERMISSION_DENIED, wire(403, Status.Code.PERMISSION_DENIED, null));
    m.put(SagaErrorCode.SAGA_NOT_FOUND, wire(404, Status.Code.NOT_FOUND, null));
    m.put(SagaErrorCode.SAGA_DEFINITION_NOT_FOUND, wire(404, Status.Code.NOT_FOUND, null));
    m.put(SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND, wire(404, Status.Code.NOT_FOUND, null));
    m.put(
        SagaErrorCode.ENDPOINT_NOT_FOUND,
        new Entry(
            List.of(404),
            List.of(),
            "REST only. An unknown gRPC method gets a bare `UNIMPLEMENTED` instead."));
    m.put(SagaErrorCode.SAGA_ALREADY_EXISTS, wire(409, Status.Code.ALREADY_EXISTS, null));
    // Raised while registering, after validation and the service swap, so unlike the codes above
    // it can leave part of a reload applied.
    m.put(
        SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT,
        other(
            "Not returned by the API. The server logs it while registering definitions. At startup"
                + " the server does not start. On a reload, the conflicting definition is not"
                + " applied, but other parts of the change, such as service changes and other"
                + " definitions, may already have taken effect. An embedded engine throws it when"
                + " the definition is registered."));
    m.put(SagaErrorCode.SAGA_WRONG_STATE, wire(422, Status.Code.FAILED_PRECONDITION, null));
    m.put(SagaErrorCode.SAGA_PARKED, wire(422, Status.Code.FAILED_PRECONDITION, null));
    m.put(
        SagaErrorCode.SAGA_DEFINITION_NOT_SERVED, wire(422, Status.Code.FAILED_PRECONDITION, null));
    m.put(SagaErrorCode.SAGA_CONCURRENT_MODIFICATION, wire(409, Status.Code.ABORTED, null));
    m.put(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE, wire(503, Status.Code.UNAVAILABLE, null));
    m.put(
        SagaErrorCode.SERVICE_UNAVAILABLE,
        wire(
            503,
            Status.Code.UNAVAILABLE,
            "The Java client SDK also raises it when it cannot reach the server."));
    m.put(
        SagaErrorCode.RATE_LIMIT_EXCEEDED,
        wire(
            429,
            Status.Code.RESOURCE_EXHAUSTED,
            "Returned only for saga starts, when a start rate limit is configured."));
    m.put(SagaErrorCode.OPERATION_ABORTED, wire(503, Status.Code.UNAVAILABLE, null));
    m.put(
        SagaErrorCode.ENGINE_OVERLOADED,
        wire(
            503,
            Status.Code.UNAVAILABLE,
            "Returned only for saga starts, when a concurrent start limit is configured."));
    m.put(
        SagaErrorCode.SAGA_STEP_NOT_PARKED,
        new Entry(
            List.of(503),
            List.of(),
            "Returned only by the participant callback routes, which are REST only."));
    m.put(SagaErrorCode.UNRECOGNIZED_RETRYABLE_SERVER_ERROR, other(CLIENT_SDK));
    m.put(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED, wire(500, Status.Code.INTERNAL, null));
    m.put(SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED, wire(500, Status.Code.INTERNAL, null));
    m.put(
        SagaErrorCode.COMPENSATION_FAILED,
        other(
            "Not returned by the API. The server, or an embedded engine, logs it when a"
                + " compensation fails, and recovery retries the compensation."));
    m.put(SagaErrorCode.INTERNAL_ERROR, wire(500, Status.Code.INTERNAL, null));
    m.put(SagaErrorCode.REQUEST_TIMEOUT, other(CLIENT_SDK));
    m.put(SagaErrorCode.REQUEST_ABORTED, other(CLIENT_SDK));
    m.put(SagaErrorCode.UNMAPPED_SERVER_STATUS, other(CLIENT_SDK));
    m.put(SagaErrorCode.SAGA_AWAIT_TIMEOUT, other(CLIENT_SDK));
    m.put(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR, other(CLIENT_SDK));
    return m;
  }

  private static Entry wire(int http, Status.Code grpc, @Nullable String note) {
    return wire(List.of(http), grpc, note);
  }

  private static Entry wire(List<Integer> http, Status.Code grpc, @Nullable String note) {
    return new Entry(http, List.of(grpc), note);
  }

  private static Entry other(String note) {
    return new Entry(List.of(), List.of(), note);
  }

  private StatusCodeReference() {}
}
