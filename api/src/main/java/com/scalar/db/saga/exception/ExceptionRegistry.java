package com.scalar.db.saga.exception;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.EnumMap;
import java.util.Map;

/**
 * Reconstructs a typed {@link SagaRuntimeException} from a wire-received error code + metadata. The
 * client SDK uses this to invert what the daemon's {@code GrpcErrorMapper} / {@code ErrorMapper}
 * put on the wire: a code string (from {@code ErrorInfo.reason} or the REST body's {@code
 * errorCode}) plus a metadata map (from {@code ErrorInfo.metadata} or the REST body's {@code
 * metadata}) → the same typed exception the server would have thrown.
 *
 * <p><b>Registry, not a switch.</b> One {@code (code → reconstructor)} entry per typed code that
 * can appear on the wire. Adding a new typed code registers one more entry; the mapper's call site
 * doesn't change. Codes with no dedicated exception type reconstruct as a raw {@link
 * SagaRuntimeException} carrying the same code and metadata.
 *
 * <p><b>Graceful degradation.</b> An unknown code (e.g. the server is newer than this client) or
 * wire metadata that doesn't satisfy the code's schema (protocol drift) degrades to {@link
 * SagaErrorCode#UNRECOGNIZED_SERVER_ERROR} carrying the raw wire code, rather than shimming the
 * exception into a partial state.
 *
 * <p><b>Not for every wire failure.</b> This handles the "server sent an ErrorInfo" path only. Wire
 * failures that carry no ErrorInfo (network error, deadline, unmapped gRPC status) route through
 * transport-specific mapping (see {@code GrpcClientSupport}).
 */
public final class ExceptionRegistry {

  @FunctionalInterface
  private interface Reconstructor {
    SagaRuntimeException reconstruct(Map<String, String> metadata);
  }

  private static final Map<SagaErrorCode, Reconstructor> REGISTRY;

  static {
    Map<SagaErrorCode, Reconstructor> m = new EnumMap<>(SagaErrorCode.class);

    // ── USER_ERROR — Bad request (100xx) ──────────────────────────────
    m.put(SagaErrorCode.INVALID_REQUEST, SagaInvalidRequestException::fromWire);
    m.put(SagaErrorCode.INVALID_ARGUMENT, SagaIllegalArgumentException::fromWire);
    m.put(
        SagaErrorCode.INVALID_DEFINITION,
        meta -> SagaDefinitionException.fromWire(SagaErrorCode.INVALID_DEFINITION, meta));
    m.put(
        SagaErrorCode.MALFORMED_DEFINITION,
        meta -> SagaDefinitionException.fromWire(SagaErrorCode.MALFORMED_DEFINITION, meta));
    m.put(
        SagaErrorCode.UNREADABLE_DEFINITION_SOURCE,
        meta -> SagaDefinitionException.fromWire(SagaErrorCode.UNREADABLE_DEFINITION_SOURCE, meta));
    m.put(
        SagaErrorCode.INVALID_STEP_CLASS,
        meta -> SagaDefinitionException.fromWire(SagaErrorCode.INVALID_STEP_CLASS, meta));
    m.put(
        SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_DAEMON,
        meta ->
            SagaDefinitionException.fromWire(
                SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_DAEMON, meta));
    m.put(
        SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED,
        meta -> SagaDefinitionException.fromWire(SagaErrorCode.HTTP_ENDPOINT_LOOKUP_FAILED, meta));

    // ── USER_ERROR — Auth (101xx) ─────────────────────────────────────
    m.put(SagaErrorCode.UNAUTHENTICATED, meta -> new SagaUnauthenticatedException());
    m.put(SagaErrorCode.PERMISSION_DENIED, meta -> new SagaPermissionDeniedException());

    // ── USER_ERROR — Not-found (102xx) ────────────────────────────────
    m.put(SagaErrorCode.SAGA_NOT_FOUND, meta -> new SagaNotFoundException(meta.get("saga_id")));
    m.put(
        SagaErrorCode.SAGA_DEFINITION_NOT_FOUND,
        meta -> new SagaDefinitionNotFoundException(meta.get("saga_name")));
    m.put(
        SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND,
        meta -> new SagaDefinitionNotFoundException(meta.get("saga_name"), meta.get("version")));

    // ── USER_ERROR — Conflict (103xx) ─────────────────────────────────
    // SAGA_ALREADY_EXISTS is intentionally NOT here: SagaAlreadyExistsException requires the
    // existing snapshot, which isn't in the wire metadata. The client re-fetches the snapshot
    // itself and constructs the typed exception directly (see GrpcSagaOrchestratorClient).
    m.put(SagaErrorCode.SAGA_ALREADY_EXISTS, meta -> raw(SagaErrorCode.SAGA_ALREADY_EXISTS, meta));
    m.put(
        SagaErrorCode.DEFINITION_VERSION_CONTENT_CONFLICT,
        meta ->
            SagaDefinitionException.fromWire(
                SagaErrorCode.DEFINITION_VERSION_CONTENT_CONFLICT, meta));

    // ── USER_ERROR — Precondition (104xx) ─────────────────────────────
    m.put(
        SagaErrorCode.SAGA_WRONG_STATE,
        meta -> SagaStatePreconditionException.fromWire(SagaErrorCode.SAGA_WRONG_STATE, meta));
    m.put(
        SagaErrorCode.SAGA_PARKED,
        meta -> SagaStatePreconditionException.fromWire(SagaErrorCode.SAGA_PARKED, meta));

    // ── RETRYABLE_SERVER_ERROR (2xxxx) ────────────────────────────────
    m.put(
        SagaErrorCode.SAGA_CONCURRENT_MODIFICATION,
        meta -> new SagaConcurrentModificationException(meta.get("saga_id")));
    m.put(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE, meta -> new SagaUnavailableException());
    m.put(SagaErrorCode.SERVICE_UNAVAILABLE, meta -> new SagaUnavailableException());
    m.put(SagaErrorCode.RATE_LIMIT_EXCEEDED, meta -> raw(SagaErrorCode.RATE_LIMIT_EXCEEDED, meta));

    // ── NON_RETRYABLE_SERVER_ERROR (3xxxx) ────────────────────────────
    // Persistence serialization/deserialization and step-level codes (STEP_TIMEOUT,
    // STEP_USER_FAILURE, COMPENSATION_FAILED) can appear only via saga state, not as top-level
    // RPC errors; they still get a raw reconstructor for defensive completeness.
    m.put(
        SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED,
        meta -> raw(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED, meta));
    m.put(
        SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED,
        meta -> raw(SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED, meta));
    m.put(SagaErrorCode.STEP_TIMEOUT, meta -> raw(SagaErrorCode.STEP_TIMEOUT, meta));
    m.put(SagaErrorCode.STEP_USER_FAILURE, meta -> raw(SagaErrorCode.STEP_USER_FAILURE, meta));
    m.put(SagaErrorCode.COMPENSATION_FAILED, meta -> raw(SagaErrorCode.COMPENSATION_FAILED, meta));
    m.put(SagaErrorCode.INTERNAL_ERROR, meta -> raw(SagaErrorCode.INTERNAL_ERROR, meta));

    // ── CLIENT_ERROR (4xxxx) ──────────────────────────────────────────
    // These are produced client-side and should not appear on the wire; register defensive
    // reconstructors so a server that echoes one still round-trips cleanly.
    m.put(SagaErrorCode.SERVER_UNREACHABLE, meta -> raw(SagaErrorCode.SERVER_UNREACHABLE, meta));
    m.put(SagaErrorCode.REQUEST_TIMEOUT, meta -> raw(SagaErrorCode.REQUEST_TIMEOUT, meta));
    m.put(SagaErrorCode.REQUEST_ABORTED, meta -> raw(SagaErrorCode.REQUEST_ABORTED, meta));
    m.put(
        SagaErrorCode.UNRECOGNIZED_SERVER_ERROR,
        meta -> raw(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR, meta));

    REGISTRY = m;
  }

  private ExceptionRegistry() {}

  /**
   * Reconstructs the typed exception the server would have thrown, given the wire code and
   * metadata. An unknown code, or metadata that doesn't satisfy the code's schema, degrades to
   * {@link SagaErrorCode#UNRECOGNIZED_SERVER_ERROR} carrying the raw wire code.
   */
  @SuppressFBWarnings(
      value = "DCN_NULLPOINTER_EXCEPTION",
      justification =
          "Catching NPE is intentional: some typed exception ctors call Objects.requireNonNull on"
              + " metadata values (e.g. SagaNotFoundException(saga_id)), so a missing wire key"
              + " surfaces as NPE rather than the IllegalArgumentException that Schema.validate"
              + " throws. Both indicate the same protocol-drift condition and degrade the same"
              + " way.")
  public static SagaRuntimeException reconstruct(String wireCode, Map<String, String> metadata) {
    SagaErrorCode code = SagaErrorCode.fromCode(wireCode).orElse(null);
    if (code == null) {
      return unrecognized(wireCode);
    }
    Reconstructor r = REGISTRY.get(code);
    if (r == null) {
      // Every enum entry SHOULD be registered above; this is a defensive catch for a code that
      // exists in the enum but was missed here (a bug we prefer to degrade rather than crash).
      return unrecognized(wireCode);
    }
    try {
      return r.reconstruct(metadata);
    } catch (IllegalArgumentException | NullPointerException schemaMismatch) {
      // Wire metadata doesn't satisfy the code's schema — protocol drift; degrade.
      return unrecognized(wireCode);
    }
  }

  private static SagaRuntimeException raw(SagaErrorCode code, Map<String, String> metadata) {
    return new SagaRuntimeException(code, metadata);
  }

  private static SagaRuntimeException unrecognized(String wireCode) {
    return new SagaRuntimeException(
        SagaErrorCode.UNRECOGNIZED_SERVER_ERROR,
        ErrorMetadata.of("server_value", wireCode == null ? "" : wireCode));
  }
}
