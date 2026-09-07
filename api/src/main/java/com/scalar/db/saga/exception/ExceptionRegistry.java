package com.scalar.db.saga.exception;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
 * wire metadata that doesn't satisfy the code's schema (protocol drift) never shims the exception
 * into a partial state: {@link #tryReconstruct} reports the condition as an empty result, and the
 * caller classifies some other way — the gRPC client falls back to transport-status dispatch, which
 * reads the status family the server did set correctly.
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
        SagaErrorCode.INVALID_STEP_DEFINITION,
        meta -> SagaDefinitionException.fromWire(SagaErrorCode.INVALID_STEP_DEFINITION, meta));
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
        SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER,
        meta ->
            SagaDefinitionException.fromWire(
                SagaErrorCode.STEP_CLASS_NOT_SUPPORTED_ON_SERVER, meta));
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
        meta -> SagaDefinitionNotFoundException.byName(meta.get("saga_name")));
    m.put(
        SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND,
        meta ->
            SagaDefinitionNotFoundException.byNameAndVersion(
                meta.get("saga_name"), meta.get("version")));
    // REST-only (the unmatched-route 404); no dedicated exception type, so it round-trips raw.
    m.put(SagaErrorCode.ENDPOINT_NOT_FOUND, meta -> raw(SagaErrorCode.ENDPOINT_NOT_FOUND, meta));

    // ── USER_ERROR — Conflict (103xx) ─────────────────────────────────
    // SAGA_ALREADY_EXISTS is intentionally NOT here: SagaAlreadyExistsException requires the
    // existing snapshot, which isn't in the wire metadata. The client re-fetches the snapshot
    // itself and constructs the typed exception directly (see GrpcSagaOrchestratorClient).
    m.put(SagaErrorCode.SAGA_ALREADY_EXISTS, meta -> raw(SagaErrorCode.SAGA_ALREADY_EXISTS, meta));
    m.put(
        SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT,
        meta ->
            SagaDefinitionException.fromWire(
                SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT, meta));

    // ── USER_ERROR — Precondition (104xx) ─────────────────────────────
    m.put(
        SagaErrorCode.SAGA_WRONG_STATE,
        meta -> SagaStatePreconditionException.fromWire(SagaErrorCode.SAGA_WRONG_STATE, meta));
    m.put(
        SagaErrorCode.SAGA_PARKED,
        meta -> SagaStatePreconditionException.fromWire(SagaErrorCode.SAGA_PARKED, meta));
    m.put(SagaErrorCode.SAGA_DEFINITION_NOT_SERVED, SagaDefinitionNotServedException::fromWire);

    // ── RETRYABLE_SERVER_ERROR (2xxxx) ────────────────────────────────
    m.put(
        SagaErrorCode.SAGA_CONCURRENT_MODIFICATION,
        meta -> new SagaConcurrentModificationException(meta.get("saga_id")));
    // Reconstructs as SagaPersistenceException, not SagaUnavailableException: the latter hardcodes
    // SERVICE_UNAVAILABLE, so substituting it silently rewrote the code (and the rendered message)
    // from DB-SAGA-20002 to DB-SAGA-20003 and dropped isRetryable(). A remote caller keying on
    // SagaUnavailableException alone no longer sees this one — key on
    // Category.RETRYABLE_SERVER_ERROR, which covers both.
    m.put(
        SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE,
        meta ->
            SagaPersistenceException.fromWire(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE, meta));
    m.put(SagaErrorCode.SERVICE_UNAVAILABLE, meta -> new SagaUnavailableException());
    m.put(SagaErrorCode.RATE_LIMIT_EXCEEDED, meta -> raw(SagaErrorCode.RATE_LIMIT_EXCEEDED, meta));
    m.put(
        SagaErrorCode.OPERATION_ABORTED,
        meta -> SagaPersistenceException.fromWire(SagaErrorCode.OPERATION_ABORTED, meta));
    // The client-side unknown-but-retryable sentinel, sibling of UNRECOGNIZED_SERVER_ERROR below;
    // registered defensively so a server that echoes it still round-trips.
    m.put(
        SagaErrorCode.UNRECOGNIZED_RETRYABLE_SERVER_ERROR,
        meta -> raw(SagaErrorCode.UNRECOGNIZED_RETRYABLE_SERVER_ERROR, meta));

    // ── NON_RETRYABLE_SERVER_ERROR (3xxxx) ────────────────────────────
    // Persistence serialization/deserialization reconstruct as the real SagaPersistenceException,
    // so catch (SagaPersistenceException) and isRetryable() behave the same remote as embedded.
    //
    // The step-level codes are never top-level RPC errors: a step failure is caught by the engine,
    // recorded as an event, and compensated, so it reaches a caller as saga state rather than as a
    // thrown exception. COMPENSATION_FAILED is the only one the engine actually attaches today;
    // STEP_TIMEOUT and STEP_USER_FAILURE are reserved and not yet produced anywhere (see
    // todos/032). All three keep a raw reconstructor so that a code echoed by a future or newer
    // server still round-trips instead of degrading.
    m.put(
        SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED,
        meta ->
            SagaPersistenceException.fromWire(
                SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED, meta));
    m.put(
        SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED,
        meta ->
            SagaPersistenceException.fromWire(
                SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED, meta));
    m.put(SagaErrorCode.STEP_TIMEOUT, meta -> raw(SagaErrorCode.STEP_TIMEOUT, meta));
    m.put(SagaErrorCode.STEP_USER_FAILURE, meta -> raw(SagaErrorCode.STEP_USER_FAILURE, meta));
    m.put(SagaErrorCode.COMPENSATION_FAILED, meta -> raw(SagaErrorCode.COMPENSATION_FAILED, meta));
    m.put(SagaErrorCode.INTERNAL_ERROR, meta -> raw(SagaErrorCode.INTERNAL_ERROR, meta));

    // ── CLIENT_ERROR (4xxxx) ──────────────────────────────────────────
    // These are produced client-side and should not appear on the wire; register defensive
    // reconstructors so a server that echoes one still round-trips cleanly.
    m.put(SagaErrorCode.SERVER_UNREACHABLE, meta -> raw(SagaErrorCode.SERVER_UNREACHABLE, meta));
    m.put(
        SagaErrorCode.UNMAPPED_SERVER_STATUS,
        meta -> raw(SagaErrorCode.UNMAPPED_SERVER_STATUS, meta));
    m.put(SagaErrorCode.REQUEST_TIMEOUT, meta -> raw(SagaErrorCode.REQUEST_TIMEOUT, meta));
    m.put(SagaErrorCode.SAGA_AWAIT_TIMEOUT, meta -> raw(SagaErrorCode.SAGA_AWAIT_TIMEOUT, meta));
    m.put(SagaErrorCode.REQUEST_ABORTED, meta -> raw(SagaErrorCode.REQUEST_ABORTED, meta));
    m.put(
        SagaErrorCode.UNRECOGNIZED_SERVER_ERROR,
        meta -> raw(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR, meta));

    REGISTRY = m;
  }

  private ExceptionRegistry() {}

  /**
   * Reconstructs the typed exception the server would have thrown, given the wire code and
   * metadata, or returns empty when this registry cannot: the code is unknown (server newer than
   * this client), unregistered, or the metadata doesn't satisfy the code's schema (protocol drift).
   * Empty means "classify some other way", not "no error" — the gRPC client answers it with
   * transport-status dispatch.
   */
  @SuppressFBWarnings(
      value = "DCN_NULLPOINTER_EXCEPTION",
      justification =
          "Catching NPE is intentional: some typed exception ctors call Objects.requireNonNull on"
              + " metadata values (e.g. SagaNotFoundException(saga_id)), so a missing wire key"
              + " surfaces as NPE rather than the IllegalArgumentException that ErrorMetadataSchema.validate"
              + " throws. Both indicate the same protocol-drift condition and degrade the same"
              + " way.")
  public static Optional<SagaRuntimeException> tryReconstruct(
      String wireCode, Map<String, String> metadata) {
    SagaErrorCode code = SagaErrorCode.fromCode(wireCode).orElse(null);
    if (code == null) {
      return Optional.empty();
    }
    Reconstructor r = REGISTRY.get(code);
    if (r == null) {
      // Every enum entry SHOULD be registered above; this is a defensive catch for a code that
      // exists in the enum but was missed here (a bug we prefer to degrade rather than crash).
      return Optional.empty();
    }
    try {
      return Optional.of(r.reconstruct(retainDeclaredKeys(code, metadata)));
    } catch (IllegalArgumentException | NullPointerException schemaMismatch) {
      // Wire metadata doesn't satisfy the code's schema — protocol drift; degrade.
      return Optional.empty();
    }
  }

  /**
   * Drops wire metadata keys the code's schema does not declare, so an enriched newer server still
   * reconstructs typed on an older client: the schema evolution rule is gain-only (see {@link
   * SagaErrorCode}), and an unknown key is dropped like an unknown proto field. A missing declared
   * key is genuine drift and still fails the constructor's validation. The equal-size shortcut is
   * safe: when nothing was added, filtering could only reproduce the map (matching keys) or leave a
   * mismatch the validation rejects either way (renamed keys).
   */
  private static Map<String, String> retainDeclaredKeys(
      SagaErrorCode code, Map<String, String> metadata) {
    List<String> declared = code.schema().requiredKeys();
    if (metadata.size() == declared.size()) {
      return metadata;
    }
    Map<String, String> filtered = new LinkedHashMap<>();
    for (String key : declared) {
      String value = metadata.get(key);
      if (value != null) {
        filtered.put(key, value);
      }
    }
    return filtered;
  }

  private static SagaRuntimeException raw(SagaErrorCode code, Map<String, String> metadata) {
    return new SagaRuntimeException(code, metadata);
  }
}
