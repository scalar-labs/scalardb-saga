package com.scalar.db.saga.api;

import net.jcip.annotations.Immutable;

/**
 * The declarative communication spec for one direction (forward or compensating) of a declarative
 * {@code ServiceStep} (Layer 2b). A call spec declares — as persistable data, with no Java code —
 * how to build a request to a remote service and how to extract its response back into the saga
 * context.
 *
 * <p>This is a sealed transport-tagged hierarchy: each subtype carries exactly the addressing its
 * transport needs, and {@link #transport()} is the discriminator persisted with the definition.
 *
 * <ul>
 *   <li>{@link HttpCall} — HTTP verb + URL path/query templating + body/output mappings.
 * </ul>
 *
 * <p>A gRPC subtype is added in Task 2.1b; the engine and store treat all subtypes uniformly via
 * {@link #transport()}.
 *
 * <p>Expression syntax shared by all subtypes:
 *
 * <ul>
 *   <li>{@code ${key}} in a request/path/query value — substitutes the saga context value for
 *       {@code key}. A value with no {@code ${...}} is passed through literally.
 *   <li>{@code $.field} in an output value — extracts a field from the service response.
 * </ul>
 */
@Immutable
public abstract sealed class CallSpec permits HttpCall {

  /**
   * The wire transport a {@link CallSpec} uses. Derived from the concrete subtype (e.g. {@link
   * HttpCall} → {@link #HTTP}); it is the discriminator persisted with the definition so the
   * correct subtype is reconstructed on reload.
   */
  public enum Transport {
    /** HTTP/1.1 or HTTP/2 via {@link HttpCall}. */
    HTTP,
    /** gRPC — declarative support is added in Task 2.1b; definitions may parse but not yet run. */
    GRPC
  }

  // Package-private constructor: the permitted subtypes all live in this package.
  CallSpec() {}

  /** The wire transport this call uses — the discriminator persisted with the definition. */
  public abstract Transport transport();
}
