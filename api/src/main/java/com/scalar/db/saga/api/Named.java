package com.scalar.db.saga.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Qualifier annotation for disambiguating resources of the same type during step resolution.
 *
 * <p>When a step constructor has multiple parameters of the same type (e.g., two {@code
 * ManagedChannel} parameters), annotate each with {@code @Named} to indicate which registered
 * resource should be injected:
 *
 * <pre>{@code
 * public class TransferStep implements Step {
 *     public TransferStep(
 *         @Named("source") ManagedChannel sourceChannel,
 *         @Named("target") ManagedChannel targetChannel) {
 *         // ...
 *     }
 * }
 * }</pre>
 *
 * <p>When only one resource of a given type is registered, {@code @Named} is not required. This
 * same rule applies to a {@link SagaHttpClient} injected from {@code httpEndpoint(name, baseUrl)}:
 * the endpoint name is required only to disambiguate when two or more endpoints are registered.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Named {

  /** The qualifier name. Must match the name used when registering the resource. */
  String value();
}
