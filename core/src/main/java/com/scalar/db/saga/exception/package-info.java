/**
 * Exception hierarchy for the saga engine.
 *
 * <h2>Constructor convention</h2>
 *
 * An exception provides a {@code Throwable cause} constructor <b>only</b> when it can originate
 * from catching another exception (e.g., store-layer failures, executor timeouts). "Not-found"
 * exceptions that represent the absence of data ({@link SagaNotFoundException}, {@link
 * SagaDefinitionNotFoundException}) omit the cause constructor because there is no underlying
 * exception to wrap.
 */
@NullMarked
package com.scalar.db.saga.exception;

import org.jspecify.annotations.NullMarked;
