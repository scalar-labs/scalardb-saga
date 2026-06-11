/**
 * Layer 2 service invocation: register typed lambdas ({@link
 * com.scalar.db.saga.invoker.HttpServiceInvoker}) under a service name instead of writing {@code
 * Step} classes. Built-in HTTP support propagates the saga context, classifies status codes, and
 * enforces outbound HTTP policy.
 */
@NullMarked
package com.scalar.db.saga.invoker;

import org.jspecify.annotations.NullMarked;
