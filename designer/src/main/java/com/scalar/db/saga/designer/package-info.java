/**
 * The saga definition designer: a local process that serves a browser editor for declarative saga
 * definitions, validates them with {@code SagaDefinitionParser}, and reads and writes them in a
 * workspace folder.
 *
 * <p>Everything here except {@link DesignerCommand} is internal to the tool. Its HTTP API serves
 * only the bundled UI, which ships in the same jar, so the API is not a wire contract and does not
 * use the server's error codes.
 */
@NullMarked
package com.scalar.db.saga.designer;

import org.jspecify.annotations.NullMarked;
