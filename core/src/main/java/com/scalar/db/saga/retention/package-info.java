/**
 * Data retention: periodic purge of resolved (terminal) sagas.
 *
 * <p>The retention manager is an internal component started by the saga engine. It is <b>not</b>
 * part of the public API.
 */
@NullMarked
package com.scalar.db.saga.retention;

import org.jspecify.annotations.NullMarked;
