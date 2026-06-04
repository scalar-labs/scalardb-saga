/**
 * Crash recovery: periodic scan of stale sagas, event replay, and resume.
 *
 * <p>The recovery manager is an internal component started by the saga engine. It is <b>not</b>
 * part of the public API.
 */
@NullMarked
package com.scalar.db.saga.recovery;

import org.jspecify.annotations.NullMarked;
