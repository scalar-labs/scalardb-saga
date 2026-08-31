package com.scalar.db.saga.server;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Configuration for the services directory and its reload behavior, mirroring {@code
 * RecoveryConfig}/{@code RetentionConfig} (one record per background concern, carrying the {@link
 * Clock}).
 *
 * <p>{@code servicesPath} feeds boot-time service loading and every reload pass; {@code
 * secretsRoot}/{@code allowedHostsCeiling} bound what a service file may reference and authorize;
 * {@code intervalSeconds} paces {@code SagaConfigReloadManager}'s passes, and {@code 0} disables
 * reload entirely (startup-only loading).
 *
 * @param servicesPath directory of per-service {@code <name>.properties} files, or {@code null}
 *     when no services are configured
 * @param intervalSeconds seconds between reload passes; {@code 0} disables reload (startup-only
 *     loading)
 * @param secretsRoot directory that {@code ${file:...}} references in service files must resolve
 *     inside (after symlink resolution); see {@link ServiceSecretResolver}
 * @param allowedHostsCeiling optional operator ceiling: when non-empty, every service's {@code
 *     allowed_hosts} must be a non-empty subset of it, so no service file can authorize egress
 *     beyond what the operator allowed
 * @param clock the clock the reload pass will schedule and stamp against
 */
public record ReloadConfig(
    @Nullable Path servicesPath,
    long intervalSeconds,
    Path secretsRoot,
    List<String> allowedHostsCeiling,
    Clock clock) {

  public ReloadConfig {
    // No requireNonNull here: this record is internal to the unpublished server module, where
    // @NullMarked + NullAway carry the null contract (unlike core's RecoveryConfig/RetentionConfig,
    // whose callers may not be compiled with NullAway).
    if (intervalSeconds < 0) {
      throw new IllegalArgumentException("intervalSeconds must be >= 0, got " + intervalSeconds);
    }
    allowedHostsCeiling = List.copyOf(allowedHostsCeiling);
  }
}
