package com.scalar.db.saga.server;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Configuration for the services directory and its reload behavior, mirroring {@code
 * RecoveryConfig}/{@code RetentionConfig} (one record per background concern, carrying the {@link
 * Clock}).
 *
 * <p>In this phase only the static half is consumed: {@code servicesPath} feeds boot-time service
 * loading, and {@code secretsRoot}/{@code allowedHostsCeiling} bound what a service file may
 * reference and authorize. {@code intervalSeconds} is parsed and documented now so the surface is
 * complete, and takes effect when the reload pass ships; {@code 0} disables reload entirely
 * (startup-only loading, today's behavior).
 *
 * @param servicesPath directory of per-service {@code <name>.properties} files, or {@code null}
 *     when no services are configured
 * @param intervalSeconds seconds between reload passes; {@code 0} disables reload
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
    Objects.requireNonNull(secretsRoot, "secretsRoot must not be null");
    Objects.requireNonNull(clock, "clock must not be null");
    if (intervalSeconds < 0) {
      throw new IllegalArgumentException("intervalSeconds must be >= 0, got " + intervalSeconds);
    }
    allowedHostsCeiling = List.copyOf(allowedHostsCeiling);
  }
}
