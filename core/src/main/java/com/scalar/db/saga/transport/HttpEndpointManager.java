package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * The single owner of the engine's HTTP endpoints and their lifecycle. It holds the current
 * endpoint set as an immutable snapshot behind a volatile reference, resolves declarative calls
 * against it per call (as the {@link TransportResolver}), and applies configuration swaps (as the
 * {@link HttpEndpointRegistrar}) with reuse / rotate / retire semantics — see the registrar's
 * javadoc for the contract. The engine-side {@code StepInstantiator} builds declarative steps
 * through it and serves code-step client lookups from its current snapshot.
 *
 * <p>Retirement is graceful and never blocks the swapping thread: a replaced or removed endpoint's
 * client is {@code shutdown()} (in-flight exchanges complete; new submissions fail pre-send as
 * retryable), then tracked on a retired list that is swept on every swap and drained with a bounded
 * wait at {@link #close()}. {@code HttpClient.close()} is never called — its Javadoc permits it to
 * block indefinitely on an abandoned streaming body.
 *
 * <p>Swaps and close are serialized on a private lock; resolution reads the volatile snapshot and
 * takes no lock.
 */
@ThreadSafe
public final class HttpEndpointManager
    implements TransportResolver, HttpEndpointRegistrar, AutoCloseable {

  /**
   * Bounded grace for in-flight exchanges at final close, before force-stopping the clients. By the
   * time the engine closes this manager its sagas have already drained per the shutdown mode, so
   * anything still in flight belongs to an abandoned drive and gets only a short courtesy window.
   */
  private static final Duration CLOSE_GRACE = Duration.ofSeconds(2);

  private final @Nullable CallbackUrlProvider callbackUrlProvider;

  // The current set, replaced wholesale by swapHttpEndpoints: an immutable map behind a volatile
  // read gives every resolution a consistent snapshot without locking (reads vastly outnumber
  // swaps, which happen at onboarding cadence). This is the ONLY state a swap diffs against: each
  // endpoint remembers its own immutable topology (HttpEndpoint.sameTopology), so there is no
  // parallel config map to keep consistent with it.
  private volatile Map<String, HttpEndpoint> endpoints;

  // Endpoints replaced or removed by swaps whose graceful shutdown has not been observed complete
  // yet. Guarded by the swap lock. Bounded by the topology-change rate: rotation retires nothing,
  // and every swap sweeps entries whose clients have terminated.
  private final List<HttpEndpoint> retired = new ArrayList<>();

  // Serializes swaps and close; a private object so no caller can interfere with the lock.
  private final Object swapLock = new Object();

  private boolean closed; // guarded by the swap lock

  private HttpEndpointManager(
      Map<String, HttpEndpoint> endpoints, @Nullable CallbackUrlProvider callbackUrlProvider) {
    this.endpoints = Map.copyOf(endpoints);
    this.callbackUrlProvider = callbackUrlProvider;
  }

  /** Creates a manager with no async-callback provisioning (see the two-argument overload). */
  public static HttpEndpointManager create(Map<String, HttpServiceConfig> endpointConfigs) {
    return create(endpointConfigs, null);
  }

  /**
   * Creates a manager with one {@link HttpEndpoint} per {@code name → config} entry, wiring {@code
   * callbackUrlProvider} (engine-global; {@code null} when async completion is not configured) into
   * each endpoint's declarative transport.
   */
  public static HttpEndpointManager create(
      Map<String, HttpServiceConfig> endpointConfigs,
      @Nullable CallbackUrlProvider callbackUrlProvider) {
    // Building the initial set is a swap from the empty set. Going through swapHttpEndpoints keeps
    // one shared build path, so its rollback also covers a config that fails here: the endpoints
    // already created are shut down rather than leaked.
    HttpEndpointManager manager = new HttpEndpointManager(Map.of(), callbackUrlProvider);
    manager.swapHttpEndpoints(endpointConfigs);
    return manager;
  }

  @Override
  public TransportAdapter resolve(String service) throws TransportException {
    HttpEndpoint endpoint = endpoints.get(service);
    if (endpoint == null) {
      // Pre-send by construction, so known-not-committed; retryable because on a replica whose
      // configuration lags a removal or rename, a later resolution succeeds once it propagates.
      throw new TransportException(
          "service '"
              + service
              + "' has no registered endpoint (removed, or its configuration has not reached this"
              + " replica yet)",
          true,
          true);
    }
    return endpoint.transportAdapter();
  }

  /** Whether an endpoint is currently registered under {@code name}. */
  public boolean contains(String name) {
    return endpoints.get(name) != null;
  }

  /** The endpoint currently registered under {@code name}, or {@code null} (for tests). */
  @Nullable HttpEndpoint endpointOrNull(String name) {
    return endpoints.get(name);
  }

  /**
   * The {@link SagaHttpClient} for the endpoint currently registered under {@code name}, or {@code
   * null} if none is. The returned client is pinned to that endpoint — a later swap does not rebind
   * it (see {@link HttpEndpointRegistrar}'s embedded-mode contract).
   */
  public @Nullable SagaHttpClient sagaHttpClient(String name) {
    HttpEndpoint endpoint = endpoints.get(name);
    return endpoint != null ? endpoint.sagaHttpClient() : null;
  }

  /**
   * A name → {@link SagaHttpClient} view built from one snapshot of the current endpoint set, for a
   * caller that decides on cardinality and then picks a client (the sole-endpoint code-step
   * lookup): reading names and clients separately could straddle a concurrent swap and miss an
   * endpoint that was registered at every instant. Each client is pinned to its endpoint like
   * {@link #sagaHttpClient(String)}.
   */
  public Map<String, SagaHttpClient> sagaHttpClients() {
    Map<String, HttpEndpoint> snapshot = endpoints;
    Map<String, SagaHttpClient> clients = new HashMap<>();
    for (Map.Entry<String, HttpEndpoint> entry : snapshot.entrySet()) {
      clients.put(entry.getKey(), entry.getValue().sagaHttpClient());
    }
    return Map.copyOf(clients);
  }

  /**
   * Wraps a declaratively-defined service step's phases as a {@link Step} (SAGA) named {@code
   * stepName}, resolving {@code service} to its endpoint once per phase call.
   */
  public Step toStep(String stepName, String service, Map<Phase, CallSpec> phases) {
    requireCallbackProviderForAsync(stepName, phases);
    return new DeclarativeBindingStep(stepName, this, service, phases);
  }

  /**
   * Wraps a declaratively-defined service step's phases as a {@link TccStep} (TCC) named {@code
   * stepName}, resolving {@code service} to its endpoint once per phase call.
   */
  public TccStep toTccStep(String stepName, String service, Map<Phase, CallSpec> phases) {
    requireCallbackProviderForAsync(stepName, phases);
    return new DeclarativeBindingTccStep(stepName, this, service, phases);
  }

  /**
   * Fails fast (at plan build / registration) if a step declares an async phase but no callback URL
   * provider is configured — such a step would park on a {@code 202} but never receive a callback
   * URL, so it could never be completed. Requires the operator to configure the callback base URL +
   * secret before registering an async definition.
   */
  private void requireCallbackProviderForAsync(String stepName, Map<Phase, CallSpec> phases) {
    if (callbackUrlProvider != null) {
      return;
    }
    for (CallSpec spec : phases.values()) {
      if (spec.isAsync()) {
        throw SagaDefinitionException.declarativeStepInvalid(
            stepName,
            "declares an async phase but async completion is not configured on the daemon (missing"
                + " callback URL / secret)");
      }
    }
  }

  @Override
  public void swapHttpEndpoints(Map<String, HttpServiceConfig> services) {
    // Defense in depth on the one user-reachable mutator (external callers may not be compiled
    // with NullAway): reject a null map, name, or config before taking the swap lock, and copy so
    // a caller mutating its map cannot race the swap. The copy keeps the caller's iteration
    // order, so which endpoints a partial-build failure already created stays deterministic for a
    // caller that supplied an ordered map.
    Objects.requireNonNull(services, "services must not be null");
    Map<String, HttpServiceConfig> candidate = new LinkedHashMap<>();
    for (Map.Entry<String, HttpServiceConfig> entry : services.entrySet()) {
      String name = Objects.requireNonNull(entry.getKey(), "service name must not be null");
      candidate.put(
          name,
          Objects.requireNonNull(
              entry.getValue(), "config for service '" + name + "' must not be null"));
    }
    synchronized (swapLock) {
      if (closed) {
        throw new IllegalStateException("HTTP endpoint manager is closed");
      }
      Map<String, HttpEndpoint> current = this.endpoints;
      Map<String, HttpEndpoint> next = new HashMap<>();
      List<HttpEndpoint> replaced = new ArrayList<>();
      List<HttpEndpoint> created = new ArrayList<>();
      try {
        for (Map.Entry<String, HttpServiceConfig> entry : candidate.entrySet()) {
          String name = entry.getKey();
          HttpServiceConfig config = entry.getValue();
          HttpEndpoint existing = current.get(name);
          if (existing != null && existing.sameTopology(config)) {
            // Same client, exchange, and policy survive; the header set is applied unconditionally
            // (an equal-value set is a free no-op) so every swap converges the live headers on its
            // candidate. The headers deliberately have no stored record to diff against: the one
            // live copy on the exchange is the whole truth, so a failed swap cannot strand a stale
            // record behind a rotation it already applied.
            existing.updateDefaultHeaders(config.defaultHeaders());
            next.put(name, existing);
          } else {
            HttpEndpoint fresh = HttpEndpoint.create(config, callbackUrlProvider);
            created.add(fresh);
            next.put(name, fresh);
            if (existing != null) {
              replaced.add(existing);
            }
          }
        }
      } catch (RuntimeException e) {
        // A later entry failed to build, so the snapshot will not be published and the endpoints
        // this attempt already created are unreachable. Shut them down and track them on the
        // retired list (swept by the next successful swap; force-stopped at close) instead of
        // leaking their clients. Reused current endpoints are not touched.
        for (HttpEndpoint endpoint : created) {
          endpoint.shutdown();
        }
        retired.addAll(created);
        throw e;
      }
      for (Map.Entry<String, HttpEndpoint> entry : current.entrySet()) {
        if (!candidate.containsKey(entry.getKey())) {
          replaced.add(entry.getValue());
        }
      }
      // Publish before retiring: a concurrent resolution sees either the old endpoint (whose
      // in-flight exchanges still complete) or the new one — a fresh resolution never returns an
      // endpoint that was already shut down.
      this.endpoints = Map.copyOf(next);
      for (HttpEndpoint endpoint : replaced) {
        endpoint.shutdown();
      }
      retired.addAll(replaced);
      retired.removeIf(HttpEndpoint::isTerminated);
    }
  }

  /** The not-yet-terminated retired endpoints (for tests asserting the retired-list bound). */
  int retiredCount() {
    synchronized (swapLock) {
      return retired.size();
    }
  }

  /**
   * Shuts down every endpoint (current and retired), waits up to {@link #CLOSE_GRACE} for in-flight
   * exchanges to complete, then force-stops whatever remains. Idempotent.
   */
  @Override
  public void close() {
    synchronized (swapLock) {
      if (closed) {
        return;
      }
      closed = true;
      List<HttpEndpoint> all = new ArrayList<>(retired);
      all.addAll(endpoints.values());
      for (HttpEndpoint endpoint : all) {
        endpoint.shutdown();
      }
      long deadlineNanos = System.nanoTime() + CLOSE_GRACE.toNanos();
      for (HttpEndpoint endpoint : all) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          break;
        }
        try {
          endpoint.awaitTermination(Duration.ofNanos(remainingNanos));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      for (HttpEndpoint endpoint : all) {
        endpoint.shutdownNow();
      }
      retired.clear();
    }
  }
}
