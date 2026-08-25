package com.scalar.db.saga.server;

import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.server.SagaServerConfig.ServiceConfig;
import com.scalar.db.saga.transport.HttpEndpointRegistrar;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciles the watched configuration with the running engine. Each {@link #run()} is one pass:
 * snapshot the services and definitions directories, validate the COMPLETE candidate set, and only
 * then apply it — services swapped first (so the fleet-visible definitions that name them can
 * resolve everywhere), definitions registered second. Any validation failure rejects the whole pass
 * and the previously applied configuration keeps serving; per-file errors are aggregated across
 * files so one team's mistake never hides another's.
 *
 * <p>The reconciler is policy-free: {@code SagaServer}'s constructor runs a pass synchronously and
 * fatally (boot keeps today's fail-fast), and {@link SagaConfigReloadManager} schedules it with
 * failures contained. Passes are externally serialized (boot runs before the scheduler starts; the
 * scheduler's fixed-delay task never overlaps itself), with {@code synchronized} as a belt.
 *
 * <p>Bookkeeping is per artifact: the applied-services map advances only when a swap commits, and
 * each definition's applied entry advances as its registration succeeds — so a pass that swaps
 * services but fails a store write records the services as applied and retries only the remaining
 * registrations next pass, instead of seeing "unchanged" and never retrying. A permanent
 * same-version-different-content store conflict (possible via the validation-read/apply-write race
 * between two replicas) is reported distinctly: it self-heals only when an operator bumps the
 * version.
 *
 * <p>Boot equivalence: the boot caller seeds {@code appliedServices} from the endpoints the
 * orchestrator was built with — parsed by the same {@link ServiceFileParser} from the same
 * directory — so the first pass verifies rather than re-applies, and any set a reload accepted also
 * cold-boots a fresh replica.
 */
@ThreadSafe
final class ConfigReconciler {

  private static final Logger logger = LoggerFactory.getLogger(ConfigReconciler.class);

  /** Cap on one definition file, mirroring the services cap; see {@code ServiceFileParser}. */
  static final long MAX_DEFINITION_FILE_BYTES = 1024 * 1024;

  private final ReloadConfig reloadConfig;
  private final @Nullable Path definitionsPath;
  // Whether async completion (callback URL + secret) is configured on this daemon: a definition
  // with an async phase can only be provisioned when it is, and the pass rejects it at validation
  // rather than letting registration fail every pass with a retry that can never succeed.
  private final boolean asyncCallbacksConfigured;
  private final ServiceSecretResolver secretResolver;
  // Fetched lazily so constructing the pass never touches the orchestrator; only a pass that
  // actually has a service diff to apply needs the registrar.
  private final Supplier<HttpEndpointRegistrar> registrar;
  private final Consumer<SagaDefinition> definitionRegistrar;

  // ── Inter-pass state (guarded by the pass serialization) ─────────────────

  private Map<String, ServiceConfig> appliedServices;
  private final Map<String, AppliedDefinition> appliedDefinitions = new HashMap<>();
  // Parse cache keyed by file name: an unchanged file (same content hash) is not re-parsed.
  private final Map<String, CachedParse> definitionParseCache = new HashMap<>();
  // Definitions whose files vanished while their registered version stays startable in the store,
  // keyed by name → the services they reference. Deleting a file retires nothing, so these
  // references outlive the file and must still be honored when a later pass removes a service.
  private final Map<String, Set<String>> vanishedDefinitionServices = new HashMap<>();
  private @Nullable String lastFailureSignature;
  private volatile ReloadStatus status;

  /** One applied definition: the file it came from and its parsed (raw, un-defaulted) form. */
  private record AppliedDefinition(String fileName, SagaDefinition definition) {}

  private record CachedParse(String contentSha256, SagaDefinition definition) {}

  /** A parsed candidate definition with its origin file, for error attribution. */
  private record CandidateDefinition(String fileName, SagaDefinition definition) {}

  /** Thrown internally when a pass is rejected; carries the aggregated reason. */
  private static final class PassRejectedException extends RuntimeException {
    final String candidateSha256;

    PassRejectedException(String message, String candidateSha256) {
      super(message);
      this.candidateSha256 = candidateSha256;
    }
  }

  ConfigReconciler(
      ReloadConfig reloadConfig,
      @Nullable Path definitionsPath,
      boolean asyncCallbacksConfigured,
      Map<String, ServiceConfig> seedAppliedServices,
      Supplier<HttpEndpointRegistrar> registrar,
      Consumer<SagaDefinition> definitionRegistrar) {
    this.reloadConfig = reloadConfig;
    this.definitionsPath = definitionsPath;
    this.asyncCallbacksConfigured = asyncCallbacksConfigured;
    this.secretResolver = new ServiceSecretResolver(reloadConfig.secretsRoot());
    this.registrar = registrar;
    this.definitionRegistrar = definitionRegistrar;
    this.appliedServices = Map.copyOf(seedAppliedServices);
    this.status = new ReloadStatus("(not yet applied)", "(not yet applied)", Instant.EPOCH, null);
  }

  /** The number of definitions currently applied; the boot caller's zero-definitions guard. */
  synchronized int appliedDefinitionCount() {
    return appliedDefinitions.size();
  }

  /** The current status snapshot (for the future admin status endpoint, and for tests). */
  ReloadStatus status() {
    return status;
  }

  /**
   * Boot entry: one pass, rethrowing the rejection so the server fails fast instead of serving
   * under a configuration it could not apply.
   */
  synchronized void runOrThrow() {
    try {
      executePass();
    } catch (PassRejectedException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  /**
   * Scheduled entry: one pass, with rejection folded into state-change logging and the status
   * snapshot.
   *
   * @return whether the pass applied (or verified) cleanly
   */
  synchronized boolean run() {
    try {
      executePass();
      return true;
    } catch (PassRejectedException e) {
      onFailure(e);
      return false;
    }
  }

  // ── The pass ─────────────────────────────────────────────────────────────

  private void executePass() {
    Instant now = Instant.now(reloadConfig.clock());
    List<String> errors = new ArrayList<>();

    // 1. SNAPSHOT + VALIDATE — no side effects on engine or store.
    MessageDigest servicesDigest = sha256();
    Map<String, ServiceConfig> candidateServices = snapshotServices(errors, servicesDigest);
    MessageDigest definitionsDigest = sha256();
    Map<String, CandidateDefinition> candidateDefinitions =
        snapshotDefinitions(errors, definitionsDigest);
    String servicesSha = hex(servicesDigest);
    String definitionsSha = hex(definitionsDigest);

    validateCrossChecks(candidateServices, candidateDefinitions, errors);

    if (!errors.isEmpty()) {
      throw new PassRejectedException(
          "Configuration rejected ("
              + errors.size()
              + " error(s)):\n - "
              + String.join("\n - ", errors),
          servicesSha + ":" + definitionsSha);
    }

    warnOnVanishedDefinitions(candidateDefinitions);
    warnOnRemovedServicesStillReferenced(candidateServices);

    // 2. APPLY services first: definitions propagate fleet-wide through the store the moment they
    // register, so the services they name must be in place before any registration.
    List<String> serviceChanges = diffServices(candidateServices);
    if (!serviceChanges.isEmpty()) {
      try {
        registrar.get().swapHttpEndpoints(toHttpServiceConfigs(candidateServices));
      } catch (RuntimeException e) {
        throw new PassRejectedException(
            "Applying the service set failed (will retry next pass): " + e.getMessage(),
            servicesSha + ":" + definitionsSha);
      }
      appliedServices = Map.copyOf(candidateServices);
      // The swap is live from here. Advance the applied-services hash immediately, before any
      // registration is attempted: a registration failure below rejects the pass, and a rejection
      // that still named the previous service set would describe endpoints this replica no longer
      // has. Any rejection already on the status stays until this pass concludes.
      status =
          new ReloadStatus(servicesSha, status.appliedDefinitionsSha256(), now, status.rejection());
    }

    // 3. APPLY definitions, advancing the applied entry per definition so a mid-apply failure
    // retries only what remains.
    List<String> definitionChanges = new ArrayList<>();
    try {
      registerChangedDefinitions(
          candidateDefinitions, definitionChanges, servicesSha, definitionsSha);
    } catch (PassRejectedException e) {
      // Whatever committed before the failure is live — swapped endpoints here, registered
      // definitions fleet-wide — so the audit line records it. The rejection log alone would read
      // as though the pass changed nothing.
      logAppliedIfChanged(serviceChanges, definitionChanges);
      throw e;
    }

    // 4. Status + audit. The INFO apply line is the audit record: names and versions only, never
    // values. A pass that found nothing to change keeps the previous applied timestamp: it
    // verified the applied state, it did not apply anything.
    Instant appliedAt =
        serviceChanges.isEmpty() && definitionChanges.isEmpty() ? status.appliedAt() : now;
    status = new ReloadStatus(servicesSha, definitionsSha, appliedAt, null);
    if (lastFailureSignature != null) {
      logger.info("Config reload recovered; the candidate set applied cleanly");
      lastFailureSignature = null;
    }
    logAppliedIfChanged(serviceChanges, definitionChanges);
  }

  /**
   * Registers every candidate definition whose parsed form differs from the applied one, advancing
   * the applied entry per definition so a mid-apply failure retries only what remains, and
   * appending each committed registration to {@code definitionChanges} so the caller can audit a
   * partial apply.
   */
  private void registerChangedDefinitions(
      Map<String, CandidateDefinition> candidateDefinitions,
      List<String> definitionChanges,
      String servicesSha,
      String definitionsSha) {
    for (CandidateDefinition candidate : candidateDefinitions.values()) {
      SagaDefinition definition = candidate.definition();
      String name = definition.getName();
      AppliedDefinition applied = appliedDefinitions.get(name);
      if (applied != null && applied.definition().equals(definition)) {
        continue; // unchanged — no store round-trip
      }
      try {
        definitionRegistrar.accept(definition);
      } catch (SagaDefinitionException e) {
        boolean permanentConflict =
            e.getErrorCode() == SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT;
        throw new PassRejectedException(
            "Registering definition "
                + name
                + " "
                + definition.getVersion()
                + " (file '"
                + candidate.fileName()
                + "') failed: "
                + e.getMessage()
                + (permanentConflict
                    ? " [permanent: the stored version differs; bump the version to resolve —"
                        + " retrying cannot fix this]"
                    : " [will retry next pass]"),
            servicesSha + ":" + definitionsSha);
      } catch (RuntimeException e) {
        throw new PassRejectedException(
            "Registering definition "
                + name
                + " "
                + definition.getVersion()
                + " (file '"
                + candidate.fileName()
                + "') failed: "
                + e.getMessage()
                + " [will retry next pass]",
            servicesSha + ":" + definitionsSha);
      }
      appliedDefinitions.put(name, new AppliedDefinition(candidate.fileName(), definition));
      definitionChanges.add(name + ":" + definition.getVersion());
    }
    // Drop applied entries whose names vanished from the snapshot: they were warned about once
    // above, and keeping them would repeat that warning every pass. What they referenced is
    // retained in vanishedDefinitionServices, which outlives the entry.
    appliedDefinitions.keySet().removeIf(name -> !candidateDefinitions.containsKey(name));
  }

  /**
   * The audit record for what this pass committed: changed service and definition names only, never
   * values, with the hashes read from the status so the line always describes the state the status
   * reports. Silent when nothing committed.
   */
  private void logAppliedIfChanged(List<String> serviceChanges, List<String> definitionChanges) {
    if (serviceChanges.isEmpty() && definitionChanges.isEmpty()) {
      return;
    }
    logger.info(
        "Config applied: services[{}] definitions[{}] servicesSha256={} definitionsSha256={}",
        serviceChanges.isEmpty() ? "unchanged" : String.join(", ", serviceChanges),
        definitionChanges.isEmpty() ? "unchanged" : String.join(", ", definitionChanges),
        status.appliedServicesSha256(),
        status.appliedDefinitionsSha256());
  }

  // ── Snapshot ─────────────────────────────────────────────────────────────

  private Map<String, ServiceConfig> snapshotServices(List<String> errors, MessageDigest digest) {
    Path servicesPath = reloadConfig.servicesPath();
    if (servicesPath == null) {
      return Map.of();
    }
    Map<String, ServiceFileParser.ServiceFile> files;
    try {
      files = ServiceFileParser.listServiceFiles(servicesPath);
    } catch (RuntimeException e) {
      errors.add(e.getMessage());
      return Map.of();
    }
    Map<String, ServiceConfig> candidates = new TreeMap<>();
    for (Map.Entry<String, ServiceFileParser.ServiceFile> entry : files.entrySet()) {
      ServiceFileParser.ServiceFile file = entry.getValue();
      // One read through the resolved target the hygiene walk validated (never the visible entry,
      // which would follow its symlink afresh), feeding both the digest and the parse: hashing and
      // parsing separate reads would let a writer change the file in between, so the hash this
      // pass records could describe content it never applied.
      byte[] content =
          readBounded(file.fileName(), file.target(), ServiceFileParser.MAX_FILE_BYTES, errors);
      if (content == null) {
        continue; // unreadable or oversized; already collected
      }
      digestContent(digest, file.fileName(), content);
      try {
        ServiceConfig service =
            ServiceFileParser.parseFile(entry.getKey(), file.fileName(), content, secretResolver);
        ServiceFileParser.requireWithinCeiling(
            entry.getKey(), service, reloadConfig.allowedHostsCeiling());
        candidates.put(entry.getKey(), service);
      } catch (RuntimeException e) {
        errors.add(e.getMessage());
      }
    }
    // D8/D16 guards need the applied set for comparison; they run even when per-file errors were
    // collected, to keep the aggregation complete.
    if (candidates.isEmpty() && !appliedServices.isEmpty() && errors.isEmpty()) {
      errors.add(
          "The candidate service set is empty while "
              + appliedServices.size()
              + " service(s) are applied — a deliberate wind-down to zero goes through a restart,"
              + " so this is treated as a mount or packaging failure and rejected.");
    }
    for (Map.Entry<String, ServiceConfig> entry : candidates.entrySet()) {
      ServiceConfig applied = appliedServices.get(entry.getKey());
      if (applied != null
          && !applied.allowedHosts().isEmpty()
          && entry.getValue().allowedHosts().isEmpty()) {
        errors.add(
            "Service '"
                + entry.getKey()
                + "' previously restricted allowed_hosts but the candidate allows all hosts —"
                + " rejected as a suspected truncation (loosening to allow-all must arrive as a"
                + " deliberate, non-empty change).");
      }
    }
    return candidates;
  }

  private Map<String, CandidateDefinition> snapshotDefinitions(
      List<String> errors, MessageDigest digest) {
    if (definitionsPath == null) {
      return Map.of();
    }
    List<DefinitionFile> files;
    try {
      files = listDefinitionFiles(definitionsPath);
    } catch (RuntimeException e) {
      errors.add(e.getMessage());
      return Map.of();
    }
    Map<String, CandidateDefinition> candidates = new LinkedHashMap<>();
    for (DefinitionFile file : files) {
      String fileName = file.fileName();
      byte[] content = readBounded(fileName, file.target(), MAX_DEFINITION_FILE_BYTES, errors);
      if (content == null) {
        continue; // unreadable or oversized; already collected
      }
      String contentSha = digestContent(digest, fileName, content);
      try {
        CachedParse cached = definitionParseCache.get(fileName);
        SagaDefinition definition;
        // MessageDigest.isEqual is constant-time; these are cache-invalidation hashes, not
        // credentials, but the constant-time compare costs nothing and satisfies the analyzer.
        if (cached != null
            && MessageDigest.isEqual(
                cached.contentSha256().getBytes(StandardCharsets.UTF_8),
                contentSha.getBytes(StandardCharsets.UTF_8))) {
          definition = cached.definition();
        } else {
          // Parsed from the same bytes the hash covers, so a cache entry can never file one
          // file's hash against another read's content.
          definition = parseDefinition(fileName, content);
          definitionParseCache.put(fileName, new CachedParse(contentSha, definition));
        }
        for (SagaDefinition.StepDefinition step : definition.getSteps()) {
          if (step instanceof SagaDefinition.ClassStep) {
            throw SagaDefinitionException.stepClassNotSupportedOnServer(
                definition.getName(), step.getName());
          }
        }
        CandidateDefinition previous =
            candidates.put(definition.getName(), new CandidateDefinition(fileName, definition));
        if (previous != null) {
          errors.add(
              "Definition files '"
                  + previous.fileName()
                  + "' and '"
                  + fileName
                  + "' both define saga '"
                  + definition.getName()
                  + "'; one saga name must live in exactly one file.");
        }
      } catch (RuntimeException e) {
        errors.add("Definition file '" + fileName + "': " + e.getMessage());
      }
    }
    if (candidates.isEmpty() && !appliedDefinitions.isEmpty() && errors.isEmpty()) {
      errors.add(
          "The candidate definition set is empty while "
              + appliedDefinitions.size()
              + " definition(s) are applied — a deliberate wind-down to zero goes through a"
              + " restart, so this is treated as a mount or packaging failure and rejected.");
    }
    return candidates;
  }

  /** One admitted definition file: visible name for attribution, validated target for reads. */
  private record DefinitionFile(String fileName, Path target) {}

  /**
   * The definitions-side hygiene walk. Same containment rules as the services walk — kubelet
   * publishes every visible key of a mounted volume as a symlink through its {@code ..data}
   * indirection, so a visible symlink is the expected shape and is admitted when it resolves to a
   * regular file still inside the directory; a link escaping the directory is the arbitrary-file
   * route the check forbids. The historical differences are kept: {@code definitions_path} may be a
   * single file, and non-definition extensions in a directory are skipped rather than rejected (a
   * README has always been legal there). Reads must go through the returned target, never re-open
   * the visible entry.
   */
  private static List<DefinitionFile> listDefinitionFiles(Path path) {
    if (!Files.exists(path)) {
      throw new IllegalArgumentException(
          "Invalid value for '"
              + SagaServerConfig.DEFINITIONS_PATH_KEY
              + "': no such file or directory "
              + Redaction.redacted(path.toString()));
    }
    if (!Files.isDirectory(path)) {
      return List.of(
          new DefinitionFile(Objects.requireNonNull(path.getFileName()).toString(), path));
    }
    Path realPath;
    try {
      realPath = path.toRealPath();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "'"
              + SagaServerConfig.DEFINITIONS_PATH_KEY
              + "' cannot be resolved ("
              + e.getClass().getSimpleName()
              + ") "
              + Redaction.redacted(path.toString()));
    }
    List<Path> entries;
    try (Stream<Path> stream = Files.list(path)) {
      entries = stream.sorted().toList();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "'"
              + SagaServerConfig.DEFINITIONS_PATH_KEY
              + "' cannot be listed ("
              + e.getClass().getSimpleName()
              + ") "
              + Redaction.redacted(path.toString()));
    }
    List<DefinitionFile> files = new ArrayList<>();
    for (Path entry : entries) {
      String fileName = Objects.requireNonNull(entry.getFileName()).toString();
      if (fileName.startsWith(".") || !isDefinitionFile(fileName)) {
        continue;
      }
      Path target;
      if (Files.isSymbolicLink(entry)) {
        // Kubelet's visible-symlink layout: admitted only when it resolves to a regular file
        // still inside the directory — a link escaping it would be a route to reading an
        // arbitrary file under a definition's name.
        try {
          target = entry.toRealPath();
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "definitions_path entry '"
                  + fileName
                  + "' is a symlink that cannot be resolved ("
                  + e.getClass().getSimpleName()
                  + ")");
        }
        if (!target.startsWith(realPath)
            || !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
          throw new IllegalArgumentException(
              "definitions_path entry '"
                  + fileName
                  + "' is a symlink that does not resolve to a regular file inside"
                  + " definitions_path, which would be a route to reading an arbitrary file under"
                  + " a definition's name. Only the mounted-volume indirection, a link resolving"
                  + " within the directory, is allowed.");
        }
      } else if (Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
        try {
          target = entry.toRealPath();
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "definitions_path entry '"
                  + fileName
                  + "' cannot be resolved ("
                  + e.getClass().getSimpleName()
                  + ")");
        }
      } else {
        // A directory or other non-regular entry whose name matches the extension has always
        // been silently ignored here.
        continue;
      }
      files.add(new DefinitionFile(fileName, target));
    }
    return files;
  }

  private static boolean isDefinitionFile(String fileName) {
    String name = fileName.toLowerCase(Locale.ROOT);
    return name.endsWith(".json") || name.endsWith(".yaml") || name.endsWith(".yml");
  }

  // ── Validation ───────────────────────────────────────────────────────────

  private void validateCrossChecks(
      Map<String, ServiceConfig> candidateServices,
      Map<String, CandidateDefinition> candidateDefinitions,
      List<String> errors) {
    for (CandidateDefinition candidate : candidateDefinitions.values()) {
      SagaDefinition definition = candidate.definition();
      // Every declarative step's service must exist in THIS candidate set: definitions propagate
      // fleet-wide through the store the moment they register, so registering one whose service
      // is absent would strand it on every replica.
      for (SagaDefinition.StepDefinition step : definition.getSteps()) {
        if (step instanceof SagaDefinition.ServiceStep serviceStep) {
          if (!candidateServices.containsKey(serviceStep.getService())) {
            errors.add(
                "Definition file '"
                    + candidate.fileName()
                    + "' step '"
                    + step.getName()
                    + "' references service '"
                    + serviceStep.getService()
                    + "', which has no service file in this candidate set.");
          }
          if (!asyncCallbacksConfigured
              && serviceStep.getPhases().values().stream().anyMatch(CallSpec::isAsync)) {
            errors.add(
                "Definition file '"
                    + candidate.fileName()
                    + "' step '"
                    + step.getName()
                    + "' declares an async phase, but async completion is not configured on this"
                    + " daemon (missing callback URL / secret) — registration would fail on every"
                    + " pass.");
          }
        }
      }
      // Un-bumped change: same name and version as the applied definition, different content.
      AppliedDefinition applied = appliedDefinitions.get(definition.getName());
      if (applied != null
          && applied.definition().getVersion().equals(definition.getVersion())
          && !applied.definition().equals(definition)) {
        errors.add(
            "Definition file '"
                + candidate.fileName()
                + "' changed saga '"
                + definition.getName()
                + "' without bumping its version ("
                + definition.getVersion()
                + "). Registered content is immutable: bump the version, never amend.");
      }
    }
  }

  private void warnOnVanishedDefinitions(Map<String, CandidateDefinition> candidateDefinitions) {
    // A file that came back is a candidate again, and validation covers it from here.
    vanishedDefinitionServices.keySet().removeIf(candidateDefinitions::containsKey);
    for (Map.Entry<String, AppliedDefinition> entry :
        new TreeMap<>(appliedDefinitions).entrySet()) {
      String name = entry.getKey();
      if (!candidateDefinitions.containsKey(name)) {
        // Remember what it referenced before the applied entry is dropped below: the registered
        // version stays startable, so removing one of its services later would break starts of it
        // with nothing left to notice.
        vanishedDefinitionServices.put(name, serviceNamesOf(entry.getValue().definition()));
        // Deleting a file retires nothing: the store's latest version keeps serving starts on
        // every replica. Delete-without-disable silently recreates the dangling-service hazard
        // validation exists to prevent, so it is the one flow that earns a loud warning.
        logger.warn(
            "Definition file for saga '{}' vanished from definitions_path, but the saga remains"
                + " registered in the store and startable. Deleting a file retires nothing;"
                + " disable the saga first, then delete its file.",
            name);
      }
    }
  }

  /**
   * Warns when a service about to be removed is still referenced by a definition whose file is gone
   * but whose registered version remains startable. Deleting a definition file retires nothing, so
   * this is the one flow that can strand a registered saga with nothing rejecting the change: the
   * candidate cross-check only covers definitions that still have files.
   *
   * <p>It warns rather than rejects deliberately — refusing would leave no way to retire a service
   * at all until a retirement marker exists — and the runbook's rule stays disable-then-delete.
   * Once a definition can be marked retired, a retired one needs no warning here.
   */
  private void warnOnRemovedServicesStillReferenced(Map<String, ServiceConfig> candidateServices) {
    for (Map.Entry<String, Set<String>> entry : vanishedDefinitionServices.entrySet()) {
      for (String service : entry.getValue()) {
        if (!candidateServices.containsKey(service) && appliedServices.containsKey(service)) {
          logger.warn(
              "Service '{}' is being removed, but saga '{}' still references it: that saga's"
                  + " definition file is gone while its registered version remains startable, so"
                  + " new starts of it will fail to resolve the service. Retire the saga before"
                  + " deleting the services it names.",
              service,
              entry.getKey());
        }
      }
    }
  }

  /** The distinct services a definition's declarative steps name. */
  private static Set<String> serviceNamesOf(SagaDefinition definition) {
    Set<String> services = new TreeSet<>();
    for (SagaDefinition.StepDefinition step : definition.getSteps()) {
      if (step instanceof SagaDefinition.ServiceStep serviceStep) {
        services.add(serviceStep.getService());
      }
    }
    return services;
  }

  // ── Apply helpers ────────────────────────────────────────────────────────

  /** Human-readable service diff (added/removed/updated names) for the audit line. */
  private List<String> diffServices(Map<String, ServiceConfig> candidates) {
    List<String> changes = new ArrayList<>();
    for (Map.Entry<String, ServiceConfig> entry : candidates.entrySet()) {
      ServiceConfig applied = appliedServices.get(entry.getKey());
      if (applied == null) {
        changes.add("+" + entry.getKey());
      } else if (!applied.equals(entry.getValue())) {
        changes.add("~" + entry.getKey());
      }
    }
    for (String name : appliedServices.keySet()) {
      if (!candidates.containsKey(name)) {
        changes.add("-" + name);
      }
    }
    return changes;
  }

  private static Map<String, HttpServiceConfig> toHttpServiceConfigs(
      Map<String, ServiceConfig> services) {
    Map<String, HttpServiceConfig> configs = new LinkedHashMap<>();
    services.forEach(
        (name, service) ->
            configs.put(
                name,
                new HttpServiceConfig(
                    service.baseUrl(),
                    service.allowedHosts(),
                    service.maxBodyBytes(),
                    null,
                    service.headers())));
    return configs;
  }

  // ── Failure handling ─────────────────────────────────────────────────────

  private void onFailure(PassRejectedException e) {
    // PassRejectedException is always constructed with a message; requireNonNull bridges the
    // JDK's nullable getMessage signature.
    String signature = Objects.requireNonNull(e.getMessage());
    if (signature.equals(lastFailureSignature)) {
      // The same failure as last pass: an operator already has the WARN; repeating it every
      // interval would bury the log.
      logger.debug("Config reload still rejected: {}", signature);
    } else {
      logger.warn("Config reload rejected: {}", signature);
      lastFailureSignature = signature;
    }
    status =
        status.withRejection(
            new ReloadStatus.Rejection(
                e.candidateSha256, signature, Instant.now(reloadConfig.clock())));
  }

  // ── Hashing ──────────────────────────────────────────────────────────────

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e); // guaranteed by the JVM spec
    }
  }

  /**
   * Reads one watched file through the target the hygiene walk validated, bounding the read at
   * {@code cap}. The bound is enforced on the bytes actually read rather than on a prior size
   * check: a file can grow between a stat and the read, and this pass repeats indefinitely against
   * directories a writer keeps updating. Returns {@code null} with an error collected when the file
   * cannot be read or exceeds the cap.
   */
  private static byte @Nullable [] readBounded(
      String fileName, Path file, long cap, List<String> errors) {
    try (InputStream in =
        Files.newInputStream(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      byte[] content = in.readNBytes((int) cap + 1);
      if (content.length > cap) {
        errors.add(
            "File '"
                + fileName
                + "' exceeds the "
                + cap
                + "-byte cap on watched configuration files.");
        return null;
      }
      return content;
    } catch (IOException e) {
      errors.add("File '" + fileName + "' cannot be read (" + e.getClass().getSimpleName() + ").");
      return null;
    }
  }

  /**
   * Folds {@code name NUL content} into the running set digest and returns that file's own content
   * hash, which keys the parse cache.
   */
  private static String digestContent(MessageDigest digest, String name, byte[] content) {
    digest.update(name.getBytes(StandardCharsets.UTF_8));
    digest.update((byte) 0);
    digest.update(content);
    return hex(sha256Of(content));
  }

  /**
   * Parses a definition from the bytes already read and hashed, dispatching on the file extension
   * the hygiene walk admitted. {@code SagaDefinitionParser.parseFile} is deliberately not used
   * here: it would re-open the file and read it unbounded, outside the cap above.
   */
  private static SagaDefinition parseDefinition(String fileName, byte[] content) {
    String text;
    try {
      text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(content)).toString();
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("is not valid UTF-8", e);
    }
    return fileName.toLowerCase(Locale.ROOT).endsWith(".json")
        ? SagaDefinitionParser.parseJson(text)
        : SagaDefinitionParser.parseYaml(text);
  }

  private static MessageDigest sha256Of(byte[] content) {
    MessageDigest digest = sha256();
    digest.update(content);
    return digest;
  }

  private static String hex(MessageDigest digest) {
    return HexFormat.of().formatHex(digest.digest());
  }
}
