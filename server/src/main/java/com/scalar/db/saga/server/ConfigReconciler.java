package com.scalar.db.saga.server;

import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.server.SagaServerConfig.ServiceConfig;
import com.scalar.db.saga.transport.HttpEndpointRegistrar;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciles the watched configuration with the running engine. Each {@link #run()} is one pass:
 * snapshot the services and definitions directories, validate the COMPLETE candidate set, and only
 * then apply it — services swapped first, so this replica can serve every definition it publishes,
 * definitions registered second. Any validation failure rejects the whole pass and the previously
 * applied configuration keeps serving; per-file errors are aggregated across files so one team's
 * mistake never hides another's.
 *
 * <p>The reconciler is policy-free: {@code SagaServer}'s constructor runs a pass synchronously and
 * fatally (boot keeps today's fail-fast), and {@link SagaConfigReloadManager} schedules it with
 * failures contained. Passes are externally serialized (boot runs before the scheduler starts; the
 * scheduler's fixed-delay task never overlaps itself), with {@code synchronized} as a belt.
 *
 * <p>Bookkeeping is per artifact: the applied-services map advances only when a swap commits, and
 * each definition's applied entry advances as its registration succeeds — so a pass that swaps
 * services but fails a store write records the services as applied and retries only the remaining
 * registrations next pass, instead of seeing "unchanged" and never retrying. A failure that another
 * pass could clear is reported distinctly from one that needs someone to act — reaching the store
 * is the only thing a later pass does differently, so everything else, a same-version-different-
 * content conflict above all, waits for a human however long it retries.
 *
 * <p>Boot equivalence: boot is a pass. The orchestrator is built with no endpoints and the first
 * pass installs them, so this class is the only translation from a service file to a live endpoint,
 * and "any set a reload accepted also cold-boots a fresh replica" holds by construction rather than
 * by two paths agreeing forever.
 */
@ThreadSafe
final class ConfigReconciler {

  private static final Logger logger = LoggerFactory.getLogger(ConfigReconciler.class);

  private final ReloadConfig reloadConfig;
  private final @Nullable Path definitionsPath;
  // Whether async completion (callback URL + secret) is configured on this daemon: a definition
  // with an async phase can only be provisioned when it is, and the pass rejects it at validation
  // rather than letting registration fail every pass with a retry that can never succeed.
  private final boolean asyncCallbacksConfigured;
  private final ServiceSecretResolver secretResolver;
  private final HttpEndpointRegistrar registrar;
  private final Consumer<SagaDefinition> definitionRegistrar;

  // ── Inter-pass state (guarded by the pass serialization) ─────────────────

  private Map<String, ServiceConfig> appliedServices;
  // Name-ordered so a pass that warns about several vanished definitions warns in a stable order.
  private final Map<String, AppliedDefinition> appliedDefinitions = new TreeMap<>();
  // Parse cache keyed by file name: an unchanged file (same content hash) is not re-parsed. It
  // saves more than the parse — a hit returns the SAME SagaDefinition instance appliedDefinitions
  // holds, so the steady-state comparison below settles on reference equality instead of deep
  // walking the step and CallSpec graph of every definition, every pass. Anything that copies a
  // cached definition on the way out would keep the cache and lose that.
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

  /**
   * Thrown internally when a pass is rejected; carries the aggregated reason and whether clearing
   * it needs the mounted files to change (see {@link
   * ReloadStatus.Rejection#operatorActionRequired}).
   */
  private static final class PassRejectedException extends RuntimeException {
    final String candidateSha256;
    final boolean operatorActionRequired;

    PassRejectedException(String message, String candidateSha256, boolean operatorActionRequired) {
      super(message);
      this.candidateSha256 = candidateSha256;
      this.operatorActionRequired = operatorActionRequired;
    }
  }

  ConfigReconciler(
      ReloadConfig reloadConfig,
      @Nullable Path definitionsPath,
      boolean asyncCallbacksConfigured,
      HttpEndpointRegistrar registrar,
      Consumer<SagaDefinition> definitionRegistrar) {
    this.reloadConfig = reloadConfig;
    this.definitionsPath = definitionsPath;
    this.asyncCallbacksConfigured = asyncCallbacksConfigured;
    this.secretResolver = new ServiceSecretResolver(reloadConfig.secretsRoot());
    this.registrar = registrar;
    this.definitionRegistrar = definitionRegistrar;
    this.appliedServices = Map.of();
    this.status = ReloadStatus.initial();
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
    String candidateSha = servicesSha + ":" + definitionsSha;

    validateCrossChecks(candidateServices, candidateDefinitions, errors);

    if (!errors.isEmpty()) {
      throw new PassRejectedException(
          "Configuration rejected (" + errors.size() + " error(s)):" + describeProblems(errors),
          candidateSha,
          true);
    }

    warnOnVanishedDefinitions(candidateDefinitions);
    warnOnRemovedServicesStillReferenced(candidateServices);

    // 2. APPLY services first. A definition is startable fleet-wide the moment it registers, so
    // this replica installs the services it names before publishing it. That orders the change
    // within this replica, which is as far as ordering reaches: a replica whose files have not
    // synced yet still sees the definition first, and its resolve fails retryably and self-heals
    // on its next pass. That bounded skew is accepted by design — a fleet-wide barrier would need
    // coordination this feature deliberately does not have.
    List<String> serviceChanges = diffServices(candidateServices);
    if (!serviceChanges.isEmpty()) {
      try {
        registrar.swapHttpEndpoints(toHttpServiceConfigs(candidateServices));
      } catch (RuntimeException e) {
        throw new PassRejectedException(
            "Applying the service set failed (will retry next pass): " + e.getMessage(),
            candidateSha,
            false);
      }
      appliedServices = Map.copyOf(candidateServices);
      // The swap is live from here. Advance the applied-services hash immediately, before any
      // registration is attempted: a registration failure below rejects the pass, and a rejection
      // that still named the previous service set would describe endpoints this replica no longer
      // has. Any rejection already on the status stays until this pass concludes.
      status =
          new ReloadStatus(
              servicesSha,
              status.appliedDefinitionsSha256(),
              now,
              status.lastPassAt(),
              status.rejection());
    }

    // 3. APPLY definitions, advancing the applied entry per definition so a mid-apply failure
    // retries only what remains.
    List<String> definitionChanges = new ArrayList<>();
    try {
      registerChangedDefinitions(candidateDefinitions, definitionChanges, candidateSha);
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
    status = new ReloadStatus(servicesSha, definitionsSha, appliedAt, now, null);
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
      String candidateSha) {
    List<String> failures = new ArrayList<>();
    boolean anyNeedsOperator = false;
    for (CandidateDefinition candidate : candidateDefinitions.values()) {
      SagaDefinition definition = candidate.definition();
      String name = definition.getName();
      AppliedDefinition applied = appliedDefinitions.get(name);
      if (applied != null && applied.definition().equals(definition)) {
        continue; // unchanged — no store round-trip
      }
      try {
        definitionRegistrar.accept(definition);
      } catch (RuntimeException e) {
        // Collect and carry on. Definitions are independent rows, so one that cannot register —
        // above all one whose conflict is permanent — must not decide the fate of the rest:
        // abandoning the loop here would let it block every definition that sorts after it, on
        // every pass, for as long as the conflict lasts.
        boolean needsOperator = needsOperatorAction(e);
        anyNeedsOperator |= needsOperator;
        failures.add(describeRegistrationFailure(candidate, e, needsOperator));
        continue;
      }
      appliedDefinitions.put(name, new AppliedDefinition(candidate.fileName(), definition));
      definitionChanges.add(name + ":" + definition.getVersion());
    }
    // Unconditional, so a failure above cannot strand this: a name whose file vanished has to be
    // dropped even when an unrelated registration failed, or its warning re-fires every pass.
    // What it referenced is retained in vanishedDefinitionServices, which outlives the entry.
    appliedDefinitions.keySet().removeIf(name -> !candidateDefinitions.containsKey(name));
    if (!failures.isEmpty()) {
      throw new PassRejectedException(
          "Registering " + failures.size() + " definition(s) failed:" + describeProblems(failures),
          candidateSha,
          anyNeedsOperator);
    }
  }

  /**
   * Whether clearing a registration failure needs someone to act, or whether another pass might
   * clear it on its own.
   *
   * <p>Reaching the store is the only thing another pass does differently, so a store failure is
   * the only transient one — and it says so itself: {@link SagaPersistenceException} carries
   * whether its code is retryable, which keeps a permanent serialization failure from being
   * reported as an outage. Everything else fails identically on every pass: a version whose stored
   * content differs, a definition the engine refuses to build, a bug in this process. Telling an
   * operator to wait for one of those is telling them to wait forever, so the default is that
   * someone has to look.
   */
  private static boolean needsOperatorAction(RuntimeException e) {
    if (e instanceof SagaPersistenceException persistence) {
      return !persistence.isRetryable();
    }
    return true;
  }

  /** Whether a failure is the store refusing to overwrite a version with different content. */
  private static boolean isVersionContentConflict(RuntimeException e) {
    return e instanceof SagaDefinitionException definitionException
        && definitionException.getErrorCode()
            == SagaErrorCode.SAGA_DEFINITION_VERSION_CONTENT_CONFLICT;
  }

  /**
   * One registration failure, naming the definition and the file it came from, and saying in the
   * text whether retrying can fix it. The status carries the same distinction as a field, so the
   * prose is for the operator reading the log, not for anything to match on.
   */
  private static String describeRegistrationFailure(
      CandidateDefinition candidate, RuntimeException e, boolean needsOperator) {
    return "definition "
        + candidate.definition().getName()
        + " "
        + candidate.definition().getVersion()
        + " (file '"
        + candidate.fileName()
        + "'): "
        + e.getMessage()
        + registrationHint(e, needsOperator);
  }

  /** The operator-facing half of a registration failure: wait, or go and do something. */
  private static String registrationHint(RuntimeException e, boolean needsOperator) {
    if (!needsOperator) {
      return " [will retry next pass]";
    }
    if (isVersionContentConflict(e)) {
      return " [permanent: the stored version differs; bump the version to resolve — retrying"
          + " cannot fix this]";
    }
    return " [permanent: this fails the same way on every pass; the definition has to change, or"
        + " the failure itself needs looking at]";
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
          readBounded(file.fileName(), file.target(), WatchedFiles.MAX_FILE_BYTES, errors);
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
    // Suppressed when per-file errors were already collected: those errors explain the empty
    // candidate set, and reporting a wind-down on top of them would be a second, misleading
    // reason for the same rejection. The allowed_hosts check below runs regardless — a service
    // that parsed cleanly can still be loosening its egress.
    if (candidates.isEmpty() && !appliedServices.isEmpty() && errors.isEmpty()) {
      errors.add(
          "The candidate service set is empty while "
              + appliedServices.size()
              + " service(s) are applied — a deliberate wind-down to zero goes through a restart,"
              + " so this is treated as a mount or packaging failure and rejected.");
    }
    // A truncation detector, not an egress control. It compares against what is applied, so it
    // catches the accident it is for — a file edited down to nothing, an allowed_hosts line lost in
    // a merge — and not a service deleted in one pass and recreated allow-all in the next, which
    // presents as a service that never had an allowlist. Retaining a removed service's allowlist
    // forever would close that at the cost of trapping the reuse of a service name behind a
    // restart. The control that holds regardless of pass sequence is the operator's
    // egress.allowed_hosts_ceiling; this is the guard for operators who set no ceiling.
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
      byte[] content = readBounded(fileName, file.target(), WatchedFiles.MAX_FILE_BYTES, errors);
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
    // Keyed by file name, so a convention that puts a version or a content hash in the name would
    // otherwise retain a parsed definition per name that ever existed.
    Set<String> presentFiles = new HashSet<>();
    for (DefinitionFile file : files) {
      presentFiles.add(file.fileName());
    }
    definitionParseCache.keySet().retainAll(presentFiles);
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
      // A single configured file, resolved like a directory entry so the NOFOLLOW read that
      // follows lands on a real file: a ConfigMap that mounts one key publishes it as a symlink
      // through kubelet's ..data indirection. Containment does not apply here — it exists because
      // entries INSIDE a watched directory are chosen by whoever writes that directory, whereas
      // this path is named by the operator in server.properties, with no enclosing directory to
      // be contained to.
      Path target = WatchedFiles.realPathOf(path, SagaServerConfig.DEFINITIONS_PATH_KEY);
      if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
        throw new IllegalArgumentException(
            "'"
                + SagaServerConfig.DEFINITIONS_PATH_KEY
                + "' does not resolve to a regular file "
                + Redaction.redacted(path.toString()));
      }
      return List.of(new DefinitionFile(WatchedFiles.fileNameOf(path), target));
    }
    Path realPath = WatchedFiles.realPathOf(path, SagaServerConfig.DEFINITIONS_PATH_KEY);
    List<Path> entries = WatchedFiles.listSorted(path, SagaServerConfig.DEFINITIONS_PATH_KEY);
    List<DefinitionFile> files = new ArrayList<>();
    for (Path entry : entries) {
      String fileName = WatchedFiles.fileNameOf(entry);
      if (fileName.startsWith(".") || !isDefinitionFile(fileName)) {
        continue;
      }
      // A directory or other non-regular entry whose name matches the extension has always been
      // silently ignored here; unlike services_path, this directory has never owned every entry.
      Path target =
          WatchedFiles.resolveEntry(
              entry, fileName, realPath, SagaServerConfig.DEFINITIONS_PATH_KEY);
      if (target == null) {
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

  /**
   * Warns when a definition's file is gone while its registered version stays startable.
   *
   * <p>Best effort by construction, and worth knowing exactly how: the applied set is rebuilt from
   * the directory at every boot, so only a process that observes both the definition and its
   * deletion can notice one. A replica that starts after the file was already deleted sees a
   * directory that simply never contained it, and never warns. Hot reload is what makes this the
   * normal case rather than the exception — a config-only change no longer restarts anything — but
   * a rolling restart between the two edits still silences the warning, and does so per replica.
   * The retirement marker that makes deletion mean something replaces this mechanism rather than
   * patching it.
   */
  private void warnOnVanishedDefinitions(Map<String, CandidateDefinition> candidateDefinitions) {
    // A file that came back is a candidate again, and validation covers it from here.
    vanishedDefinitionServices.keySet().removeIf(candidateDefinitions::containsKey);
    for (Map.Entry<String, AppliedDefinition> entry : appliedDefinitions.entrySet()) {
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

  /**
   * The service diff as {@code +added}, {@code ~updated}, {@code -removed} names. It is both the
   * audit line's content and the predicate that decides whether a swap happens at all: an empty
   * diff means the applied endpoints already match the candidate set.
   */
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

  /**
   * Renders collected problems as one indented block, each flattened to a single line. Property
   * keys and file names reach these messages straight from the mounted files, so a newline inside
   * one would render as what looks like a separate log record. Every collected problem is consumed
   * here, which is what makes that hold for messages this class does not build itself.
   */
  private static String describeProblems(List<String> problems) {
    StringBuilder rendered = new StringBuilder();
    for (String problem : problems) {
      rendered.append("\n - ").append(Redaction.oneLine(problem));
    }
    return rendered.toString();
  }

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
    Instant now = Instant.now(reloadConfig.clock());
    status =
        status.withRejection(
            new ReloadStatus.Rejection(e.candidateSha256, signature, now, e.operatorActionRequired),
            now);
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
   * Reads one watched file, collecting the failure instead of throwing so one unreadable file does
   * not decide the fate of everything the rest of the directory says.
   */
  private static byte @Nullable [] readBounded(
      String fileName, Path file, long cap, List<String> errors) {
    try {
      return WatchedFiles.read(fileName, file, cap);
    } catch (IllegalArgumentException e) {
      errors.add(e.getMessage());
      return null;
    }
  }

  /**
   * Folds {@code name NUL length content} into the running set digest and returns that file's own
   * content hash, which keys the parse cache.
   *
   * <p>The length is what makes the framing unambiguous. Name and content alone concatenate into a
   * stream that does not say where one file ends, so one file whose content embedded {@code
   * othername NUL othercontent} would hash identically to the two files it names — and this digest
   * is what an operator compares across replicas to tell a lagging one from a rejecting one. It
   * takes a deliberate NUL inside a config file, which only a service file could even carry (JSON
   * and YAML reject a raw one), so this is not a reachable attack; it is a digest that should not
   * need the caveat.
   */
  private static String digestContent(MessageDigest digest, String name, byte[] content) {
    digest.update(name.getBytes(StandardCharsets.UTF_8));
    digest.update((byte) 0);
    digest.update(ByteBuffer.allocate(Long.BYTES).putLong(content.length).array());
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
    String lower = fileName.toLowerCase(Locale.ROOT);
    if (lower.endsWith(".json")) {
      return SagaDefinitionParser.parseJson(text);
    }
    if (lower.endsWith(".yaml") || lower.endsWith(".yml")) {
      return SagaDefinitionParser.parseYaml(text);
    }
    // Rejected rather than guessed, matching SagaDefinitionParser.parseFile. Only a single-file
    // definitions_path can reach this — the directory walk filters by extension first — and
    // defaulting it to YAML would silently accept a file the parser used to refuse.
    throw new IllegalArgumentException(
        "has no recognized definition extension (.json, .yaml, .yml)");
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
