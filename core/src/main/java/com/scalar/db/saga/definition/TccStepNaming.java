package com.scalar.db.saga.definition;

/**
 * The step-name suffixes that qualify a TCC step's base name per forward/backward phase (e.g. a TCC
 * step {@code seat} runs as {@code seat.reserve}, {@code seat.confirm}, and {@code seat.cancel}).
 *
 * <p>This is the single source of truth shared by the two sides that must agree on the qualified
 * name: the engine's execution-plan wrappers ({@code TccReserveStep}/{@code TccConfirmStep}), which
 * name the step that a {@code STEP_PENDING} marker records, and the declarative transport binding
 * ({@code DeclarativeBindingTccStep}), which hands the participant the correlation header and the
 * async callback URL. If the two drifted, an async TCC step would park under one name but mint a
 * callback URL keyed on another, so the callback could never match the parked step.
 *
 * <p>{@code public} solely for cross-package access within the module (engine and transport).
 */
public final class TccStepNaming {

  /** Suffix for the reserve (Try) phase. */
  public static final String RESERVE_SUFFIX = ".reserve";

  /** Suffix for the confirm phase. */
  public static final String CONFIRM_SUFFIX = ".confirm";

  /** Suffix for the cancel (compensating) phase. */
  public static final String CANCEL_SUFFIX = ".cancel";

  private TccStepNaming() {}
}
