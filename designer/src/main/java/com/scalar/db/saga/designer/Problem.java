package com.scalar.db.saga.designer;

import org.jspecify.annotations.Nullable;

/**
 * One thing wrong with the definition being edited, as the UI shows it.
 *
 * @param code the parser's error code (such as {@code INVALID_STEP_DEFINITION}), or {@link #FORM}
 *     for a value the form holds that cannot be written to the file at all
 * @param stepId the editor's id of the step the problem belongs to, or {@code null} when it belongs
 *     to the definition as a whole
 * @param message what is wrong, for display
 */
record Problem(String code, @Nullable String stepId, String message) {

  /** The code of a problem found while converting the form, before the parser runs. */
  static final String FORM = "FORM";
}
