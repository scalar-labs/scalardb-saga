package com.scalar.db.saga.server;

import java.nio.charset.Charset;
import java.nio.file.Path;

/**
 * The {@code <charset>:<path>} body of a {@code ${file:...}} reference, parsed once for both
 * resolvers so they classify a malformed reference the same way.
 *
 * <p>Both failures here are permanent: neither depends on what is mounted, so both must be reported
 * rather than softened, wherever the reference is being resolved.
 *
 * @param charset the charset the file is decoded with
 * @param path the file the reference names, not yet checked for existence or containment
 */
record SecretFileReference(Charset charset, Path path) {

  /**
   * Parses the lookup key commons-text hands a {@code file} lookup, e.g. {@code
   * UTF-8:/run/secrets/token}.
   *
   * @throws PermanentReferenceException if the form is wrong or the charset is one no JVM knows
   */
  static SecretFileReference parse(String key) {
    int colon = key.indexOf(':');
    if (colon <= 0 || colon == key.length() - 1) {
      throw new PermanentReferenceException(
          "A ${file:...} reference must be ${file:<charset>:<path>}, e.g."
              + " ${file:UTF-8:/run/secrets/token}; got '${file:"
              + Redaction.oneLine(key)
              + "}'");
    }
    String charsetName = key.substring(0, colon);
    Charset charset;
    try {
      charset = Charset.forName(charsetName);
    } catch (IllegalArgumentException e) {
      // IllegalCharsetNameException and UnsupportedCharsetException both land here, and mean the
      // same thing to an operator: no JVM will decode the file with this.
      throw new PermanentReferenceException(
          "'"
              + Redaction.oneLine(charsetName)
              + "' in a ${file:...} reference is not a charset this JVM can decode with; use a"
              + " standard name such as UTF-8");
    }
    return new SecretFileReference(charset, Path.of(key.substring(colon + 1)));
  }
}
