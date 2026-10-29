package com.scalar.db.saga.integration;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Empties the external store before every integration test. Registered through {@code
 * META-INF/services}, so it needs JUnit's extension autodetection, which the build turns on only
 * when an external store is configured; on SQLite it is never loaded.
 */
public final class IntegrationTestStoreExtension implements BeforeEachCallback {

  /** Creates the extension; JUnit instantiates it through the service-loader registration. */
  public IntegrationTestStoreExtension() {}

  @Override
  public void beforeEach(ExtensionContext context) {
    IntegrationTestStore.reset();
  }
}
