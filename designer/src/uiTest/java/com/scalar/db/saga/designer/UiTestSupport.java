package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.AriaRole;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs a designer on a fresh workspace for each test, and a Chromium page on it. A test fails if
 * the page logs an error or throws, so a broken module fails every test, not only the one that uses
 * it.
 */
abstract class UiTestSupport {

  private static Playwright playwright;
  private static Browser browser;

  @TempDir Path workspace;
  private DesignerServer server;
  private BrowserContext context;
  protected Page page;
  private final List<String> pageErrors = new ArrayList<>();

  @BeforeAll
  static void launchBrowser() {
    playwright = Playwright.create();
    browser = playwright.chromium().launch();
  }

  @AfterAll
  static void closeBrowser() {
    browser.close();
    playwright.close();
  }

  @BeforeEach
  void startDesigner() throws IOException {
    server =
        new DesignerServer(
            new DesignerApi(new Workspace(workspace), null),
            new RequestGuard("127.0.0.1"),
            "127.0.0.1",
            0);
    server.start();
    context = browser.newContext(new Browser.NewContextOptions().setAcceptDownloads(true));
    page = context.newPage();
    // Chromium also logs every non-2xx response as a console error; those are answers the page
    // handles, such as a 409 that opens the conflict dialog, so only the page's own errors count.
    page.onConsoleMessage(
        message -> {
          if (message.type().equals("error")
              && !message.text().startsWith("Failed to load resource")) {
            pageErrors.add(message.text());
          }
        });
    page.onPageError(error -> pageErrors.add(error));
  }

  @AfterEach
  void stopDesigner() {
    context.close();
    server.close();
    assertThat(pageErrors).as("errors the page logged").isEmpty();
  }

  /** The designer's address. */
  protected String url() {
    return "http://127.0.0.1:" + server.port() + "/";
  }

  /** Writes a file into the workspace. */
  protected Path write(String name, String text) throws IOException {
    return Files.writeString(workspace.resolve(name), text);
  }

  /** Reads a file from the workspace. */
  protected String read(String name) throws IOException {
    return Files.readString(workspace.resolve(name));
  }

  /** Copies one of the getting-started definitions into the workspace. */
  protected void copyGettingStarted(String name) throws IOException {
    Path root = Path.of(System.getProperty("designer.fixtures.root"));
    Files.copy(
        root.resolve("getting-started/conf/definitions").resolve(name), workspace.resolve(name));
  }

  /** Opens the designer on its file list, and opens a file from it. */
  protected void open(String file) {
    page.navigate(url());
    button("Open " + file).click();
    page.getByLabel("Steps").waitFor();
  }

  /** A button by its accessible name, which must match exactly. */
  protected Locator button(String name) {
    return page.getByRole(
        AriaRole.BUTTON, new Page.GetByRoleOptions().setName(name).setExact(true));
  }

  /** A labelled section of the page, such as a call's "Execution". */
  protected Locator region(String name) {
    return page.getByRole(
        AriaRole.REGION, new Page.GetByRoleOptions().setName(name).setExact(true));
  }

  /** The message the page shows after an action, such as a save. */
  protected Locator toast() {
    return page.locator(".toast");
  }

  /** The names on the step cards, in order. */
  protected Locator cards() {
    return page.locator(".card .t");
  }
}
