package com.scalar.db.saga.designer;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

import com.microsoft.playwright.Download;
import com.microsoft.playwright.Route;
import com.microsoft.playwright.options.AriaRole;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.Test;

class SaveUiTest extends UiTestSupport {

  @Test
  void save_afterTheFileChangedOnDisk_asksAndOverwritesOnRequest() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    write("order-saga.json", read("order-saga.json").replace("\"order-saga\"", "\"changed\""));
    page.getByLabel("Saga name").fill("mine");

    // Act
    button("Save").click();
    assertThat(page.getByRole(AriaRole.DIALOG)).containsText("changed since you opened it");
    button("Overwrite").click();

    // Assert
    assertThat(toast()).containsText("Saved");
    assertThat(read("order-saga.json")).contains("\"mine\"").doesNotContain("\"changed\"");
  }

  @Test
  void save_afterTheFileChangedOnDisk_reloadsOnRequestAndOffersTheEditsBack() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    write("order-saga.json", read("order-saga.json").replace("\"order-saga\"", "\"changed\""));
    page.getByLabel("Saga name").fill("mine");

    // Act
    button("Save").click();
    button("Reload from disk").click();

    // Assert
    assertThat(page.getByRole(AriaRole.DIALOG)).containsText("never saved");
    button("Discard them").click();
    assertThat(page.getByLabel("Saga name")).hasValue("changed");
    assertThat(read("order-saga.json")).contains("\"changed\"");
  }

  @Test
  void open_withUnsavedEditsFromAnEarlierVisit_offersThemBack() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    page.getByLabel("Saga name").fill("draft-name");
    // The draft is written when the edit renders; wait for that before leaving the page.
    assertThat(page.locator(".dirty")).isVisible();

    // Act
    open("order-saga.json");
    button("Restore").click();

    // Assert
    assertThat(page.getByLabel("Saga name")).hasValue("draft-name");
    assertThat(read("order-saga.json")).doesNotContain("draft-name");
  }

  @Test
  void open_afterASuccessfulSave_offersNoDraft() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    page.getByLabel("Saga name").fill("saved-name");
    button("Save").click();
    assertThat(toast()).containsText("Saved");

    // Act
    open("order-saga.json");

    // Assert
    assertThat(page.getByLabel("Saga name")).hasValue("saved-name");
    assertThat(page.getByRole(AriaRole.DIALOG)).hasCount(0);
  }

  @Test
  void export_definition_downloadsItInTheChosenFormat() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    button("Export").click();
    button("YAML").click();

    // Act
    Download download = page.waitForDownload(() -> button("Download the definition").click());

    // Assert
    assertThat(download.suggestedFilename()).isEqualTo("order-saga.yaml");
    assertThat(Files.readString(download.path())).startsWith("name: order-saga\n");
  }

  @Test
  void export_bundle_downloadsTheDefinitionAndEveryServiceTemplate() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    button("Export").click();

    // Act
    Download download =
        page.waitForDownload(() -> button("Download with service templates (zip)").click());

    // Assert
    assertThat(download.suggestedFilename()).isEqualTo("order-saga.zip");
    assertThat(entries(download))
        .contains(
            "definitions/order-saga.json",
            "services/payment.properties",
            "services/inventory.properties");
  }

  @Test
  void export_withProblems_offersNoDownload() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    region("Execution").getByLabel("charge execution method").selectOption("GET");
    assertThat(page.locator(".card").first()).containsText("problem");

    // Act
    button("Export").click();

    // Assert
    assertThat(button("Download the definition")).isDisabled();
    assertThat(page.getByRole(AriaRole.ALERT)).containsText("Fix the problem first");
  }

  @Test
  void open_restoreDialogClosedWithoutAnAnswer_keepsTheDraftForNextTime() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    page.getByLabel("Saga name").fill("draft-name");
    assertThat(page.locator(".dirty")).isVisible();
    open("order-saga.json");

    // Act
    page.keyboard().press("Escape");
    open("order-saga.json");

    // Assert
    button("Restore").click();
    assertThat(page.getByLabel("Saga name")).hasValue("draft-name");
  }

  @Test
  void save_answerArrivingAfterTheFileWasLeft_changesNothingElse() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    copyGettingStarted("order-saga-pivot.json");
    AtomicReference<Route> held = new AtomicReference<>();
    page.route(
        "**/api/files/order-saga.json",
        route -> {
          if (route.request().method().equals("PUT") && held.get() == null) {
            held.set(route);
          } else {
            route.resume();
          }
        });
    open("order-saga.json");
    page.getByLabel("Saga name").fill("mine");
    button("Save").click();
    page.waitForCondition(() -> held.get() != null);

    // Act
    button("Files").click();
    button("Open order-saga-pivot.json").click();
    page.getByLabel("Steps").waitFor();
    page.waitForResponse("**/api/files/order-saga.json", () -> requireNonNull(held.get()).resume());

    // Assert
    page.getByLabel("Saga name").fill("pivot-renamed");
    button("Save").click();
    assertThat(toast()).containsText("Saved order-saga-pivot.json");
    assertThat(read("order-saga-pivot.json")).contains("\"pivot-renamed\"");
  }

  @Test
  void validate_answerForThePreviousFile_isNotShownOnTheNextOne() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    copyGettingStarted("order-saga-pivot.json");
    AtomicReference<Route> held = new AtomicReference<>();
    page.route(
        "**/api/validate",
        route -> {
          boolean invalid = String.valueOf(route.request().postData()).contains("\"GET\"");
          if (invalid && held.get() == null) {
            held.set(route);
          } else {
            route.resume();
          }
        });
    open("order-saga.json");
    region("Execution").getByLabel("charge execution method").selectOption("GET");
    page.waitForCondition(() -> held.get() != null);

    // Act
    button("Files").click();
    button("Open order-saga-pivot.json").click();
    assertThat(button("No problems")).isVisible();
    page.waitForResponse("**/api/validate", () -> requireNonNull(held.get()).resume());

    // Assert
    assertThat(button("No problems")).isVisible();
    assertThat(page.locator(".card .chip.err")).hasCount(0);
  }

  @Test
  void save_editsMadeWhileItRuns_areKeptForTheNextVisit() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    AtomicReference<Route> held = new AtomicReference<>();
    page.route(
        "**/api/files/order-saga.json",
        route -> {
          if (route.request().method().equals("PUT") && held.get() == null) {
            held.set(route);
          } else {
            route.resume();
          }
        });
    open("order-saga.json");
    page.getByLabel("Saga name").fill("saved-name");
    button("Save").click();
    page.waitForCondition(() -> held.get() != null);
    page.getByLabel("Saga name").fill("later-name");

    // Act
    page.waitForResponse("**/api/files/order-saga.json", () -> requireNonNull(held.get()).resume());
    assertThat(toast()).containsText("Saved");
    open("order-saga.json");

    // Assert
    assertThat(page.getByLabel("Saga name")).hasValue("saved-name");
    button("Restore").click();
    assertThat(page.getByLabel("Saga name")).hasValue("later-name");
  }

  @Test
  void validate_answerForAnEarlierEdit_isNotShownForTheCurrentModel() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    assertThat(button("No problems")).isVisible();
    List<Route> held = new CopyOnWriteArrayList<>();
    page.route("**/api/validate", held::add);
    region("Execution").getByLabel("charge execution method").selectOption("GET");
    page.waitForCondition(() -> held.size() == 1);

    // Act
    region("Execution").getByLabel("charge execution method").selectOption("POST");
    page.waitForResponse("**/api/validate", () -> held.get(0).resume());

    // Assert
    // The model is back to what the first check found valid, and the next request is still held.
    assertThat(page.locator(".chip.err")).hasCount(0);
    assertThat(button("No problems")).isVisible();
    page.waitForCondition(() -> held.size() == 2);
    held.get(1).resume();
  }

  @Test
  void validate_requestFailed_saysSoAndChecksAgainOnRequest() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    assertThat(button("No problems")).isVisible();
    page.route("**/api/validate", Route::abort);

    // Act
    page.getByLabel("Saga name").fill("renamed");

    // Assert
    assertThat(button("The check failed; check again")).isVisible();
    button("Export").click();
    assertThat(button("Download the definition")).isDisabled();
    page.keyboard().press("Escape");
    page.unroute("**/api/validate");
    button("The check failed; check again").click();
    assertThat(button("No problems")).isVisible();
  }

  private static List<String> entries(Download download) throws IOException {
    List<String> names = new ArrayList<>();
    try (InputStream in = Files.newInputStream(download.path());
        ZipInputStream zip = new ZipInputStream(in, StandardCharsets.UTF_8)) {
      for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
        names.add(entry.getName());
      }
    }
    return names;
  }
}
