package com.scalar.db.saga.designer;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.FilePayload;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class FilesUiTest extends UiTestSupport {

  @Test
  void load_workspaceWithAnUnreadableFile_listsEveryFileAndTheError() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    write("broken.json", "{oops");

    // Act
    page.navigate(url());

    // Assert
    assertThat(page.locator(".file-table tbody tr")).hasCount(2);
    assertThat(button("Open order-saga.json")).isVisible();
    assertThat(page.locator(".err-text")).containsText("Unexpected character");
  }

  @Test
  void newDefinition_builtWithTheFormAndSaved_writesTheFile() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    button("New definition").click();
    page.getByRole(AriaRole.DIALOG).getByLabel("Saga name").fill("order");
    button("Create").click();
    button("Add a step").click();
    page.getByLabel("Step name").fill("charge");
    page.getByLabel("Service").fill("payment");
    region("Execution").getByLabel("Path").fill("/charge");
    region("Compensation").getByLabel("Path").fill("/refund");
    assertThat(button("No problems")).isVisible();
    button("Save").click();

    // Assert
    assertThat(toast()).containsText("Saved order.json");
    JsonNode saved = DefinitionFormats.read(read("order.json"), Format.JSON);
    assertThat(saved.get("name").asText()).isEqualTo("order");
    assertThat(saved.at("/steps/0/execution/path").asText()).isEqualTo("/charge");
    assertThat(saved.at("/steps/0/compensation/path").asText()).isEqualTo("/refund");
  }

  @Test
  void newDefinition_nameOfAnExistingFile_cannotBeCreated() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    page.navigate(url());

    // Act
    button("New definition").click();
    page.getByRole(AriaRole.DIALOG).getByLabel("Saga name").fill("order-saga");

    // Assert
    assertThat(button("Create")).isDisabled();
    assertThat(page.getByRole(AriaRole.ALERT)).containsText("already exists");
  }

  @Test
  void importFile_yamlUploaded_opensItAndSavesItIntoTheWorkspace() throws Exception {
    // Arrange
    String yaml =
        """
        name: imported
        version: "1.10"
        steps:
          - name: charge
            service: payment
            execution: {method: POST, path: /charge}
            compensation: {method: POST, path: /refund}
        """;
    page.navigate(url());

    // Act
    page.getByLabel("Import a definition file")
        .setInputFiles(
            new FilePayload(
                "imported.yaml", "application/yaml", yaml.getBytes(StandardCharsets.UTF_8)));
    assertThat(page.getByLabel("Saga name")).hasValue("imported");
    button("Save").click();

    // Assert
    assertThat(toast()).containsText("Saved imported.yaml");
    assertThat(read("imported.yaml")).contains("version: \"1.10\"");
  }
}
