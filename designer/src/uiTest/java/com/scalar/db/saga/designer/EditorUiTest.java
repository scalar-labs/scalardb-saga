package com.scalar.db.saga.designer;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.options.AriaRole;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.util.List;
import org.junit.jupiter.api.Test;

class EditorUiTest extends UiTestSupport {

  private static final String WITH_EXTRAS =
      """
      {"name": "order", "version": "1", "steps": [{"name": "charge", "service": "payment",
        "colour": "blue",
        "execution": {"method": "POST", "path": "/charge"},
        "compensation": {"method": "POST", "path": "/refund"}}]}
      """;

  @Test
  void open_definition_showsEveryStepAndTheFirstStepsCalls() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");

    // Act
    open("order-saga-pivot.json");

    // Assert
    assertThat(cards()).hasText(new String[] {"charge", "reserve", "ship"});
    assertThat(page.getByLabel("Step name")).hasValue("charge");
    assertThat(region("Execution").getByLabel("Path")).hasValue("/charge");
    assertThat(region("Compensation").getByLabel("Path")).hasValue("/refund");
    assertThat(button("No problems")).isVisible();
  }

  @Test
  void save_withoutEdits_leavesTheFileByteForByte() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    String before = read("order-saga-pivot.json");
    open("order-saga-pivot.json");

    // Act
    button("Save").click();

    // Assert
    assertThat(toast()).containsText("No changes to the definition");
    assertThat(read("order-saga-pivot.json")).isEqualTo(before);
  }

  @Test
  void edit_bodyOnAGet_showsTheProblemOnItsStepAndIsNeverWritten() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    String before = read("order-saga-pivot.json");
    open("order-saga-pivot.json");

    // Act
    region("Execution").getByLabel("charge execution method").selectOption("GET");

    // Assert
    assertThat(page.locator(".card").first()).containsText("1 problem");
    assertThat(page.locator(".editor .problem")).hasCount(1);
    button("Save").click();
    assertThat(toast()).containsText("Not saved");
    assertThat(read("order-saga-pivot.json")).isEqualTo(before);
  }

  @Test
  void moveDown_firstStep_reordersTheStepsInTheSavedFile() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");

    // Act
    button("Move charge down").click();
    button("Save").click();

    // Assert
    assertThat(toast()).containsText("Saved");
    JsonNode saved = DefinitionFormats.read(read("order-saga.json"), Format.JSON);
    assertThat(saved.at("/steps/0/name").asText()).isEqualTo("reserve");
    assertThat(saved.at("/steps/1/name").asText()).isEqualTo("charge");
  }

  @Test
  void drag_lastStepOntoTheFirst_movesItThere() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");

    // Act
    page.locator(".card").nth(2).dragTo(page.locator(".card").nth(0));

    // Assert
    assertThat(cards()).hasText(new String[] {"ship", "charge", "reserve"});
  }

  @Test
  void undo_afterADelete_putsTheStepBackWhereItWas() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");
    page.locator(".card").nth(1).click();

    // Act
    button("Delete").click();
    assertThat(cards()).hasText(new String[] {"charge", "ship"});
    button("Undo").click();

    // Assert
    assertThat(cards()).hasText(new String[] {"charge", "reserve", "ship"});
    assertThat(page.getByLabel("Step name")).hasValue("reserve");
  }

  @Test
  void duplicate_firstStep_insertsACopyWithAnUnusedName() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");

    // Act
    button("Duplicate").click();

    // Assert
    assertThat(cards()).hasText(new String[] {"charge", "charge-2", "reserve", "ship"});
    assertThat(page.getByLabel("Step name")).hasValue("charge-2");
  }

  @Test
  void save_fixedNumberInTheBody_writesItsExactText() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");
    var execution = region("Execution");

    // Act
    execution.getByText("Add field").click();
    execution.getByLabel("charge execution body field 3 name").fill("fee");
    execution.getByLabel("charge execution body field 3 kind").selectOption("fixed");
    execution.getByLabel("charge execution body field 3 type").selectOption("number");
    execution.getByLabel("charge execution body field 3 value").fill("100.50");
    button("Save").click();

    // Assert
    assertThat(toast()).containsText("Saved");
    assertThat(read("order-saga-pivot.json")).contains("\"fee\" : 100.50");
  }

  @Test
  void save_nestedObjectInTheBody_writesAnObject() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");
    var execution = region("Execution");

    // Act
    execution.getByText("Add field").first().click();
    execution.getByLabel("charge execution body field 3 name").fill("shipping");
    execution.getByLabel("charge execution body field 3 kind").selectOption("object");
    execution.getByText("Add field").nth(0).click();
    execution.getByLabel("charge execution body field 3 field 1 name").fill("city");
    execution.getByLabel("charge execution body field 3 field 1 context key").fill("city");
    button("Save").click();

    // Assert
    assertThat(toast()).containsText("Saved");
    JsonNode saved = DefinitionFormats.read(read("order-saga-pivot.json"), Format.JSON);
    assertThat(saved.at("/steps/0/execution/jsonBody/shipping/city").asText()).isEqualTo("${city}");
  }

  @Test
  void textBody_chosenOverAJsonBody_replacesIt() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");
    var execution = region("Execution");

    // Act
    execution
        .getByRole(AriaRole.BUTTON, new Locator.GetByRoleOptions().setName("Text body"))
        .click();
    execution.getByLabel("charge execution text body").fill("order=${orderId}");
    button("Save").click();

    // Assert
    assertThat(toast()).containsText("Saved");
    JsonNode saved = DefinitionFormats.read(read("order-saga-pivot.json"), Format.JSON);
    assertThat(saved.at("/steps/0/execution").has("jsonBody")).isFalse();
    assertThat(saved.at("/steps/0/execution/stringBody").asText()).isEqualTo("order=${orderId}");
  }

  @Test
  void chip_earlierStepsOutputClicked_insertsItsPlaceholderAtTheCursor() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");
    page.locator(".card").nth(1).click();
    var path = region("Execution").getByLabel("Path", exact());
    path.fill("/reserve/");

    // Act
    region("Execution").getByText("${paymentId}").click();

    // Assert
    assertThat(path).hasValue("/reserve/${paymentId}");
  }

  @Test
  void remove_unknownField_clearsTheProblemAndDropsItFromTheFile() throws Exception {
    // Arrange
    write("order.json", WITH_EXTRAS);
    open("order.json");
    assertThat(page.locator(".editor .problem")).hasCount(1);

    // Act
    button("Remove colour").click();

    // Assert
    assertThat(button("No problems")).isVisible();
    button("Save").click();
    assertThat(toast()).containsText("Saved");
    assertThat(read("order.json")).doesNotContain("colour");
  }

  @Test
  void open_classStep_showsItReadOnly() throws Exception {
    // Arrange
    write(
        "order.json",
        """
        {"name": "order", "steps": [{"name": "audit", "stepClass": "com.example.Audit"}]}
        """);

    // Act
    open("order.json");

    // Assert
    assertThat(page.getByText("A class step")).isVisible();
    assertThat(page.locator(".editor")).containsText("com.example.Audit");
  }

  @Test
  void edit_versionTheFileHeldAsANumber_replacesItWithTheTypedText() throws Exception {
    // Arrange
    write(
        "order.yaml",
        """
        name: order
        version: 1.1
        steps:
          - name: charge
            service: payment
            execution: {method: POST, path: /charge}
            compensation: {method: POST, path: /refund}
        """);
    open("order.yaml");

    // Act
    page.getByLabel("Version").fill("1.10");

    // Assert
    assertThat(button("No problems")).isVisible();
    button("Save").click();
    assertThat(toast()).containsText("Saved");
    assertThat(read("order.yaml")).contains("version: \"1.10\"");
  }

  @Test
  void open_stepsThatIsNotAList_opensAndAddingAStepReplacesIt() throws Exception {
    // Arrange
    write("order.json", "{\"name\": \"order\", \"version\": \"1\", \"steps\": {}}");

    // Act
    open("order.json");
    assertThat(page.getByText("is not a list")).isVisible();
    button("Add a step").click();
    page.getByLabel("Service").fill("payment");
    region("Execution").getByLabel("Path", exact()).fill("/charge");
    region("Compensation").getByLabel("Path", exact()).fill("/refund");

    // Assert
    assertThat(button("No problems")).isVisible();
    button("Save").click();
    assertThat(toast()).containsText("Saved");
    JsonNode saved = DefinitionFormats.read(read("order.json"), Format.JSON);
    assertThat(saved.at("/steps/0/service").asText()).isEqualTo("payment");
  }

  @Test
  void undo_afterANewStepWasAdded_keepsEveryStepIdDistinct() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    button("Add a step").click();
    button("Delete").click();
    button("Add a step").click();

    // Act
    button("Undo").click();
    button("Save").click();

    // Assert
    assertThat(page.locator(".card")).hasCount(5);
    assertThat(toast()).containsText("Not saved: fix");
  }

  @Test
  void undo_afterLeavingTheFile_isNoLongerOffered() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    button("Add a step").click();
    button("Delete").click();
    assertThat(button("Undo")).isVisible();

    // Act
    button("Files").click();

    // Assert
    assertThat(button("Open order-saga.json")).isVisible();
    assertThat(button("Undo")).hasCount(0);
  }

  @Test
  void textBody_chosenWhenTheFileHasBothBodies_keepsTheFilesText() throws Exception {
    // Arrange
    write(
        "order.json",
        """
        {"name": "order", "version": "1", "steps": [{"name": "charge", "service": "payment",
          "execution": {"method": "POST", "path": "/charge", "jsonBody": {"a": "b"},
            "stringBody": "order=${orderId}"},
          "compensation": {"method": "POST", "path": "/refund"}}]}
        """);
    open("order.json");

    // Act
    region("Execution")
        .getByRole(AriaRole.BUTTON, new Locator.GetByRoleOptions().setName("Text body"))
        .click();

    // Assert
    assertThat(region("Execution").getByLabel("charge execution text body"))
        .hasValue("order=${orderId}");
    assertThat(region("Execution").getByLabel("charge execution body field 1 name")).hasCount(0);
  }

  @Test
  void edit_contentUnderTheSameVersion_warnsUntilTheVersionChanges() throws Exception {
    // Arrange
    write("order.json", WITH_EXTRAS.replace("\"colour\": \"blue\",", ""));
    open("order.json");
    var warning = page.getByText("the version did not");

    // Act
    region("Execution").getByLabel("Path", exact()).fill("/charges");

    // Assert
    assertThat(warning).isVisible();
    page.getByLabel("Version").fill("2");
    assertThat(warning).hasCount(0);
  }

  private static Locator.GetByLabelOptions exact() {
    return new Locator.GetByLabelOptions().setExact(true);
  }

  @Test
  void keysFor_laterCaptureOfTheSameKey_replacesTheEarlierOne() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object keys =
        page.evaluate(
            """
            async () => {
              const { keysFor } = await import("/context.js");
              const call = (name, value) => ({ output: [{ name, value }] });
              const model = { steps: [
                { name: "a", phases: { execution: call("id", "$.a"), compensation: call("x", "$.x") } },
                { name: "b", phases: { execution: call("id", "$.b") } },
                { name: "c", phases: { execution: call("other", "$.c") } },
              ] };
              return keysFor(model, 2, "execution");
            }
            """);

    // Assert
    assertThat(String.valueOf(keys)).isEqualTo("[{key=id, from=b}]");
  }

  @Test
  void keysFor_tccReservation_offersOnlyEarlierReservations() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object keys = page.evaluate(TCC_KEYS, List.of(1, "reservation"));

    // Assert
    assertThat(String.valueOf(keys)).isEqualTo("[{key=hold, from=a}]");
  }

  @Test
  void keysFor_tccConfirmation_offersEveryReservationAndEarlierConfirmations() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object keys = page.evaluate(TCC_KEYS, List.of(1, "confirmation"));

    // Assert
    assertThat(String.valueOf(keys))
        .isEqualTo("[{key=hold, from=a}, {key=seat, from=b}, {key=done, from=a}]");
  }

  /** The keys for a call of two TCC steps that each capture a key in both forward phases. */
  private static final String TCC_KEYS =
      """
      async ([index, phase]) => {
        const { keysFor } = await import("/context.js");
        const call = (name) => ({ output: [{ name, value: "$." + name }] });
        const model = { mode: "TCC", steps: [
          { name: "a", phases: { reservation: call("hold"), confirmation: call("done") } },
          { name: "b", phases: { reservation: call("seat"), confirmation: call("paid") } },
        ] };
        return keysFor(model, index, phase);
      }
      """;
}
