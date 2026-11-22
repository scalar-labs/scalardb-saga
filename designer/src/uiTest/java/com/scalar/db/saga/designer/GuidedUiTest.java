package com.scalar.db.saga.designer;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.AriaRole;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The guided parts: the key picker, samples and capture, recovery strategy, pivot and mode. */
class GuidedUiTest extends UiTestSupport {

  private static final String ONE_STEP =
      """
      {
        "name": "order",
        "version": "1.9",
        "steps": [
          {
            "name": "charge",
            "service": "payment",
            "execution": {"method": "POST", "path": "/charge", "jsonBody": {"orderId": "${orderId}"}},
            "compensation": {"method": "POST", "path": "/refund"}
          }
        ]
      }
      """;

  @Test
  void open_keysReadButProducedByNoStep_becomeInputKeys() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");

    // Act
    open("order-saga.json");

    // Assert
    assertThat(region("Sample input").getByLabel("Input keys"))
        .hasText("orderIdamountitemquantity");
    assertThat(region("Execution").getByLabel("charge execution body field 1 context key"))
        .hasValue("orderId");
    assertThat(page.locator(".dirty")).hasCount(0);
  }

  @Test
  void reorder_stepMovedAboveTheOneItReadsFrom_flagsTheKeyAndKeepsIt() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");

    // Act
    button("Move ship up").click();
    button("Move ship up").click();
    cards().first().click();

    // Assert
    Locator execution = region("Execution");
    assertThat(execution.getByLabel("ship execution body field 2 context key"))
        .hasValue("paymentId");
    assertThat(execution.getByRole(AriaRole.NOTE).first()).containsText("not available here");
  }

  @Test
  void compensation_ownOutput_isListedButCannotBePicked() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");

    // Act
    open("order-saga.json");

    // Assert
    Locator own =
        region("Compensation")
            .getByLabel("charge compensation body field 1 context key")
            .locator("option", new Locator.LocatorOptions().setHasText("this step's own output"));
    assertThat(own).hasText("paymentId · this step's own output");
    assertThat(own).isDisabled();
  }

  @Test
  void compensation_ownOutput_isNotOfferedForThePath() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");

    // Act
    open("order-saga.json");

    // Assert
    Locator chips = page.getByLabel("charge compensation: keys to insert");
    assertThat(chips).containsText("${orderId}");
    assertThat(chips).not().containsText("${paymentId}");
  }

  @Test
  void capture_fieldOfTheSampleResponse_addsAnOutputUnderAFreeKeyAndRemovesItAgain()
      throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    region("Execution")
        .getByLabel("charge execution sample response")
        .fill("{\"payment_id\": \"p-1\", \"amount\": 100}");
    Locator capture = button("Capture $.amount");

    // Act
    capture.click();

    // Assert
    assertThat(capture).hasText("Captured as chargeAmount");
    assertThat(region("Execution").getByLabel("Output 2: expression")).hasValue("$.amount");
    capture.click();
    assertThat(region("Execution").getByLabel("Output 2: expression")).hasCount(0);
  }

  @Test
  void capture_fieldsTheEngineCannotRead_areShownWithTheReason() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");

    // Act
    region("Execution")
        .getByLabel("charge execution sample response")
        .fill("{\"payment_id\": \"p-1\", \"a.b\": 1, \"items\": [{\"sku\": \"x\"}]}");

    // Assert
    Locator tree = page.getByLabel("charge execution sample fields");
    assertThat(tree).containsText("A field name with a dot cannot be captured");
    assertThat(tree).containsText("Items of a list cannot be captured");
    assertThat(button("Capture $.items")).isVisible();
  }

  @Test
  void output_expressionMissingFromTheSample_isFlagged() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");

    // Act
    region("Execution").getByLabel("charge execution sample response").fill("{\"id\": \"p-1\"}");

    // Assert
    assertThat(region("Execution").getByRole(AriaRole.NOTE).last())
        .containsText("Missing from the sample response");
  }

  @Test
  void save_samples_writesThemBesideTheDefinitionAndOpensThemAgain() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    region("Execution").getByLabel("charge execution sample response").fill("{\"payment_id\": 1}");
    page.getByRole(AriaRole.TEXTBOX, new Page.GetByRoleOptions().setName("Sample input"))
        .fill("{\"orderId\": \"o-1\", \"amount\": 5, \"item\": \"a\", \"quantity\": 1}");

    // Act
    button("Save").click();
    assertThat(toast()).containsText("samples saved");
    open("order-saga.json");

    // Assert
    assertThat(read(".designer/order-saga.json")).contains("payment_id").contains("o-1");
    assertThat(region("Execution").getByLabel("charge execution sample response"))
        .hasValue("{\"payment_id\": 1}");
    assertThat(page.locator(".dirty")).hasCount(0);
  }

  @Test
  void save_definitionOnlyEditWithTheSamplesAlreadyOnDisk_leavesNoDraft() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    page.getByLabel("Saga name").fill("first");
    button("Save").click();
    assertThat(page.locator(".dirty")).hasCount(0);
    page.getByLabel("Saga name").fill("second");
    assertThat(page.locator(".dirty")).isVisible();

    // Act
    button("Save").click();
    assertThat(page.locator(".dirty")).hasCount(0);
    assertThat(read("order-saga.json")).contains("\"second\"");
    write("order-saga.json", read("order-saga.json").replace("\"second\"", "\"pulled\""));
    open("order-saga.json");

    // Assert
    assertThat(page.getByLabel("Saga name")).hasValue("pulled");
    assertThat(page.getByRole(AriaRole.DIALOG)).hasCount(0);
  }

  @Test
  void withInputKeys_existingSample_addsTheKeysAndKeepsTheRestAsWritten() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object samples =
        page.evaluate(
            """
            async () => {
              const { withInputKeys } = await import("/context.js");
              return [
                withInputKeys('{"amount": 12345678901234567890, "rate": 1.10}', ["customerId"]),
                withInputKeys("{ }", ["toString"]),
                withInputKeys('{"a": 1}', ["a"]),
                withInputKeys(null, ["a"]),
                withInputKeys("[1]", ["a"]),
              ];
            }
            """);

    // Assert
    assertThat(String.valueOf(samples))
        .isEqualTo(
            "[{\"amount\": 12345678901234567890, \"rate\": 1.10,\n  \"customerId\": null\n},"
                + " {\n  \"toString\": null\n}, {\"a\": 1}, {\n  \"a\": null\n}, null]");
  }

  @Test
  void sampleHas_inheritedPropertyName_isMissing() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object has =
        page.evaluate(
            """
            async () => {
              const { sampleHas } = await import("/context.js");
              return [sampleHas({}, "$.constructor"), sampleHas({ a: { b: 1 } }, "$.a.b")];
            }
            """);

    // Assert
    assertThat(String.valueOf(has)).isEqualTo("[false, true]");
  }

  @Test
  void switchMode_targetPhaseAlreadyHeldAndClassStepPivot_namesBothAsDropped() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object result =
        page.evaluate(
            """
            async () => {
              const { switchMode } = await import("/model.js");
              const call = (path) => ({ method: "POST", path });
              const model = { mode: "SAGA", recoveryStrategy: null, steps: [
                { name: "a", pivot: null,
                  phases: { execution: call("/a"), compensation: call("/u"), reservation: call("/old") } },
                { name: "b", stepClass: "com.example.B", pivot: true, phases: {} },
              ] };
              const dropped = switchMode(model, "TCC");
              return [dropped, model.steps[0].phases.reservation.path, model.steps[1].pivot];
            }
            """);

    // Assert
    assertThat(String.valueOf(result))
        .isEqualTo(
            "[[the reservation call of a, replaced by its execution, the pivot mark on b], /a,"
                + " null]");
  }

  @Test
  void recoveryStrategy_mixedChosen_marksAPivotAndWritesIt() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");

    // Act
    page.getByLabel("MIXED").check();

    // Assert
    assertThat(page.getByRole(AriaRole.SEPARATOR)).hasAccessibleName("Point of no return");
    cards().last().click();
    assertThat(region("Compensation").getByRole(AriaRole.NOTE).first()).containsText("Never runs");
    button("Save").click();
    assertThat(toast()).containsText("Saved");
    JsonNode saved = DefinitionFormats.read(read("order-saga.json"), Format.JSON);
    assertThat(saved.get("recoveryStrategy").asText()).isEqualTo("MIXED");
    assertThat(saved.at("/steps/1/pivot").asBoolean()).isTrue();
  }

  @Test
  void recoveryStrategy_switchedAwayFromMixed_clearsThePivot() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");

    // Act
    page.getByLabel("BACKWARD").check();

    // Assert
    assertThat(page.getByRole(AriaRole.SEPARATOR)).hasCount(0);
    assertThat(button("No problems")).isVisible();
    button("Save").click();
    assertThat(toast()).containsText("Saved");
    JsonNode saved = DefinitionFormats.read(read("order-saga-pivot.json"), Format.JSON);
    assertThat(saved.has("recoveryStrategy")).isFalse();
    assertThat(saved.get("steps").findValues("pivot")).isEmpty();
  }

  @Test
  void pivot_checkedOnAnotherStep_movesTheMark() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");
    page.getByLabel("MIXED").check();

    // Act
    cards().first().click();
    page.getByRole(AriaRole.CHECKBOX, new Page.GetByRoleOptions().setName("Pivot")).check();

    // Assert
    assertThat(page.locator(".card .chip.warn")).hasCount(1);
    assertThat(page.locator(".card-row.after-pivot")).hasCount(2);
  }

  @Test
  void mode_switchedToTcc_mapsThePhasesAndNamesWhatItDrops() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");
    open("order-saga-pivot.json");

    // Act
    page.getByLabel("Mode").selectOption("TCC");

    // Assert
    Locator dialog = page.getByRole(AriaRole.DIALOG);
    assertThat(dialog).containsText("the recovery strategy MIXED");
    assertThat(dialog).containsText("the pivot mark on");
    button("Switch to TCC").click();
    assertThat(region("Reservation").getByLabel("Path", exact())).hasValue("/charge");
    assertThat(region("Cancellation").getByLabel("Path", exact())).hasValue("/refund");
    assertThat(region("Confirmation")).containsText("no confirmation call");
    assertThat(region("When a step fails")).hasCount(0);
  }

  @Test
  void mode_switchCancelled_changesNothing() throws Exception {
    // Arrange
    copyGettingStarted("order-saga.json");
    open("order-saga.json");

    // Act
    page.getByLabel("Mode").selectOption("TCC");
    button("Cancel").click();

    // Assert
    assertThat(page.getByLabel("Mode")).hasValue("SAGA");
    assertThat(region("Execution")).isVisible();
    assertThat(page.locator(".dirty")).hasCount(0);
  }

  @Test
  void bumpVersion_contentChangedUnderTheSameVersion_raisesTheLastNumberAndFocusesIt()
      throws Exception {
    // Arrange
    write("order.json", ONE_STEP);
    open("order.json");
    region("Execution").getByLabel("Path", exact()).fill("/charges");

    // Act
    button("Bump version to 1.10").click();

    // Assert
    assertThat(page.getByLabel("Version")).hasValue("1.10");
    assertThat(page.getByLabel("Version")).isFocused();
    assertThat(page.getByText("the version did not")).hasCount(0);
  }

  @Test
  void bumpVersion_variousVersions_raisesTheLastNumberKeepingItsWidth() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object bumped =
        page.evaluate(
            """
            async (versions) => {
              const { bumpVersion } = await import("/model.js");
              return versions.map(bumpVersion);
            }
            """,
            List.of("1.9", "v2", "rc09", "1.0-beta", "beta", "99999999999999999999"));

    // Assert
    assertThat(String.valueOf(bumped))
        .isEqualTo("[1.10, v3, rc10, 1.1-beta, null, 100000000000000000000]");
  }

  @Test
  void suggestKey_fieldNames_becomeCamelCaseKeys() throws Exception {
    // Arrange
    page.navigate(url());

    // Act
    Object keys =
        page.evaluate(
            """
            async (names) => {
              const { suggestKey, collisionFreeKey } = await import("/context.js");
              return [
                ...names.map(suggestKey),
                collisionFreeKey("amount", "charge", ["amount"]),
                collisionFreeKey("amount", "charge", ["amount", "chargeAmount"]),
              ];
            }
            """,
            List.of("inventory_id", "tracking-no", "paymentID", "2fa", "---"));

    // Assert
    assertThat(String.valueOf(keys))
        .isEqualTo(
            "[inventoryId, trackingNo, paymentId, field2fa, value, chargeAmount,"
                + " chargeAmount2]");
  }

  private static Locator.GetByLabelOptions exact() {
    return new Locator.GetByLabelOptions().setExact(true);
  }
}
