package com.scalar.db.saga.designer;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;

import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.AriaRole;
import org.junit.jupiter.api.Test;

class SmokeUiTest extends UiTestSupport {

  @Test
  void open_definitionInTheWorkspace_showsItsSteps() throws Exception {
    // Arrange
    copyGettingStarted("order-saga-pivot.json");

    // Act
    page.navigate(url());
    page.getByRole(
            AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Open order-saga-pivot.json"))
        .click();

    // Assert
    assertThat(page.getByLabel("Steps")).containsText("charge");
    assertThat(page.getByLabel("Saga name")).hasValue("order-saga-pivot");
  }
}
