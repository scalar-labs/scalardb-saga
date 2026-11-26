package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import com.scalar.db.saga.designer.ServiceTemplates.Template;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.Test;

class ServiceTemplatesTest {

  @Test
  void services_definitionGiven_returnsServiceNamesInOrderOfFirstUse() throws Exception {
    // Arrange
    JsonNode definition =
        DefinitionFormats.read(
            "{\"steps\": [{\"service\": \"payment\"}, {\"service\": \"inventory\"},"
                + " {\"service\": \"payment\"}, {\"stepClass\": \"x.Y\"}, {\"service\": \" \"}, 3]}",
            Format.JSON);

    // Act Assert
    assertThat(ServiceTemplates.services(definition)).containsExactly("payment", "inventory");
  }

  @Test
  void template_usableNameGiven_leavesBaseUrlBlankAndCommentsTheOptionalKeys() {
    // Act
    Template template = ServiceTemplates.template("payment", "order-saga");

    // Assert
    assertThat(template.usable()).isTrue();
    assertThat(template.fileName()).isEqualTo("payment.properties");
    assertThat(template.text())
        .contains("used by order-saga.")
        .contains("\nbase_url=\n")
        .contains("\n# allowed_hosts=payment.internal\n")
        .contains("\n# max_body_bytes=1048576\n")
        .contains("\n# header.Authorization=${file:UTF-8:/run/secrets/payment/authorization}\n");
  }

  @Test
  void template_definitionNameWithLineBreaks_keepsTheCommentOnOneLine() {
    // Act
    Template template = ServiceTemplates.template("payment", "evil\r\nbase_url=http://x");

    // Assert
    assertThat(template.text()).doesNotContain("\nbase_url=http://x");
  }

  @Test
  void template_nameUnusableAsAFileNameGiven_isMarkedUnusableWithNoText() {
    // Act
    Template template = ServiceTemplates.template("../etc/passwd", "n");

    // Assert
    assertThat(template.usable()).isFalse();
    assertThat(template.text()).isEmpty();
  }

  @Test
  void bundle_templatesGiven_zipsTheDefinitionAndUsableTemplatesOnly() throws Exception {
    // Arrange
    List<Template> templates =
        List.of(ServiceTemplates.template("payment", "n"), ServiceTemplates.template("../x", "n"));

    // Act
    byte[] zip = ServiceTemplates.bundle("n.json", "{}\n", templates);

    // Assert
    List<String> names = new ArrayList<>();
    try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zip))) {
      for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
        names.add(entry.getName());
        if (entry.getName().equals("definitions/n.json")) {
          assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("{}\n");
        }
      }
    }
    assertThat(names).containsExactly("definitions/n.json", "services/payment.properties");
  }

  @Test
  void bundle_sameInputTwice_givesTheSameBytes() {
    // Arrange
    List<Template> templates = List.of(ServiceTemplates.template("payment", "n"));

    // Act Assert
    assertThat(ServiceTemplates.bundle("n.json", "{}", templates))
        .isEqualTo(ServiceTemplates.bundle("n.json", "{}", templates));
  }
}
