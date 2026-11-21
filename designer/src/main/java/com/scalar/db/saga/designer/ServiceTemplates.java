package com.scalar.db.saga.designer;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * {@code <service>.properties} templates for the services a definition names, and the zip bundle
 * laid out like the server's configuration folder.
 *
 * <p>A template leaves {@code base_url} blank on purpose. Deployed unfilled, it fails the server's
 * reload with an error naming the file and the key, rather than sending calls to a placeholder
 * host.
 */
final class ServiceTemplates {

  // Names that are safe as one file name and one zip entry name. A service named otherwise gets
  // no template, so a name can never place a file outside the services folder.
  private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9._-]{0,127}");

  private ServiceTemplates() {}

  /** One service's template; {@code text} is empty when the name is not usable as a file name. */
  record Template(String service, String fileName, String text, boolean usable) {}

  /** Returns the service names a definition's steps use, in order of first use. */
  static List<String> services(JsonNode definition) {
    Set<String> services = new LinkedHashSet<>();
    for (JsonNode step : definition.path("steps")) {
      JsonNode service = step.get("service");
      if (service != null && service.isTextual() && !service.asText().isBlank()) {
        services.add(service.asText());
      }
    }
    return List.copyOf(services);
  }

  /** Returns the template for one service of the named definition. */
  static Template template(String service, String definitionName) {
    String fileName = service + ".properties";
    if (!SAFE_NAME.matcher(service).matches()) {
      return new Template(service, fileName, "", false);
    }
    String text =
        "# Service file for '"
            + service
            + "', used by "
            + definitionName.replaceAll("[\\r\\n]", " ")
            + ".\n"
            + "# Put it in the server's services_path as "
            + fileName
            + ".\n"
            + "\n"
            + "# Required: the service's base URL, http or https.\n"
            + "base_url=\n"
            + "\n"
            + "# Optional: the hosts calls to this service may reach, separated by commas.\n"
            + "# allowed_hosts="
            + service
            + ".internal\n"
            + "\n"
            + "# Optional: the largest request or response body, in bytes (default 1 MiB).\n"
            + "# max_body_bytes=1048576\n"
            + "\n"
            + "# Optional: a header sent with every call, such as a credential read from a file.\n"
            + "# header.Authorization=${file:UTF-8:/run/secrets/"
            + service
            + "/authorization}\n";
    return new Template(service, fileName, text, true);
  }

  /**
   * Builds a zip holding {@code definitions/<definitionFile>} and {@code
   * services/<service>.properties} for each usable template given.
   */
  static byte[] bundle(String definitionFile, String definitionText, List<Template> templates) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream zip = new ZipOutputStream(bytes, StandardCharsets.UTF_8)) {
      add(zip, "definitions/" + definitionFile, definitionText);
      for (Template template : templates) {
        if (template.usable()) {
          add(zip, "services/" + template.fileName(), template.text());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to build a zip in memory", e);
    }
    return bytes.toByteArray();
  }

  private static void add(ZipOutputStream zip, String name, String text) throws IOException {
    ZipEntry entry = new ZipEntry(name);
    // A fixed time, so the same input always gives the same bytes.
    entry.setTime(0L);
    zip.putNextEntry(entry);
    zip.write(text.getBytes(StandardCharsets.UTF_8));
    zip.closeEntry();
  }
}
