package com.scalar.db.saga.server.docs;

import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaErrorCode.Category;
import io.grpc.Status;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders {@code guides/scalardb-saga-status-codes.md} from {@link SagaErrorCode} and {@link
 * StatusCodeReference}. {@code StatusCodesPageTest} fails when the committed page differs from what
 * this renders; {@code ./gradlew :server:generateStatusCodes} rewrites it.
 */
public final class StatusCodesPage {

  /** The page's path from the repository root. */
  public static final String PATH = "guides/scalardb-saga-status-codes.md";

  private static final Map<Category, String> DESCRIPTIONS = descriptions();

  private static final Map<Integer, String> REASONS =
      Map.of(
          400, "Bad Request",
          401, "Unauthorized",
          403, "Forbidden",
          404, "Not Found",
          409, "Conflict",
          413, "Content Too Large",
          422, "Unprocessable Content",
          429, "Too Many Requests",
          500, "Internal Server Error",
          503, "Service Unavailable");

  private static Map<Category, String> descriptions() {
    Map<Category, String> m = new EnumMap<>(Category.class);
    m.put(
        Category.USER_ERROR,
        "User errors. The request, or the state of the saga it names, is wrong; sending the same"
            + " request again fails the same way.");
    m.put(
        Category.RETRYABLE_SERVER_ERROR,
        "Retryable server errors. The failure is transient; retry the same request with backoff.");
    m.put(
        Category.NON_RETRYABLE_SERVER_ERROR,
        "Non-retryable server errors. Retrying does not help; the server or its data needs"
            + " attention.");
    m.put(
        Category.CLIENT_ERROR,
        "Client errors. The Java client SDK raises them itself, when a call fails without a"
            + " ScalarDB Saga error from the server.");
    return m;
  }

  /**
   * Writes the page.
   *
   * @param args the output path
   * @throws IOException if the page cannot be written
   */
  public static void main(String[] args) throws IOException {
    Files.writeString(Path.of(args[0]), render(), StandardCharsets.UTF_8);
  }

  /**
   * Renders the whole page.
   *
   * @return the page's Markdown
   */
  public static String render() {
    StringBuilder sb = new StringBuilder();
    sb.append(
        """
        <!-- Generated from SagaErrorCode by `./gradlew :server:generateStatusCodes`. Do not edit \
        this file; edit the enum or StatusCodeReference instead. -->

        # ScalarDB Saga Status Codes

        This page lists the status codes that ScalarDB Saga reports. A REST error response carries \
        the code in the `errorCode` field of its body, and a gRPC error carries it as the reason \
        of its `google.rpc.ErrorInfo` detail. The Java client SDK and an embedded engine throw a \
        `SagaRuntimeException` whose `getErrorCode()` returns it.

        The first digit after `DB-SAGA-` is the code's class, which says whether retrying can \
        help. Decide on retries by the class rather than by the HTTP or gRPC status: two codes \
        can share a status and need opposite reactions.

        ## Status code classes and descriptions

        | Class | Description |
        | --- | --- |
        """);
    for (Category category : Category.values()) {
      sb.append("| `DB-SAGA-")
          .append(category.id())
          .append("xxxx` | ")
          .append(DESCRIPTIONS.get(category))
          .append(" |\n");
    }
    for (Category category : Category.values()) {
      List<SagaErrorCode> codes = new ArrayList<>();
      for (SagaErrorCode code : SagaErrorCode.values()) {
        if (code.category() == category && !StatusCodeReference.RESERVED.contains(code)) {
          codes.add(code);
        }
      }
      sb.append("\n## `DB-SAGA-").append(category.id()).append("xxxx` status codes\n\n");
      sb.append("The following are status codes and messages for this class.\n");
      for (SagaErrorCode code : codes) {
        appendCode(sb, code);
      }
    }
    return sb.toString();
  }

  private static void appendCode(StringBuilder sb, SagaErrorCode code) {
    StatusCodeReference.Entry entry = StatusCodeReference.ENTRIES.get(code);
    if (entry == null) {
      throw new IllegalStateException(code + " has no StatusCodeReference entry");
    }
    sb.append("\n### `").append(code.code()).append("`\n\n");
    sb.append("**Message**\n\n```markdown\n").append(code.message()).append("\n```\n\n");
    sb.append("**Status**\n\n").append(status(entry)).append("\n\n");
    sb.append("**Cause**\n\n").append(code.cause()).append("\n\n");
    sb.append("**Action**\n\n").append(code.action()).append('\n');
  }

  private static String status(StatusCodeReference.Entry entry) {
    List<String> parts = new ArrayList<>();
    if (!entry.http().isEmpty()) {
      parts.add(
          "HTTP "
              + entry.http().stream()
                  .map(s -> "`" + s + " " + REASONS.get(s) + "`")
                  .collect(Collectors.joining(" or ")));
    }
    if (!entry.grpc().isEmpty()) {
      parts.add(
          "gRPC "
              + entry.grpc().stream()
                  .map(Status.Code::name)
                  .map(s -> "`" + s + "`")
                  .collect(Collectors.joining(" or ")));
    }
    String statuses = String.join(", ", parts);
    if (entry.note() == null) {
      return statuses;
    }
    return statuses.isEmpty() ? entry.note() : statuses + ". " + entry.note();
  }

  private StatusCodesPage() {}
}
