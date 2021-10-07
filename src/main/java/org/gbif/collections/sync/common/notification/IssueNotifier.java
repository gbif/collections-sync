package org.gbif.collections.sync.common.notification;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.clients.proxy.NotificationProxyClient;
import org.gbif.collections.sync.config.SyncConfig;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

/** Factory to create {@link Issue}. */
@Slf4j
public abstract class IssueNotifier {

  protected static final String NEW_LINE = "\n";
  protected static final String CODE_SEPARATOR = "```";
  private static final String FAILS_TITLE =
      "Some operations have failed updating the registry in the %s";
  private static final String FAIL_LABEL = "%s fail";

  protected static final UnaryOperator<String> PORTAL_URL_NORMALIZER =
      url -> {
        if (url != null && url.endsWith("/")) {
          return url.substring(0, url.length() - 1);
        }
        return url;
      };

  protected SyncConfig.NotificationConfig notificationConfig;
  protected final NotificationProxyClient notificationProxyClient;
  protected final String syncTimestampLabel;
  private final String registryInstitutionLink;
  private final String registryCollectionLink;
  private final String registryPersonLink;

  protected IssueNotifier(SyncConfig config) {
    this.notificationConfig = config.getNotification();
    this.registryInstitutionLink =
        PORTAL_URL_NORMALIZER.apply(notificationConfig.getRegistryPortalUrl()) + "/institution/%s";
    this.registryCollectionLink =
        PORTAL_URL_NORMALIZER.apply(notificationConfig.getRegistryPortalUrl()) + "/collection/%s";
    this.registryPersonLink =
        PORTAL_URL_NORMALIZER.apply(notificationConfig.getRegistryPortalUrl()) + "/person/%s";
    syncTimestampLabel =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    notificationProxyClient = NotificationProxyClient.create(config);

    log.info("Issue factory created with sync timestamp label: {}", syncTimestampLabel);
  }

  public void createFailsNotification(List<SyncResult.FailedAction> fails) {
    // create body
    StringBuilder body = new StringBuilder();
    body.append("The next operations have failed when updating the registry during the ")
        .append(getProcessName())
        .append(". Please check them: ");

    fails.forEach(
        f ->
            body.append(NEW_LINE)
                .append(CODE_SEPARATOR)
                .append(NEW_LINE)
                .append("Error: ")
                .append(f.getMessage())
                .append(NEW_LINE)
                .append("Entity: ")
                .append(f.getEntity())
                .append(NEW_LINE)
                .append(CODE_SEPARATOR)
                .append(NEW_LINE));

    Issue issue =
        Issue.builder()
            .title(String.format(FAILS_TITLE, getProcessName()))
            .body(body.toString())
            .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
            .labels(
                Sets.newHashSet(String.format(FAIL_LABEL, getProcessName()), syncTimestampLabel))
            .build();

    notificationProxyClient.sendNotification(issue);
  }

  protected static String formatEntity(Object entity) {
    return NEW_LINE
        + CODE_SEPARATOR
        + NEW_LINE
        + entity.toString()
        + NEW_LINE
        + CODE_SEPARATOR
        + NEW_LINE;
  }

  protected <T> String formatRegistryEntities(List<T> entities, Function<T, String> keyExtractor) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < entities.size(); i++) {
      T entity = entities.get(i);
      sb.append(i + 1)
          .append(". ")
          .append(createRegistryLink(keyExtractor.apply(entity), entity))
          .append(NEW_LINE);
    }

    sb.append(NEW_LINE).append(CODE_SEPARATOR).append(NEW_LINE);

    for (int i = 0; i < entities.size(); i++) {
      T entity = entities.get(i);
      sb.append(i + 1).append(". ").append(entity.toString()).append(NEW_LINE);
    }

    sb.append(CODE_SEPARATOR).append(NEW_LINE);
    return sb.toString();
  }

  private <T> String createRegistryLink(String id, T entity) {
    String linkTemplate;
    if (entity instanceof Institution) {
      linkTemplate = registryInstitutionLink;
    } else if (entity instanceof Collection) {
      linkTemplate = registryCollectionLink;
    } else {
      linkTemplate = registryPersonLink;
    }

    URI uri = URI.create(String.format(linkTemplate, id));
    return "[" + uri.toString() + "](" + uri.toString() + ")";
  }

  protected abstract String getProcessName();
}
