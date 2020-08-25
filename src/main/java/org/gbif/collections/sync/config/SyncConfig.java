package org.gbif.collections.sync.config;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.gbif.collections.sync.CliSyncArgs;

import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@EqualsAndHashCode
@Slf4j
public class SyncConfig {

  private RegistryConfig registry;
  private NotificationConfig notification;
  private boolean saveResultsToFile;
  private boolean dryRun = true;
  private boolean sendNotifications;

  @Getter
  @Setter
  @EqualsAndHashCode
  public static class RegistryConfig {
    private String wsUrl;
    private String wsUser;
    private String wsPassword;
  }

  @Getter
  @Setter
  @EqualsAndHashCode
  public static class NotificationConfig {
    private String githubWsUrl;
    private String githubUser;
    private String githubPassword;
    private String registryPortalUrl;
    private Set<String> ghIssuesAssignees = new HashSet<>();
  }

  public static void processCliArgs(CliSyncArgs args, SyncConfig syncConfig) {
    Objects.requireNonNull(args);

    if (args.getDryRun() != null) {
      syncConfig.setDryRun(args.getDryRun());
    }

    if (args.getSendNotifications() != null) {
      syncConfig.setSendNotifications(args.getSendNotifications());
    }

    if (syncConfig.isSendNotifications() && !isEmptyCollection(args.getGithubAssignees())) {
      syncConfig.getNotification().setGhIssuesAssignees(args.getGithubAssignees());
    }
  }

  protected static void validateSyncConfig(SyncConfig config) {
    // do some checks for required fields
    if (config.getRegistry() == null || Strings.isNullOrEmpty(config.getRegistry().getWsUrl())) {
      throw new IllegalArgumentException("Registry URL is required");
    }

    if (!config.isDryRun()
        && (config.getRegistry() == null
            || (Strings.isNullOrEmpty(config.getRegistry().getWsUser())
                || Strings.isNullOrEmpty(config.getRegistry().getWsPassword())))) {
      throw new IllegalArgumentException(
          "Registry WS credentials are required if we are not doing a dry run");
    }

    if (config.isSendNotifications()) {
      validateNotificationConfig(config.getNotification());
    }
  }

  private static void validateNotificationConfig(NotificationConfig notificationConfig) {
    if (Strings.isNullOrEmpty(notificationConfig.getGithubWsUrl())) {
      throw new IllegalArgumentException(
          "Github API URL is required if we are sending notifications");
    }

    if (!notificationConfig.getGithubWsUrl().endsWith("/")) {
      throw new IllegalArgumentException("Github API URL must finish with a /");
    }

    if (Strings.isNullOrEmpty(notificationConfig.getGithubUser())
        || Strings.isNullOrEmpty(notificationConfig.getGithubPassword())) {
      throw new IllegalArgumentException(
          "Github credentials are required if we are sending notifications");
    }
  }

  protected static boolean isEmptyCollection(Collection<String> list) {
    if (list == null || list.isEmpty()) {
      return true;
    }

    return list.stream().allMatch(Strings::isNullOrEmpty);
  }
}
