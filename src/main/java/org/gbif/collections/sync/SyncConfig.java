package org.gbif.collections.sync;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class SyncConfig {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectReader YAML_READER = YAML_MAPPER.readerFor(SyncConfig.class);

  private String registryWsUrl;
  private String registryWsUser;
  private String registryWsPassword;
  private String ihWsUrl;
  private NotificationConfig notification = new NotificationConfig();
  private boolean saveResultsToFile;
  private boolean dryRun;
  private boolean sendNotifications;

  @Getter
  @Setter
  public static class NotificationConfig {
    private String githubWsUrl;
    private String githubUser;
    private String githubPassword;
    private String ihPortalUrl;
    private String registryPortalUrl;
    private Set<String> ghIssuesAssignees;
  }

  public static SyncConfig fromFileName(String configFileName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(configFileName), "Config file path is required");

    File configFile = Paths.get(configFileName).toFile();
    SyncConfig config;
    try {
      config = YAML_READER.readValue(configFile);
    } catch (IOException e) {
      log.error("Couldn't load config from file {}", configFileName, e);
      throw new IllegalArgumentException("Couldn't load config file");
    }

    if (config == null) {
      throw new IllegalArgumentException("Config is empty");
    }

    return validateConfig(config);
  }

  public static SyncConfig fromCliArgs(CliSyncApp.CliArgs args) {
    Objects.requireNonNull(args);

    SyncConfig config = fromFileName(args.getConfPath());

    if (args.getDryRun() != null) {
      config.setDryRun(args.getDryRun());
    }

    if (args.getSendNotifications() != null) {
      config.setSendNotifications(args.getSendNotifications());
    }

    if (config.isSendNotifications() && !isEmptyCollection(args.getGithubAssignees())) {
      config.getNotification().setGhIssuesAssignees(args.getGithubAssignees());
    }

    return config;
  }

  private static SyncConfig validateConfig(SyncConfig config) {
    // do some checks for required fields
    if (Strings.isNullOrEmpty(config.getRegistryWsUrl())
        || Strings.isNullOrEmpty(config.getIhWsUrl())) {
      throw new IllegalArgumentException("Registry and IH WS URLs are required");
    }

    if (!config.isDryRun()
        && (Strings.isNullOrEmpty(config.getRegistryWsUser())
            || Strings.isNullOrEmpty(config.getRegistryWsPassword()))) {
      throw new IllegalArgumentException(
          "Registry WS credentials are required if we are not doing a dry run");
    }

    if (config.isSendNotifications()) {
      validateNotificationConfig(config.getNotification());
    }

    return config;
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

    if (Strings.isNullOrEmpty(notificationConfig.getRegistryPortalUrl())
        || Strings.isNullOrEmpty(notificationConfig.getIhPortalUrl())) {
      throw new IllegalArgumentException("Portal URLs are required");
    }
  }

  private static boolean isEmptyCollection(Collection<String> list) {
    if (list == null || list.isEmpty()) {
      return true;
    }

    return list.stream().allMatch(Strings::isNullOrEmpty);
  }
}
