package org.gbif.collections.sync.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;

import org.gbif.collections.sync.CliSyncArgs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.config.SyncConfig.processCliArgs;
import static org.gbif.collections.sync.config.SyncConfig.validateSyncConfig;

@Getter
@Setter
@EqualsAndHashCode
@Slf4j
public class IHConfig {

  protected static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  protected static final ObjectReader YAML_READER = YAML_MAPPER.readerFor(IHConfig.class);

  private SyncConfig syncConfig;
  private String ihWsUrl;
  private String ihPortalUrl;

  public static IHConfig fromFileName(String configFileName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(configFileName), "Config file path is required");

    File configFile = Paths.get(configFileName).toFile();
    IHConfig ihConfig;
    try {
      ihConfig = YAML_READER.readValue(configFile);
    } catch (IOException e) {
      log.error("Couldn't load config from file {}", configFileName, e);
      throw new IllegalArgumentException("Couldn't load config file");
    }

    if (ihConfig == null) {
      throw new IllegalArgumentException("IH Config is empty");
    }

    validateSyncConfig(ihConfig.getSyncConfig());
    validateIhConfig(ihConfig);

    return ihConfig;
  }

  public static IHConfig fromCliArgs(CliSyncArgs args) {
    Objects.requireNonNull(args);
    IHConfig config = fromFileName(args.getConfPath());
    processCliArgs(args, config.getSyncConfig());
    return config;
  }

  private static void validateIhConfig(IHConfig ihConfig) {
    if (Strings.isNullOrEmpty(ihConfig.getIhWsUrl())) {
      throw new IllegalArgumentException("IH WS URL is required");
    }

    if (Strings.isNullOrEmpty(ihConfig.getIhPortalUrl())) {
      throw new IllegalArgumentException("IH portal URL is required");
    }
  }
}
