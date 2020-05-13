package org.gbif.collections.sync.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;

import org.gbif.collections.sync.CliSyncArgs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.config.SyncConfig.processCliArgs;
import static org.gbif.collections.sync.config.SyncConfig.validateSyncConfig;

@Getter
@Setter
@Slf4j
public class IDigBioConfig {

  protected static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  protected static final ObjectReader YAML_READER = YAML_MAPPER.readerFor(IDigBioConfig.class);

  private SyncConfig syncConfig;
  private String exportFilePath;

  @JsonProperty("iDigBioPortalUrl")
  private String iDigBioPortalUrl;

  public static IDigBioConfig fromFileName(String configFileName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(configFileName), "Config file path is required");

    File configFile = Paths.get(configFileName).toFile();
    IDigBioConfig iDigBioConfig;
    try {
      iDigBioConfig = YAML_READER.readValue(configFile);
    } catch (IOException e) {
      log.error("Couldn't load config from file {}", configFileName, e);
      throw new IllegalArgumentException("Couldn't load config file");
    }

    if (iDigBioConfig == null) {
      throw new IllegalArgumentException("IDigBio Config is empty");
    }

    validateSyncConfig(iDigBioConfig.getSyncConfig());
    validateIDigBioConfig(iDigBioConfig);

    return iDigBioConfig;
  }

  public static IDigBioConfig fromCliArgs(CliSyncArgs args) {
    Objects.requireNonNull(args);
    IDigBioConfig config = fromFileName(args.getConfPath());
    processCliArgs(args, config.getSyncConfig());
    return config;
  }

  private static void validateIDigBioConfig(IDigBioConfig iDigBioConfig) {
    if (Strings.isNullOrEmpty(iDigBioConfig.getExportFilePath())) {
      throw new IllegalArgumentException("iDigBio export file is required");
    }

    if (Strings.isNullOrEmpty(iDigBioConfig.getIDigBioPortalUrl())) {
      throw new IllegalArgumentException("iDigBio portal URL is required");
    }
  }
}
