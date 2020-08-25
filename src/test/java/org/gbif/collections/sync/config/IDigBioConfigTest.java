package org.gbif.collections.sync.config;

import java.nio.file.Paths;
import java.util.Collections;

import org.gbif.collections.sync.CliSyncArgs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests the {@link IDigBioConfig}. */
public class IDigBioConfigTest {

  private static final String CONFIG_TEST_PATH = "src/test/resources/idigbio-config.yaml";

  @Test
  public void loadIDigBioConfigFromFileTest() {
    String path = Paths.get(CONFIG_TEST_PATH).toFile().getAbsolutePath();
    IDigBioConfig config = IDigBioConfig.fromFileName(path);

    assertNotNull(config);
    assertNotNull(config.getSyncConfig().getRegistry().getWsUrl());
    assertTrue(config.getSyncConfig().isSaveResultsToFile());
    assertTrue(config.getSyncConfig().isDryRun());
    assertTrue(config.getSyncConfig().isSendNotifications());
    assertNotNull(config.getSyncConfig().getNotification());
    assertFalse(config.getSyncConfig().getNotification().getGhIssuesAssignees().isEmpty());
    assertNotNull(config.getExportFilePath());
    assertNotNull(config.getIDigBioPortalUrl());
  }

  @Test
  public void loadIDigBioConfigFromCliArgsTest() {
    CliSyncArgs cliArgs = new CliSyncArgs();
    cliArgs.setConfPath(Paths.get(CONFIG_TEST_PATH).toFile().getAbsolutePath());
    cliArgs.setDryRun(false);
    cliArgs.setGithubAssignees(Collections.singleton("test"));

    IDigBioConfig config = IDigBioConfig.fromCliArgs(cliArgs);

    assertNotNull(config);
    assertNotNull(config.getIDigBioPortalUrl());
    assertNotNull(config.getExportFilePath());
    assertNotNull(config.getSyncConfig().getRegistry().getWsUrl());
    assertTrue(config.getSyncConfig().isSaveResultsToFile());
    assertFalse(config.getSyncConfig().isDryRun());
    assertTrue(config.getSyncConfig().isSendNotifications());
    assertNotNull(config.getSyncConfig().getNotification());
    assertEquals(1, config.getSyncConfig().getNotification().getGhIssuesAssignees().size());
    assertTrue(config.getSyncConfig().getNotification().getGhIssuesAssignees().contains("test"));
  }
}
