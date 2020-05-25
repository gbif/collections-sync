package org.gbif.collections.sync;

import java.nio.file.Paths;
import java.util.Collections;

import org.gbif.collections.sync.config.IHConfig;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests the {@link IHConfig}. */
public class IHConfigTest {

  private static final String CONFIG_TEST_PATH = "src/test/resources/ih-config.yaml";

  @Test
  public void loadIHConfigFromFileTest() {
    String path = Paths.get(CONFIG_TEST_PATH).toFile().getAbsolutePath();
    IHConfig config = IHConfig.fromFileName(path);

    assertNotNull(config);
    assertNotNull(config.getSyncConfig().getRegistryWsUrl());
    assertTrue(config.getSyncConfig().isSaveResultsToFile());
    assertTrue(config.getSyncConfig().isDryRun());
    assertTrue(config.getSyncConfig().isSendNotifications());
    assertNotNull(config.getSyncConfig().getNotification());
    assertFalse(config.getSyncConfig().getNotification().getGhIssuesAssignees().isEmpty());
    assertNotNull(config.getIhWsUrl());
    assertNotNull(config.getIhWsUrl());
    assertNotNull(config.getIhPortalUrl());
  }

  @Test
  public void loadIHConfigFromCliArgsTest() {
    CliSyncArgs cliArgs = new CliSyncArgs();
    cliArgs.setConfPath(Paths.get(CONFIG_TEST_PATH).toFile().getAbsolutePath());
    cliArgs.setDryRun(false);
    cliArgs.setGithubAssignees(Collections.singleton("test"));

    IHConfig config = IHConfig.fromCliArgs(cliArgs);

    assertNotNull(config);
    assertNotNull(config.getSyncConfig().getRegistryWsUrl());
    assertNotNull(config.getIhWsUrl());
    assertTrue(config.getSyncConfig().isSaveResultsToFile());
    assertFalse(config.getSyncConfig().isDryRun());
    assertTrue(config.getSyncConfig().isSendNotifications());
    assertNotNull(config.getSyncConfig().getNotification());
    assertEquals(1, config.getSyncConfig().getNotification().getGhIssuesAssignees().size());
    assertTrue(config.getSyncConfig().getNotification().getGhIssuesAssignees().contains("test"));
  }
}
