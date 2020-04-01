package org.gbif.collections.sync;

import java.nio.file.Paths;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests the {@link SyncConfig}. */
public class SyncConfigTest {

  private static final String CONFIG_TEST_PATH = "src/test/resources/sync-config.yaml";

  @Test
  public void loadConfigFromFileTest() {
    String path = Paths.get(CONFIG_TEST_PATH).toFile().getAbsolutePath();
    SyncConfig config = SyncConfig.fromFileName(path);

    assertNotNull(config);
    assertNotNull(config.getRegistryWsUrl());
    assertNotNull(config.getIhWsUrl());
    assertTrue(config.isSaveResultsToFile());
    assertTrue(config.isDryRun());
    assertTrue(config.isSendNotifications());
    assertNotNull(config.getNotification());
    assertFalse(config.getNotification().getGhIssuesAssignees().isEmpty());
  }

  @Test
  public void loadConfigFromCliArgsTest() {
    CliSyncApp.CliArgs cliArgs = new CliSyncApp.CliArgs();
    cliArgs.setConfPath(Paths.get(CONFIG_TEST_PATH).toFile().getAbsolutePath());
    cliArgs.setDryRun(false);
    cliArgs.setGithubAssignees(Collections.singleton("test"));

    SyncConfig config = SyncConfig.fromCliArgs(cliArgs);

    assertNotNull(config);
    assertNotNull(config.getRegistryWsUrl());
    assertNotNull(config.getIhWsUrl());
    assertTrue(config.isSaveResultsToFile());
    assertFalse(config.isDryRun());
    assertTrue(config.isSendNotifications());
    assertNotNull(config.getNotification());
    assertEquals(1, config.getNotification().getGhIssuesAssignees().size());
    assertTrue(config.getNotification().getGhIssuesAssignees().contains("test"));
  }
}
