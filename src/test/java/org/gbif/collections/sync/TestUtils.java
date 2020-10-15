package org.gbif.collections.sync;

import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.config.SyncConfig.NotificationConfig;
import org.gbif.collections.sync.config.SyncConfig.RegistryConfig;

import static org.junit.Assert.assertEquals;

public class TestUtils {

  public static void assertEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
  }

  public static boolean isEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
    return staffMatch.getMatchedPersons().isEmpty()
        && staffMatch.getNewPersons().isEmpty()
        && staffMatch.getRemovedPersons().isEmpty()
        && staffMatch.getConflicts().isEmpty();
  }

  public static SyncConfig createTestSyncConfig() {
    SyncConfig syncConfig = new SyncConfig();
    syncConfig.setDryRun(true);
    syncConfig.setSendNotifications(false);

    RegistryConfig registryConfig = new RegistryConfig();
    registryConfig.setWsUser("wsUser");
    syncConfig.setRegistry(registryConfig);

    NotificationConfig notificationConfig = new NotificationConfig();
    notificationConfig.setRegistryPortalUrl("http://test.com");
    syncConfig.setNotification(notificationConfig);

    return syncConfig;
  }
}
