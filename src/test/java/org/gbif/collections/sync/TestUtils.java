package org.gbif.collections.sync;

import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.config.SyncConfig.NotificationConfig;

import static org.junit.Assert.assertEquals;

public class TestUtils {

  public static void assertEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
  }

  public static SyncConfig createTestSyncConfig() {
    SyncConfig syncConfig = new SyncConfig();

    NotificationConfig notificationConfig = new NotificationConfig();
    notificationConfig.setRegistryPortalUrl("http://test.com");
    syncConfig.setNotification(notificationConfig);

    return syncConfig;
  }
}