package org.gbif.collections.sync;

import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.config.SyncConfig.NotificationConfig;
import org.gbif.collections.sync.config.SyncConfig.RegistryConfig;

import static org.junit.Assert.assertEquals;

public class TestUtils {

  public static void assertEmptyContactMatch(SyncResult.ContactMatch contactMatch) {
    assertEquals(0, contactMatch.getMatchedContacts().size());
    assertEquals(0, contactMatch.getNewContacts().size());
    assertEquals(0, contactMatch.getRemovedContacts().size());
    assertEquals(0, contactMatch.getConflicts().size());
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
