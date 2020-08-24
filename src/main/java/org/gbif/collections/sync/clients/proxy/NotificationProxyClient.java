package org.gbif.collections.sync.clients.proxy;

import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.clients.http.GithubClient;
import org.gbif.collections.sync.common.notification.Issue;
import org.gbif.collections.sync.config.SyncConfig;

public class NotificationProxyClient {

  protected final CallExecutor callExecutor;
  protected GithubClient githubClient;

  private NotificationProxyClient(SyncConfig config) {
    this.callExecutor = new CallExecutor(config);
    if (config != null && config.isSendNotifications()) {
      githubClient = GithubClient.getInstance(config.getNotification());
    }
  }

  public static NotificationProxyClient create(SyncConfig config) {
    return new NotificationProxyClient(config);
  }

  public void sendNotification(Issue issue) {
    callExecutor.sendNotification(
        () -> githubClient.createIssue(issue),
        e -> new FailedAction(issue, "Failed to create fails notification: " + e.getMessage()));
  }
}
