package org.gbif.collections.sync.common;

import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.http.CallExecutor;
import org.gbif.collections.sync.http.clients.GithubClient;
import org.gbif.collections.sync.notification.Issue;

public class NotificationProxyClient {

  protected final CallExecutor callExecutor;
  protected GithubClient githubClient;

  private NotificationProxyClient(SyncConfig config) {
    this.callExecutor = new CallExecutor(config);
    if (config != null) {
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
