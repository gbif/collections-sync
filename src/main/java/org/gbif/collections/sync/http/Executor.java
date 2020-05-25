package org.gbif.collections.sync.http;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.http.clients.GithubClient;
import org.gbif.collections.sync.notification.Issue;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Executor {

  public static void executeOrAddFailAsync(
      Runnable runnable,
      Function<Throwable, FailedAction> failCreator,
      boolean dryRun,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    if (!dryRun) {
      CompletableFuture.runAsync(runnable)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  syncResultBuilder.failedAction(failCreator.apply(e));
                }
              });
    }
  }

  public static void executeOrAddFail(
      Runnable runnable,
      Function<Throwable, FailedAction> failCreator,
      boolean dryRun,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    if (!dryRun) {
      try {
        runnable.run();
      } catch (Exception e) {
        syncResultBuilder.failedAction(failCreator.apply(e));
      }
    }
  }

  public static <T> T executeAndReturnOrAddFail(
      Supplier<T> execution,
      Function<Throwable, FailedAction> failCreator,
      boolean dryRun,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    return executeAndReturnOrAddFail(execution, failCreator, dryRun, syncResultBuilder, null);
  }

  public static <T> T executeAndReturnOrAddFail(
      Supplier<T> execution,
      Function<Throwable, FailedAction> failCreator,
      boolean dryRun,
      SyncResult.SyncResultBuilder syncResultBuilder,
      T defaultReturnValue) {
    if (!dryRun) {
      try {
        return execution.get();
      } catch (Exception e) {
        syncResultBuilder.failedAction(failCreator.apply(e));
      }
    }

    return defaultReturnValue;
  }

  public static void createGHIssue(
      Issue issue,
      boolean sendNotifications,
      GithubClient githubClient,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    if (sendNotifications) {
      Optional<Issue> existingIssueOpt = githubClient.findIssueWithSameTitle(issue.getTitle());
      Runnable runnable;
      String errorMsg;
      if (existingIssueOpt.isPresent()) {
        // if it exists we update the labels to add the one of this sync. We also merge the
        // assignees in case the original ones were modified in Github
        Issue existingIssue = existingIssueOpt.get();
        issue.setNumber(existingIssue.getNumber());
        issue.getLabels().addAll(existingIssue.getLabels());
        issue.getAssignees().addAll(existingIssue.getAssignees());

        runnable = () -> githubClient.updateIssue(issue);
        errorMsg = "Failed to add sync timestamp label to issue: ";
      } else {
        // if it doesn't exist we create it
        runnable = () -> githubClient.createIssue(issue);
        errorMsg = "Failed to create issue: ";
      }

      // do the call
      CompletableFuture.runAsync(runnable)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  syncResultBuilder.failedAction(
                      new FailedAction(issue, errorMsg + e.getMessage()));
                }
              });
    }
  }
}
