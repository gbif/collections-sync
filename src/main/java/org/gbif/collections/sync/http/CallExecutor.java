package org.gbif.collections.sync.http;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.config.SyncConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CallExecutor {

  private final boolean dryRun;
  private final boolean sendNotifications;

  // TODO: replace the syncResultBuilder with sth else

  public CallExecutor(SyncConfig syncConfig) {
    if (syncConfig != null) {
      this.dryRun = syncConfig.isDryRun();
      this.sendNotifications = syncConfig.isSendNotifications();
    } else {
      this.dryRun = true;
      this.sendNotifications = false;
    }
  }

  public void executeOrAddFailAsync(
      Runnable action,
      Function<Throwable, FailedAction> failCreator,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    if (!dryRun) {
      CompletableFuture.runAsync(action)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  syncResultBuilder.failedAction(failCreator.apply(e));
                }
              });
    }
  }

  public void executeOrAddFail(
      Runnable action,
      Function<Throwable, FailedAction> failCreator,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    if (!dryRun) {
      try {
        action.run();
      } catch (Exception e) {
        syncResultBuilder.failedAction(failCreator.apply(e));
      }
    }
  }

  public <T> T executeAndReturnOrAddFail(
      Supplier<T> execution,
      Function<Throwable, FailedAction> failCreator,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    return executeAndReturnOrAddFail(execution, failCreator, syncResultBuilder, null);
  }

  public <T> T executeAndReturnOrAddFail(
      Supplier<T> execution,
      Function<Throwable, FailedAction> failCreator,
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

  public void sendNotification(
      Runnable runnable,
      Function<Throwable, FailedAction> failCreator,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    if (sendNotifications) {
      // do the call
      CompletableFuture.runAsync(runnable)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  syncResultBuilder.failedAction(failCreator.apply(e));
                }
              });
    }
  }
}
