package org.gbif.collections.sync.clients.proxy;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.config.SyncConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CallExecutor {

  private static final ConcurrentMap<SyncConfig, CallExecutor> executorsMap =
      new ConcurrentHashMap<>();

  private final boolean dryRun;
  private final boolean sendNotifications;
  private Path failedActionsPath;

  private CallExecutor(SyncConfig syncConfig) {
    if (syncConfig != null) {
      this.dryRun = syncConfig.isDryRun();
      this.sendNotifications = syncConfig.isSendNotifications();
    } else {
      this.dryRun = true;
      this.sendNotifications = false;
    }

    log.info(
        "Call Executor created with dryRun {} and sendNotifications {}", dryRun, sendNotifications);
  }

  public static CallExecutor getInstance(SyncConfig config) {
    CallExecutor instance = executorsMap.get(config);
    if (instance != null) {
      return instance;
    } else {
      CallExecutor newInstance = new CallExecutor(config);
      executorsMap.put(config, newInstance);
      return newInstance;
    }
  }

  public void executeOrAddFailAsync(
      Runnable action, Function<Throwable, FailedAction> exceptionHandler) {
    if (!dryRun) {
      CompletableFuture.runAsync(action)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  writeFailedAction(exceptionHandler.apply(e));
                }
              });
    }
  }

  public void executeOrAddFail(
      Runnable action, Function<Throwable, FailedAction> exceptionHandler) {
    if (!dryRun) {
      try {
        action.run();
      } catch (Exception e) {
        writeFailedAction(exceptionHandler.apply(e));
      }
    }
  }

  public <T> T executeAndReturnOrAddFail(
      Supplier<T> execution, Function<Throwable, FailedAction> exceptionHandler) {
    return executeAndReturnOrAddFail(execution, exceptionHandler, null);
  }

  public <T> T executeAndReturnOrAddFail(
      Supplier<T> execution,
      Function<Throwable, FailedAction> exceptionHandler,
      T defaultReturnValue) {
    if (!dryRun) {
      try {
        return execution.get();
      } catch (Exception e) {
        writeFailedAction(exceptionHandler.apply(e));
      }
    }

    return defaultReturnValue;
  }

  public void sendNotification(
      Runnable runnable, Function<Throwable, FailedAction> exceptionHandler) {
    if (sendNotifications) {
      // do the call
      CompletableFuture.runAsync(runnable)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  writeFailedAction(exceptionHandler.apply(e));
                }
              });
    }
  }

  private void writeFailedAction(FailedAction failedAction) {
    if (failedActionsPath == null) {
      failedActionsPath = Paths.get("failed_actions_" + System.currentTimeMillis());
    }

    try {
      try (BufferedWriter writer =
          Files.newBufferedWriter(failedActionsPath, StandardOpenOption.APPEND)) {
        writer.write(failedAction.toString());
        writer.newLine();
      }
    } catch (IOException e) {
      log.error("Error persisting a failed action to a file: {}", failedAction.toString(), e);
    }
  }
}
