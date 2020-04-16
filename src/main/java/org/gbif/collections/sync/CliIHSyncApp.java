package org.gbif.collections.sync;

import org.gbif.collections.sync.ih.IHSync;

import java.nio.file.Paths;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;

/** CLI app to sync IH with GrSciColl entities in GBIF registry. */
@Slf4j
public class CliIHSyncApp {

  public static void main(String[] args) {
    // parse args
    CliSyncArgs cliArgs = new CliSyncArgs();
    JCommander.newBuilder().addObject(cliArgs).build().parse(args);

    SyncConfig config = SyncConfig.fromCliArgs(cliArgs);

    // sync IH
    SyncResult ihSyncResult = IHSync.builder().config(config).build().sync();

    // save results to a file
    if (config.isSaveResultsToFile()) {
      SyncResultExporter.exportResultsToFile(
          ihSyncResult, Paths.get("ih_sync_result_" + System.currentTimeMillis()));
    } else {
      log.info("Sync result: {}", ihSyncResult);
    }
  }
}
