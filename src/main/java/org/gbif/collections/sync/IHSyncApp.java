package org.gbif.collections.sync;

import java.nio.file.Paths;

import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.IHSync;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;

/** CLI app to sync IH with GrSciColl entities in GBIF registry. */
@Slf4j
public class IHSyncApp {

  public static void main(String[] args) {
    // parse args
    CliSyncArgs cliArgs = new CliSyncArgs();
    JCommander.newBuilder().addObject(cliArgs).build().parse(args);

    IHConfig config = IHConfig.fromCliArgs(cliArgs);

    // sync IH
    SyncResult ihSyncResult = IHSync.builder().ihConfig(config).build().sync();

    // save results to a file
    if (config.getSyncConfig().isSaveResultsToFile()) {
      SyncResultExporter.exportResultsToFile(
          ihSyncResult, Paths.get("ih_sync_result_" + System.currentTimeMillis()));
    } else {
      log.info("Sync result: {}", ihSyncResult);
    }
  }
}
