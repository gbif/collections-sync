package org.gbif.collections.sync;

import org.gbif.collections.sync.idigbio.IDigBioSync;

import java.nio.file.Paths;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;

/** CLI app to sync iDigBio with GrSciColl entities in GBIF registry. */
@Slf4j
public class CliIDigBioSyncApp {

  public static void main(String[] args) {
    // parse args
    CliSyncArgs cliArgs = new CliSyncArgs();
    JCommander.newBuilder().addObject(cliArgs).build().parse(args);

    SyncConfig config = SyncConfig.fromCliArgs(cliArgs);

    // sync iDigBio
    SyncResult syncResult = IDigBioSync.builder().config(config).build().sync();

    // save results to a file
    if (config.isSaveResultsToFile()) {
      SyncResultExporter.exportResultsToFile(
          syncResult, Paths.get("idigbio_sync_result_" + System.currentTimeMillis()));
    } else {
      log.info("Sync result: {}", syncResult);
    }
  }
}
