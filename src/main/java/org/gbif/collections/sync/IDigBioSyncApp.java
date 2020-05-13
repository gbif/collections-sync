package org.gbif.collections.sync;

import java.nio.file.Paths;

import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.IDigBioSync;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;

/** CLI app to sync iDigBio with GrSciColl entities in GBIF registry. */
@Slf4j
public class IDigBioSyncApp {

  public static void main(String[] args) {
    // parse args
    CliSyncArgs cliArgs = new CliSyncArgs();
    JCommander.newBuilder().addObject(cliArgs).build().parse(args);

    IDigBioConfig iDigBioConfig = IDigBioConfig.fromCliArgs(cliArgs);

    // sync iDigBio
    SyncResult syncResult = IDigBioSync.builder().iDigBioConfig(iDigBioConfig).build().sync();

    // save results to a file
    if (iDigBioConfig.getSyncConfig().isSaveResultsToFile()) {
      SyncResultExporter.exportResultsToFile(
          syncResult, Paths.get("idigbio_sync_result_" + System.currentTimeMillis()));
    } else {
      log.info("Sync result: {}", syncResult);
    }
  }
}
