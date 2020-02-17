package org.gbif.collections.sync;

import org.gbif.collections.sync.ih.IHSync;
import org.gbif.collections.sync.ih.IHSyncResult;
import org.gbif.collections.sync.ih.IHSyncResultExporter;

import java.nio.file.Paths;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;

/** CLI app to sync IH with GrSciColl entities in GBIF registry. */
@Slf4j
public class CliSyncApp {

  public static void main(String[] args) {
    // parse args
    CliArgs cliArgs = new CliArgs();
    JCommander.newBuilder().addObject(cliArgs).build().parse(args);

    SyncConfig config =
        SyncConfig.fromFileName(cliArgs.confPath)
            .orElseThrow(() -> new IllegalArgumentException("No valid config provided"));

    // sync IH
    IHSyncResult ihSyncResult = IHSync.builder().config(config).build().sync();
    log.info("Sync result: {}", ihSyncResult);

    // save results to a file
    if (config.isSaveResultsToFile()) {
      IHSyncResultExporter.exportResultsToFile(
          ihSyncResult, Paths.get("ih_sync_result_" + System.currentTimeMillis()));
    }
  }

  private static class CliArgs {
    @Parameter(names = {"--config", "-c"})
    private String confPath;
  }
}
