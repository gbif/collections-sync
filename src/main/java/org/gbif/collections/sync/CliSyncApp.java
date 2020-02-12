package org.gbif.collections.sync;

import org.gbif.collections.sync.ih.IHSync;
import org.gbif.collections.sync.ih.IHSyncResult;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;

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
    IHSyncResult IHSyncResult = IHSync.builder().config(config).build().sync();
    log.info("Sync result: {}", IHSyncResult);

    // save results to a file
    // TODO
    //    if (config.isSaveResultsToFile()) {
    //      DiffResultExporter.exportResultsToFile(
    //          diffResult, Paths.get("ih_sync_result_" + System.currentTimeMillis()));
    //    }
  }

  private static class CliArgs {
    @Parameter(names = {"--config", "-c"})
    private String confPath;
  }
}
