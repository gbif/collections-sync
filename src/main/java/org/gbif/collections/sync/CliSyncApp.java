package org.gbif.collections.sync;

import org.gbif.collections.sync.ih.IHSync;
import org.gbif.collections.sync.ih.IHSyncResult;
import org.gbif.collections.sync.ih.IHSyncResultExporter;

import java.nio.file.Paths;
import java.util.Set;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/** CLI app to sync IH with GrSciColl entities in GBIF registry. */
@Slf4j
public class CliSyncApp {

  public static void main(String[] args) {
    // parse args
    CliArgs cliArgs = new CliArgs();
    JCommander.newBuilder().addObject(cliArgs).build().parse(args);

    SyncConfig config = SyncConfig.fromCliArgs(cliArgs);

    // sync IH
    IHSyncResult ihSyncResult = IHSync.builder().config(config).build().sync();

    // save results to a file
    if (config.isSaveResultsToFile()) {
      IHSyncResultExporter.exportResultsToFile(
          ihSyncResult, Paths.get("ih_sync_result_" + System.currentTimeMillis()));
    } else {
      log.info("Sync result: {}", ihSyncResult);
    }
  }

  @Getter
  @Setter
  static class CliArgs {
    @Parameter(
        names = {"--config", "-c"},
        required = true)
    private String confPath;

    @Parameter(
        names = {"--dryRun", "-dr"},
        arity = 1)
    private Boolean dryRun;

    @Parameter(
        names = {"--sendNotifications", "-n"},
        arity = 1)
    private Boolean sendNotifications;

    @Parameter(names = {"--githubAssignees", "-ga"})
    private Set<String> githubAssignees;
  }
}
