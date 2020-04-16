package org.gbif.collections.sync;

import java.util.Set;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CliSyncArgs {

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
