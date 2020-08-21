package org.gbif.collections.sync.ih.match.strategy;

import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.match.MatchResult;
import org.gbif.collections.sync.notification.IHIssueNotifier;

public class ConflictStrategy implements IHMatchResultStrategy<Conflict> {

  private final IHIssueNotifier issueNotifier;

  private ConflictStrategy(IHConfig ihConfig) {
    this.issueNotifier = IHIssueNotifier.create(ihConfig);
  }

  public static ConflictStrategy create(IHConfig ihConfig) {
    return new ConflictStrategy(ihConfig);
  }

  @Override
  public Conflict apply(MatchResult matchResult) {
    issueNotifier.createConflict(matchResult.getAllMatches(), matchResult.getIhInstitution());
    return new Conflict(matchResult.getIhInstitution(), matchResult.getAllMatches());
  }
}
