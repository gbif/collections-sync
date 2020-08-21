package org.gbif.collections.sync.idigbio.match.strategy;

import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.notification.IDigBioIssueNotifier;

public class ConflictStrategy implements IDigBioMatchResultStrategy<Conflict> {

  private final IDigBioIssueNotifier issueNotifier;

  private ConflictStrategy(IDigBioConfig iDigBioConfig) {
    this.issueNotifier = IDigBioIssueNotifier.create(iDigBioConfig);
  }

  public static ConflictStrategy create(IDigBioConfig iDigBioConfig) {
    return new ConflictStrategy(iDigBioConfig);
  }

  @Override
  public Conflict apply(MatchResult matchResult) {
    issueNotifier.createConflict(matchResult.getAllMatches(), matchResult.getIDigBioRecord());
    return new Conflict(matchResult.getIDigBioRecord(), matchResult.getAllMatches());
  }
}
