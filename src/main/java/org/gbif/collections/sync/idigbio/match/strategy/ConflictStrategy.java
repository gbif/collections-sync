package org.gbif.collections.sync.idigbio.match.strategy;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.notification.IDigBioIssueNotifier;

import lombok.Builder;

public class ConflictStrategy implements MatchResultStrategy<MatchResult, Conflict> {

  private final IDigBioIssueNotifier issueNotifier;
  private final SyncResult.SyncResultBuilder syncResultBuilder;

  @Builder
  public ConflictStrategy(
      IDigBioConfig iDigBioConfig, SyncResult.SyncResultBuilder syncResultBuilder) {
    this.issueNotifier = IDigBioIssueNotifier.create(iDigBioConfig, syncResultBuilder);
    this.syncResultBuilder = syncResultBuilder;
  }

  @Override
  public Conflict handleAndReturn(MatchResult matchResult) {
    issueNotifier.createConflict(matchResult.getAllMatches(), matchResult.getIDigBioRecord());
    Conflict conflict = new Conflict(matchResult.getIDigBioRecord(), matchResult.getAllMatches());
    syncResultBuilder.conflict(conflict);
    return conflict;
  }
}
