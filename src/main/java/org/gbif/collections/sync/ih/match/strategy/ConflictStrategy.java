package org.gbif.collections.sync.ih.match.strategy;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.match.MatchResult;
import org.gbif.collections.sync.notification.IHIssueNotifier;

import lombok.Builder;

public class ConflictStrategy implements MatchResultStrategy<MatchResult, Conflict> {

  private final IHIssueNotifier issueNotifier;
  private final SyncResult.SyncResultBuilder syncResultBuilder;

  @Builder
  public ConflictStrategy(IHConfig ihConfig, SyncResult.SyncResultBuilder syncResultBuilder) {
    this.issueNotifier = IHIssueNotifier.create(ihConfig, syncResultBuilder);
    this.syncResultBuilder = syncResultBuilder;
  }

  @Override
  public Conflict handleAndReturn(MatchResult matchResult) {
    issueNotifier.createConflict(matchResult.getAllMatches(), matchResult.getIhInstitution());
    Conflict conflict = new Conflict(matchResult.getIhInstitution(), matchResult.getAllMatches());
    syncResultBuilder.conflict(conflict);
    return conflict;
  }
}
