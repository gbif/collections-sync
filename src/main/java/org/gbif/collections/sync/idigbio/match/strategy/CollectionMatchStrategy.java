package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Collections;

import org.gbif.api.model.collections.Collection;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.CollectionOnlyMatch;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;

import lombok.Builder;

public class CollectionMatchStrategy extends IDigBioBaseStrategy
    implements MatchResultStrategy<MatchResult, CollectionOnlyMatch> {

  @Builder
  public CollectionMatchStrategy(
      SyncConfig syncConfig, SyncResult.SyncResultBuilder syncResultBuilder, Matcher matcher) {
    super(syncConfig, syncResultBuilder, matcher);
  }

  @Override
  public CollectionOnlyMatch handleAndReturn(MatchResult matchResult) {
    EntityMatch<Collection> entityMatch = updateCollection(matchResult);

    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Collections.singletonList(entityMatch.getMerged()));

    CollectionOnlyMatch collectionOnlyMatch =
        CollectionOnlyMatch.builder().matchedCollection(entityMatch).staffMatch(staffMatch).build();

    syncResultBuilder.collectionOnlyMatch(collectionOnlyMatch);

    return collectionOnlyMatch;
  }
}
