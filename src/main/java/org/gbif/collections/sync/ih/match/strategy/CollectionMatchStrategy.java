package org.gbif.collections.sync.ih.match.strategy;

import java.util.Collections;

import org.gbif.api.model.collections.Collection;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.CollectionOnlyMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.EntityConverter;
import org.gbif.collections.sync.ih.IHProxyClient;
import org.gbif.collections.sync.ih.match.MatchResult;

import lombok.Builder;

public class CollectionMatchStrategy extends IHBaseStrategy
    implements IHMatchResultStrategy<CollectionOnlyMatch> {

  @Builder
  public CollectionMatchStrategy(
      IHConfig ihConfig, IHProxyClient proxyClient, EntityConverter entityConverter) {
    super(ihConfig, entityConverter, proxyClient);
  }

  @Override
  public CollectionOnlyMatch apply(MatchResult matchResult) {
    SyncResult.EntityMatch<Collection> collectionEntityMatch = updateCollection(matchResult);

    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Collections.singletonList(collectionEntityMatch.getMatched()));

    return CollectionOnlyMatch.builder()
        .matchedCollection(collectionEntityMatch)
        .staffMatch(staffMatch)
        .build();
  }
}
