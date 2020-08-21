package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Collections;

import org.gbif.api.model.collections.Collection;
import org.gbif.collections.sync.SyncResult.CollectionOnlyMatch;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.idigbio.IDigBioProxyClient;
import org.gbif.collections.sync.idigbio.match.MatchResult;

public class CollectionMatchStrategy extends IDigBioBaseStrategy
    implements IDigBioMatchResultStrategy<CollectionOnlyMatch> {

  private CollectionMatchStrategy(IDigBioProxyClient proxyClient) {
    super(proxyClient);
  }

  public static CollectionMatchStrategy create(IDigBioProxyClient proxyClient) {
    return new CollectionMatchStrategy(proxyClient);
  }

  @Override
  public CollectionOnlyMatch apply(MatchResult matchResult) {
    EntityMatch<Collection> entityMatch = updateCollection(matchResult);

    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Collections.singletonList(entityMatch.getMerged()));

    return CollectionOnlyMatch.builder()
        .matchedCollection(entityMatch)
        .staffMatch(staffMatch)
        .build();
  }
}
