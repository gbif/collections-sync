package org.gbif.collections.sync.ih.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.EntityConverter;
import org.gbif.collections.sync.ih.IHProxyClient;
import org.gbif.collections.sync.ih.match.MatchResult;

import lombok.Builder;

public class InstitutionAndCollectionMatchStrategy extends IHBaseStrategy
    implements IHMatchResultStrategy<InstitutionAndCollectionMatch> {

  @Builder
  public InstitutionAndCollectionMatchStrategy(
      IHConfig ihConfig, IHProxyClient proxyClient, EntityConverter entityConverter) {
    super(ihConfig, entityConverter, proxyClient);
  }

  @Override
  public InstitutionAndCollectionMatch apply(MatchResult matchResult) {
    // update institution
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(matchResult);

    // update collection
    SyncResult.EntityMatch<Collection> collectionEntityMatch = updateCollection(matchResult);

    // update staff
    Institution institution = institutionEntityMatch.getMerged();
    Collection collection = collectionEntityMatch.getMerged();

    // then we handle the staff of both entities at the same time to avoid creating duplicates
    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(matchResult, Arrays.asList(institution, collection));

    return InstitutionAndCollectionMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .matchedCollection(collectionEntityMatch)
        .staffMatch(staffMatch)
        .build();
  }
}
