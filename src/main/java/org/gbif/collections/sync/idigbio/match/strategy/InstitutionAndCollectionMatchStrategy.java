package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.idigbio.IDigBioProxyClient;
import org.gbif.collections.sync.idigbio.match.MatchResult;

public class InstitutionAndCollectionMatchStrategy extends IDigBioBaseStrategy
    implements IDigBioMatchResultStrategy<InstitutionAndCollectionMatch> {

  private InstitutionAndCollectionMatchStrategy(IDigBioProxyClient proxyClient) {
    super(proxyClient);
  }

  public static InstitutionAndCollectionMatchStrategy create(IDigBioProxyClient proxyClient) {
    return new InstitutionAndCollectionMatchStrategy(proxyClient);
  }

  @Override
  public InstitutionAndCollectionMatch apply(MatchResult matchResult) {
    // update institution
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(matchResult);

    // update collection
    EntityMatch<Collection> collectionEntityMatch = updateCollection(matchResult);

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
