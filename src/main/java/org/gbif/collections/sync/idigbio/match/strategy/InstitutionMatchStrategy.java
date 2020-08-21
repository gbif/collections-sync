package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.idigbio.IDigBioProxyClient;
import org.gbif.collections.sync.idigbio.match.MatchResult;

public class InstitutionMatchStrategy extends IDigBioBaseStrategy
    implements IDigBioMatchResultStrategy<InstitutionOnlyMatch> {

  private InstitutionMatchStrategy(IDigBioProxyClient proxyClient) {
    super(proxyClient);
  }

  public static InstitutionMatchStrategy create(IDigBioProxyClient proxyClient) {
    return new InstitutionMatchStrategy(proxyClient);
  }

  @Override
  public InstitutionOnlyMatch apply(MatchResult matchResult) {
    EntityMatch<Institution> entityMatch = updateInstitution(matchResult);

    // create new collection linked to the institution
    Collection createdCollection = createCollection(matchResult);

    // same staff for both entities
    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Arrays.asList(entityMatch.getMerged(), createdCollection));

    return InstitutionOnlyMatch.builder()
        .matchedInstitution(entityMatch)
        .newCollection(createdCollection)
        .staffMatch(staffMatch)
        .build();
  }
}
