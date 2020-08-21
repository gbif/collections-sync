package org.gbif.collections.sync.ih.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.ih.IHProxyClient;
import org.gbif.collections.sync.ih.match.MatchResult;

public class InstitutionMatchStrategy extends IHBaseStrategy
    implements IHMatchResultStrategy<InstitutionOnlyMatch> {

  private InstitutionMatchStrategy(IHProxyClient proxyClient) {
    super(proxyClient);
  }

  public static InstitutionMatchStrategy create(IHProxyClient proxyClient) {
    return new InstitutionMatchStrategy(proxyClient);
  }

  @Override
  public InstitutionOnlyMatch apply(MatchResult matchResult) {
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(matchResult);

    // create new collection linked to the institution
    Collection createdCollection =
        createCollection(matchResult, institutionEntityMatch.getMerged().getKey());

    // same staff for both entities
    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Arrays.asList(institutionEntityMatch.getMatched(), createdCollection));

    return InstitutionOnlyMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .newCollection(createdCollection)
        .staffMatch(staffMatch)
        .build();
  }
}
