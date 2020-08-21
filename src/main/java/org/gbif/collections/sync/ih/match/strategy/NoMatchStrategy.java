package org.gbif.collections.sync.ih.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.NoEntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.ih.IHProxyClient;
import org.gbif.collections.sync.ih.match.MatchResult;

public class NoMatchStrategy extends IHBaseStrategy
    implements IHMatchResultStrategy<NoEntityMatch> {

  private NoMatchStrategy(IHProxyClient proxyClient) {
    super(proxyClient);
  }

  public static NoMatchStrategy create(IHProxyClient proxyClient) {
    return new NoMatchStrategy(proxyClient);
  }

  @Override
  public NoEntityMatch apply(MatchResult matchResult) {
    // create institution
    Institution newInstitution =
        entityConverter.convertToInstitution(matchResult.getIhInstitution());
    Institution createdInstitution = proxyClient.createInstitution(newInstitution);

    // create collection
    Collection createdCollection = createCollection(matchResult, createdInstitution.getKey());

    // same staff for both entities
    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Arrays.asList(createdInstitution, createdCollection));

    return NoEntityMatch.builder()
        .newCollection(createdCollection)
        .newInstitution(createdInstitution)
        .staffMatch(staffMatch)
        .build();
  }
}
