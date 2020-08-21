package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.NoEntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.idigbio.EntityConverter;
import org.gbif.collections.sync.idigbio.IDigBioProxyClient;
import org.gbif.collections.sync.idigbio.match.MatchResult;

public class NoMatchStrategy extends IDigBioBaseStrategy
    implements IDigBioMatchResultStrategy<NoEntityMatch> {

  private NoMatchStrategy(IDigBioProxyClient proxyClient) {
    super(proxyClient);
  }

  public static NoMatchStrategy create(IDigBioProxyClient proxyClient) {
    return new NoMatchStrategy(proxyClient);
  }

  @Override
  public NoEntityMatch apply(MatchResult matchResult) {
    // create institution
    Institution newInstitution =
        EntityConverter.convertToInstitution(matchResult.getIDigBioRecord());

    Institution createdInstitution = proxyClient.createInstitution(newInstitution);

    proxyClient.addNewlyCreatedIDigBioInstitution(createdInstitution);

    // create collection
    Collection createdCollection = createCollection(matchResult);

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
