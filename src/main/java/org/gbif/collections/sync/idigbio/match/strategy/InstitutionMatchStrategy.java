package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;

import lombok.Builder;

public class InstitutionMatchStrategy extends IDigBioBaseStrategy
    implements MatchResultStrategy<MatchResult, InstitutionOnlyMatch> {

  @Builder
  public InstitutionMatchStrategy(
      SyncConfig syncConfig, SyncResult.SyncResultBuilder syncResultBuilder, Matcher matcher) {
    super(syncConfig, syncResultBuilder, matcher);
  }

  @Override
  public InstitutionOnlyMatch handleAndReturn(MatchResult matchResult) {
    EntityMatch<Institution> entityMatch = updateInstitution(matchResult);

    // create new collection linked to the institution
    Collection createdCollection = createCollection(matchResult);

    // same staff for both entities
    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Arrays.asList(entityMatch.getMerged(), createdCollection));

    InstitutionOnlyMatch institutionOnlyMatch =
        InstitutionOnlyMatch.builder()
            .matchedInstitution(entityMatch)
            .newCollection(createdCollection)
            .staffMatch(staffMatch)
            .build();

    syncResultBuilder.institutionOnlyMatch(institutionOnlyMatch);

    return institutionOnlyMatch;
  }
}
