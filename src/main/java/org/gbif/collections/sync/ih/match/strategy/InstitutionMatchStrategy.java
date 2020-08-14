package org.gbif.collections.sync.ih.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.EntityConverter;
import org.gbif.collections.sync.ih.match.MatchResult;

import lombok.Builder;

public class InstitutionMatchStrategy extends IHBaseStrategy
    implements MatchResultStrategy<MatchResult, InstitutionOnlyMatch> {

  // TODO: sacar failed actions y invalid afuera y no setera nada aqui en el result??

  @Builder
  public InstitutionMatchStrategy(
      IHConfig ihConfig,
      EntityConverter entityConverter,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    super(ihConfig, entityConverter, syncResultBuilder);
  }

  @Override
  public InstitutionOnlyMatch handleAndReturn(MatchResult matchResult) {
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(matchResult);

    // create new collection linked to the institution
    Collection createdCollection =
        createCollection(matchResult, institutionEntityMatch.getMerged().getKey());

    // same staff for both entities
    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Arrays.asList(institutionEntityMatch.getMatched(), createdCollection));

    InstitutionOnlyMatch institutionOnlyMatch =
        InstitutionOnlyMatch.builder()
            .matchedInstitution(institutionEntityMatch)
            .newCollection(createdCollection)
            .staffMatch(staffMatch)
            .build();

    syncResultBuilder.institutionOnlyMatch(institutionOnlyMatch);

    return institutionOnlyMatch;
  }
}
