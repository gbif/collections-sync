package org.gbif.collections.sync.ih.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.EntityConverter;
import org.gbif.collections.sync.ih.match.MatchResult;

import lombok.Builder;

public class InstitutionAndCollectionMatchStrategy extends IHBaseStrategy
    implements MatchResultStrategy<MatchResult, InstitutionAndCollectionMatch> {

  @Builder
  public InstitutionAndCollectionMatchStrategy(
      IHConfig ihConfig,
      EntityConverter entityConverter,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    super(ihConfig, entityConverter, syncResultBuilder);
  }

  @Override
  public InstitutionAndCollectionMatch handleAndReturn(MatchResult matchResult) {
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

    InstitutionAndCollectionMatch institutionAndCollectionMatch =
        InstitutionAndCollectionMatch.builder()
            .matchedInstitution(institutionEntityMatch)
            .matchedCollection(collectionEntityMatch)
            .staffMatch(staffMatch)
            .build();

    syncResultBuilder.instAndCollMatch(institutionAndCollectionMatch);

    return institutionAndCollectionMatch;
  }
}
