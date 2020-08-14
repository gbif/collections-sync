package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;

import lombok.Builder;

public class InstitutionAndCollectionMatchStrategy extends IDigBioBaseStrategy
    implements MatchResultStrategy<MatchResult, InstitutionAndCollectionMatch> {

  // TODO: base class to reuse ctor??
  @Builder
  public InstitutionAndCollectionMatchStrategy(
      SyncConfig syncConfig, SyncResult.SyncResultBuilder syncResultBuilder, Matcher matcher) {
    super(syncConfig, syncResultBuilder, matcher);
  }

  @Override
  public InstitutionAndCollectionMatch handleAndReturn(MatchResult matchResult) {
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
