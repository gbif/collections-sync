package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.Arrays;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.NoEntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.idigbio.EntityConverter;
import org.gbif.collections.sync.idigbio.IDigBioRecord;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;

import com.google.common.base.Strings;
import lombok.Builder;

public class NoMatchStrategy extends IDigBioBaseStrategy
    implements MatchResultStrategy<MatchResult, NoEntityMatch> {

  @Builder
  public NoMatchStrategy(
      SyncConfig syncConfig, SyncResult.SyncResultBuilder syncResultBuilder, Matcher matcher) {
    super(syncConfig, syncResultBuilder, matcher);
  }

  @Override
  public NoEntityMatch handleAndReturn(MatchResult matchResult) {
    if (!hasCodeAndName(matchResult.getIDigBioRecord())) {
      syncResultBuilder.invalidEntity(matchResult.getIDigBioRecord());
      return null;
    }

    // create institution
    Institution newInstitution =
        EntityConverter.convertToInstitution(matchResult.getIDigBioRecord());

    Institution createdInstitution = institutionHandler.createEntity(newInstitution);

    matcher.getMatchData().addNewlyCreatedIDigBioInstitution(createdInstitution);

    // create collection
    Collection createdCollection = createCollection(matchResult);

    // same staff for both entities
    StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(
            matchResult, Arrays.asList(createdInstitution, createdCollection));

    NoEntityMatch noEntityMatch =
        NoEntityMatch.builder()
            .newCollection(createdCollection)
            .newInstitution(createdInstitution)
            .staffMatch(staffMatch)
            .build();

    syncResultBuilder.noMatch(noEntityMatch);

    return noEntityMatch;
  }

  private boolean hasCodeAndName(IDigBioRecord iDigBioRecord) {
    return (!Strings.isNullOrEmpty(iDigBioRecord.getInstitution())
            || !Strings.isNullOrEmpty(iDigBioRecord.getCollection()))
        && (!Strings.isNullOrEmpty(iDigBioRecord.getInstitutionCode())
            || !Strings.isNullOrEmpty(iDigBioRecord.getCollectionCode()));
  }
}
