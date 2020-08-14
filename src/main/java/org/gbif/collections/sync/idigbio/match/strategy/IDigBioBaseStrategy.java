package org.gbif.collections.sync.idigbio.match.strategy;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.handler.CollectionHandler;
import org.gbif.collections.sync.handler.InstitutionHandler;
import org.gbif.collections.sync.idigbio.EntityConverter;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;
import org.gbif.collections.sync.idigbio.match.StaffMatchResultHandler;

public class IDigBioBaseStrategy {

  protected final SyncResult.SyncResultBuilder syncResultBuilder;
  protected final Matcher matcher;
  protected final CollectionHandler collectionHandler;
  protected final InstitutionHandler institutionHandler;
  protected final StaffMatchResultHandler staffMatchResultHandler;

  protected IDigBioBaseStrategy(
      SyncConfig syncConfig, SyncResult.SyncResultBuilder syncResultBuilder, Matcher matcher) {
    this.syncResultBuilder = syncResultBuilder;
    collectionHandler =
        CollectionHandler.builder()
            .syncConfig(syncConfig)
            .syncResultBuilder(syncResultBuilder)
            .build();
    institutionHandler =
        InstitutionHandler.builder()
            .syncConfig(syncConfig)
            .syncResultBuilder(syncResultBuilder)
            .build();
    this.matcher = matcher;
    this.staffMatchResultHandler =
        new StaffMatchResultHandler(syncConfig, matcher, syncResultBuilder);
  }

  protected EntityMatch<Collection> updateCollection(MatchResult matchResult) {
    Collection mergedCollection =
        EntityConverter.convertToCollection(
            matchResult.getCollectionMatched(), matchResult.getIDigBioRecord());

    EntityMatch<Collection> entityMatch =
        collectionHandler.updateEntity(matchResult.getCollectionMatched(), mergedCollection);

    if (entityMatch.isUpdate()) {
      // update institution in our match data
      Collection updatedCollection = collectionHandler.getEntity(mergedCollection);
      matcher
          .getMatchData()
          .updateCollection(matchResult.getCollectionMatched(), updatedCollection);
      entityMatch.setMerged(updatedCollection);
    }

    return entityMatch;
  }

  protected EntityMatch<Institution> updateInstitution(MatchResult matchResult) {
    Institution mergedInstitution =
        EntityConverter.convertToInstitution(
            matchResult.getInstitutionMatched(), matchResult.getIDigBioRecord());

    EntityMatch<Institution> entityMatch =
        institutionHandler.updateEntity(matchResult.getInstitutionMatched(), mergedInstitution);

    if (entityMatch.isUpdate()) {
      // update institution in our match data
      Institution updatedInstitution = institutionHandler.getEntity(mergedInstitution);
      matcher.getMatchData().updateInstitution(updatedInstitution);
      entityMatch.setMerged(updatedInstitution);
    }

    return entityMatch;
  }

  protected Collection createCollection(MatchResult matchResult) {
    Collection newCollection =
        EntityConverter.convertToCollection(
            matchResult.getIDigBioRecord(), matchResult.getInstitutionMatched());

    return collectionHandler.createEntity(newCollection);
  }
}
