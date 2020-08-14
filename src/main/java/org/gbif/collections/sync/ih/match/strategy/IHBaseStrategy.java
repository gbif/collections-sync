package org.gbif.collections.sync.ih.match.strategy;

import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.handler.CollectionHandler;
import org.gbif.collections.sync.handler.InstitutionHandler;
import org.gbif.collections.sync.ih.EntityConverter;
import org.gbif.collections.sync.ih.match.MatchResult;
import org.gbif.collections.sync.ih.match.StaffMatchResultHandler;

public abstract class IHBaseStrategy {

  protected final InstitutionHandler institutionHandler;
  protected final CollectionHandler collectionHandler;
  protected final EntityConverter entityConverter;
  protected final SyncResult.SyncResultBuilder syncResultBuilder;
  protected final StaffMatchResultHandler staffMatchResultHandler;

  protected IHBaseStrategy(
      IHConfig ihConfig,
      EntityConverter entityConverter,
      SyncResult.SyncResultBuilder syncResultBuilder) {
    staffMatchResultHandler =
        new StaffMatchResultHandler(ihConfig, entityConverter, syncResultBuilder);
    this.entityConverter = entityConverter;
    collectionHandler =
        CollectionHandler.builder()
            .syncConfig(ihConfig.getSyncConfig())
            .syncResultBuilder(syncResultBuilder)
            .build();
    institutionHandler =
        InstitutionHandler.builder()
            .syncConfig(ihConfig.getSyncConfig())
            .syncResultBuilder(syncResultBuilder)
            .build();
    this.syncResultBuilder = syncResultBuilder;
  }

  protected EntityMatch<Institution> updateInstitution(MatchResult matchResult) {
    Institution existingInstitution = matchResult.getInstitutions().iterator().next();
    Institution mergedInstitution =
        entityConverter.convertToInstitution(matchResult.getIhInstitution(), existingInstitution);
    return institutionHandler.updateEntity(existingInstitution, mergedInstitution);
  }

  protected EntityMatch<Collection> updateCollection(MatchResult matchResult) {
    Collection existingCollection = matchResult.getCollections().iterator().next();
    Collection mergedCollection =
        entityConverter.convertToCollection(matchResult.getIhInstitution(), existingCollection);
    return collectionHandler.updateEntity(existingCollection, mergedCollection);
  }

  protected Collection createCollection(MatchResult matchResult, UUID institutionKey) {
    // create new collection linked to the institution
    Collection newCollection =
        entityConverter.convertToCollection(matchResult.getIhInstitution(), institutionKey);

    return collectionHandler.createEntity(newCollection);
  }
}
