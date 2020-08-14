package org.gbif.collections.sync.handler;

import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.SyncResult.SyncResultBuilder;
import org.gbif.collections.sync.config.SyncConfig;

import lombok.Builder;

public class CollectionHandler extends EntityHandler<Collection> {

  private CollectionHandler(SyncConfig syncConfig, SyncResultBuilder syncResultBuilder) {
    super(syncConfig, syncResultBuilder);
  }

  @Builder
  public static CollectionHandler create(
      SyncConfig syncConfig, SyncResultBuilder syncResultBuilder) {
    return new CollectionHandler(syncConfig, syncResultBuilder);
  }

  @Override
  protected Collection get(UUID key) {
    return grSciCollHttpClient.getCollection(key);
  }

  @Override
  protected void update(Collection entity) {
    grSciCollHttpClient.updateCollection(entity);
  }

  @Override
  protected UUID create(Collection entity) {
    return grSciCollHttpClient.createCollection(entity);
  }

  @Override
  protected void addIdentifierToEntity(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToCollection(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntity(UUID entityKey, MachineTag machineTag) {
    grSciCollHttpClient.addMachineTagToCollection(entityKey, machineTag);
  }
}
