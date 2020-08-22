package org.gbif.collections.sync.common.handler;

import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.config.SyncConfig;

public class CollectionHandler extends BaseEntityHandler<Collection> {

  private CollectionHandler(SyncConfig syncConfig) {
    super(syncConfig);
  }

  public static CollectionHandler create(SyncConfig syncConfig) {
    return new CollectionHandler(syncConfig);
  }

  @Override
  protected Collection getCall(UUID key) {
    return grSciCollHttpClient.getCollection(key);
  }

  @Override
  protected void updateCall(Collection entity) {
    grSciCollHttpClient.updateCollection(entity);
  }

  @Override
  protected UUID createCall(Collection entity) {
    return grSciCollHttpClient.createCollection(entity);
  }

  @Override
  protected void addIdentifierToEntityCall(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToCollection(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntityCall(UUID entityKey, MachineTag machineTag) {
    grSciCollHttpClient.addMachineTagToCollection(entityKey, machineTag);
  }
}
