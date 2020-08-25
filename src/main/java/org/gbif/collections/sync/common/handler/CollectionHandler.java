package org.gbif.collections.sync.common.handler;

import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.proxy.CallExecutor;

public class CollectionHandler extends BaseEntityHandler<Collection> {

  private CollectionHandler(CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    super(callExecutor, grSciCollHttpClient);
  }

  public static CollectionHandler create(
      CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    return new CollectionHandler(callExecutor, grSciCollHttpClient);
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
