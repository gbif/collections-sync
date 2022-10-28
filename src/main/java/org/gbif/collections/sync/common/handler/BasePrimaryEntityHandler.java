package org.gbif.collections.sync.common.handler;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.MasterSourceMetadata;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.LenientEquals;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.proxy.CallExecutor;

import java.util.UUID;

public abstract class BasePrimaryEntityHandler<
        T extends LenientEquals<T> & CollectionEntity & Identifiable & MachineTaggable>
    extends BaseEntityHandler<T> {

  public BasePrimaryEntityHandler(
      CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    super(callExecutor, grSciCollHttpClient);
  }

  @Override
  protected void addSubEntities(T entity) {
    entity.getIdentifiers().stream()
        .filter(i -> i.getKey() == null)
        .forEach(i -> addIdentifierToEntityCall(entity.getKey(), i));
    entity.getMachineTags().stream()
        .filter(mt -> mt.getKey() == null)
        .forEach(mt -> addMachineTagToEntityCall(entity.getKey(), mt));
    if (entity.getMasterSourceMetadata() != null
        && entity.getMasterSourceMetadata().getKey() == null) {
      addMasterSourceMetadataToEntityCall(entity.getKey(), entity.getMasterSourceMetadata());
    }
  }

  protected abstract void addMasterSourceMetadataToEntityCall(
      UUID entityKey, MasterSourceMetadata metadata);
}
