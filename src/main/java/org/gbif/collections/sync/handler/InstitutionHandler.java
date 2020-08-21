package org.gbif.collections.sync.handler;

import java.util.UUID;

import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.config.SyncConfig;

public class InstitutionHandler extends BaseEntityHandler<Institution> {

  private InstitutionHandler(SyncConfig syncConfig) {
    super(syncConfig);
  }

  public static InstitutionHandler create(SyncConfig syncConfig) {
    return new InstitutionHandler(syncConfig);
  }

  @Override
  protected Institution getCall(UUID key) {
    return grSciCollHttpClient.getInstitution(key);
  }

  @Override
  protected void updateCall(Institution entity) {
    grSciCollHttpClient.updateInstitution(entity);
  }

  @Override
  protected UUID createCall(Institution entity) {
    return grSciCollHttpClient.createInstitution(entity);
  }

  @Override
  protected void addIdentifierToEntityCall(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToInstitution(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntityCall(UUID entityKey, MachineTag machineTag) {
    grSciCollHttpClient.addMachineTagToInstitution(entityKey, machineTag);
  }
}
