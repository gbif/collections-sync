package org.gbif.collections.sync.handler;

import java.util.UUID;

import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.SyncResult.SyncResultBuilder;
import org.gbif.collections.sync.config.SyncConfig;

import lombok.Builder;

public class InstitutionHandler extends EntityHandler<Institution> {

  private InstitutionHandler(SyncConfig syncConfig, SyncResultBuilder syncResultBuilder) {
    super(syncConfig, syncResultBuilder);
  }

  @Builder
  public static InstitutionHandler create(
      SyncConfig syncConfig, SyncResultBuilder syncResultBuilder) {
    return new InstitutionHandler(syncConfig, syncResultBuilder);
  }

  @Override
  protected Institution get(UUID key) {
    return grSciCollHttpClient.getInstitution(key);
  }

  @Override
  protected void update(Institution entity) {
    grSciCollHttpClient.updateInstitution(entity);
  }

  @Override
  protected UUID create(Institution entity) {
    return grSciCollHttpClient.createInstitution(entity);
  }

  @Override
  protected void addIdentifierToEntity(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToInstitution(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntity(UUID entityKey, MachineTag machineTag) {
    grSciCollHttpClient.addMachineTagToInstitution(entityKey, machineTag);
  }
}
