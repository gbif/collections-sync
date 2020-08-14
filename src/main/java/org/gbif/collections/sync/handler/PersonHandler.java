package org.gbif.collections.sync.handler;

import java.util.UUID;

import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.SyncResult.SyncResultBuilder;
import org.gbif.collections.sync.config.SyncConfig;

import lombok.Builder;

public class PersonHandler extends BasePersonHandler {

  private PersonHandler(SyncConfig syncConfig, SyncResultBuilder syncResultBuilder) {
    super(syncConfig, syncResultBuilder);
  }

  @Builder
  public static PersonHandler create(SyncConfig syncConfig, SyncResultBuilder syncResultBuilder) {
    return new PersonHandler(syncConfig, syncResultBuilder);
  }

  @Override
  protected Person get(UUID key) {
    return grSciCollHttpClient.getPerson(key);
  }

  @Override
  protected void update(Person entity) {
    grSciCollHttpClient.updatePerson(entity);
  }

  @Override
  protected UUID create(Person entity) {
    return grSciCollHttpClient.createPerson(entity);
  }

  @Override
  protected void addIdentifierToEntity(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToPerson(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntity(UUID entityKey, MachineTag machineTag) {
    // do nothing
  }
}
