package org.gbif.collections.sync.handler;

import java.util.UUID;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.LenientEquals;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.http.CallExecutor;
import org.gbif.collections.sync.http.clients.GrSciCollHttpClient;

public abstract class EntityHandler<
    T extends LenientEquals<T> & CollectionEntity & Identifiable & MachineTaggable> {

  protected final CallExecutor callExecutor;
  protected final SyncResult.SyncResultBuilder syncResultBuilder;
  protected GrSciCollHttpClient grSciCollHttpClient;

  public EntityHandler(SyncConfig syncConfig, SyncResult.SyncResultBuilder syncResultBuilder) {
    this.callExecutor = new CallExecutor(syncConfig);
    this.syncResultBuilder = syncResultBuilder;
    if (syncConfig != null) {
      this.grSciCollHttpClient = GrSciCollHttpClient.getInstance(syncConfig);
    }
  }

  public EntityMatch<T> updateEntity(T entityMatched, T entityMerged) {
    EntityMatch.EntityMatchBuilder<T> entityMatchBuilder =
        EntityMatch.<T>builder().matched(entityMatched).merged(entityMerged);
    if (!entityMerged.equals(entityMatched)) {
      // check if we need to update the entity
      if (!entityMerged.lenientEquals(entityMatched)) {
        callExecutor.executeOrAddFail(
            () -> update(entityMerged),
            e -> new FailedAction(entityMerged, "Failed to update entity: " + e.getMessage()),
            syncResultBuilder);
      }
      // create identifiers and machine tags if needed
      callExecutor.executeOrAddFailAsync(
          () -> addSubEntities(entityMerged),
          e ->
              new FailedAction(
                  entityMerged,
                  "Failed to add identifiers and machine tags of entity: " + e.getMessage()),
          syncResultBuilder);

      entityMatchBuilder.update(true);
    }

    return entityMatchBuilder.build();
  }

  public T createEntity(T newEntity) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> {
          UUID createdKey = create(newEntity);
          return get(createdKey);
        },
        e -> new FailedAction(newEntity, "Failed to create entity: " + e.getMessage()),
        syncResultBuilder,
        newEntity);
  }

  public T getEntity(T entity) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> get(entity.getKey()),
        e -> new FailedAction(entity, "Failed to get updated entity: " + e.getMessage()),
        syncResultBuilder,
        entity);
  }

  protected void addSubEntities(T entity) {
    entity.getIdentifiers().stream()
        .filter(i -> i.getKey() == null)
        .forEach(i -> addIdentifierToEntity(entity.getKey(), i));
    entity.getMachineTags().stream()
        .filter(mt -> mt.getKey() == null)
        .forEach(mt -> addMachineTagToEntity(entity.getKey(), mt));
  }

  protected abstract T get(UUID key);

  protected abstract void update(T entity);

  protected abstract UUID create(T entity);

  protected abstract void addIdentifierToEntity(UUID entityKey, Identifier identifier);

  protected abstract void addMachineTagToEntity(UUID entityKey, MachineTag machineTag);
}
