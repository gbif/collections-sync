package org.gbif.collections.sync.handler;

import java.util.UUID;
import java.util.function.Function;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.LenientEquals;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.http.CallExecutor;
import org.gbif.collections.sync.http.clients.GrSciCollHttpClient;

public abstract class BaseEntityHandler<
        T extends LenientEquals<T> & CollectionEntity & Identifiable & MachineTaggable>
    implements EntityHandler<T> {

  protected final CallExecutor callExecutor;
  protected GrSciCollHttpClient grSciCollHttpClient;

  public BaseEntityHandler(SyncConfig syncConfig) {
    this.callExecutor = new CallExecutor(syncConfig);
    if (syncConfig != null) {
      this.grSciCollHttpClient = GrSciCollHttpClient.getInstance(syncConfig);
    }
  }

  @Override
  public boolean update(T oldEntity, T newEntity) {
    if (!newEntity.equals(oldEntity)) {
      // check if we need to update the entity
      if (!newEntity.lenientEquals(oldEntity)) {
        callExecutor.executeOrAddFail(
            () -> updateCall(newEntity),
            exceptionHandler(newEntity, "Failed to update entity"));
      }
      // create identifiers and machine tags if needed
      callExecutor.executeOrAddFailAsync(
          () -> addSubEntities(newEntity),
          exceptionHandler(newEntity, "Failed to add identifiers and machine tags of entity"));

      return true;
    }
    return false;
  }

  @Override
  public T create(T newEntity) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> {
          UUID createdKey = createCall(newEntity);
          return getCall(createdKey);
        },
        exceptionHandler(newEntity, "Failed to create entity"),
        newEntity);
  }

  @Override
  public T get(T entity) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> getCall(entity.getKey()),
        exceptionHandler(entity, "Failed to get updated entity"),
        entity);
  }

  protected void addSubEntities(T entity) {
    entity.getIdentifiers().stream()
        .filter(i -> i.getKey() == null)
        .forEach(i -> addIdentifierToEntityCall(entity.getKey(), i));
    entity.getMachineTags().stream()
        .filter(mt -> mt.getKey() == null)
        .forEach(mt -> addMachineTagToEntityCall(entity.getKey(), mt));
  }

  protected abstract T getCall(UUID key);

  protected abstract void updateCall(T entity);

  protected abstract UUID createCall(T entity);

  protected abstract void addIdentifierToEntityCall(UUID entityKey, Identifier identifier);

  protected abstract void addMachineTagToEntityCall(UUID entityKey, MachineTag machineTag);

  public Function<Throwable, FailedAction> exceptionHandler(Object obj, String msg) {
    return e -> new FailedAction(obj, msg + ": " + e.getMessage());
  }
}
