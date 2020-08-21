package org.gbif.collections.sync.handler;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.LenientEquals;
import org.gbif.api.model.registry.MachineTaggable;

public interface EntityHandler<
    T extends LenientEquals<T> & CollectionEntity & Identifiable & MachineTaggable> {

  boolean update(T oldEntity, T newEntity);

  T create(T newEntity);

  T get(T entity);
}
