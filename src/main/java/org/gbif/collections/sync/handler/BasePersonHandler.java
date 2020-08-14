package org.gbif.collections.sync.handler;

import java.util.List;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.SyncResult.SyncResultBuilder;
import org.gbif.collections.sync.config.SyncConfig;

import static org.gbif.collections.sync.Utils.isPersonInContacts;

public abstract class BasePersonHandler extends EntityHandler<Person> {

  public BasePersonHandler(SyncConfig syncConfig, SyncResultBuilder syncResultBuilder) {
    super(syncConfig, syncResultBuilder);
  }

  public <T extends CollectionEntity & Contactable> Person createPersonAndLinkToEntities(
      Person newPerson, List<T> entities) {
    // create new person in the registry and link it to the entities
    Person createdPerson = createEntity(newPerson);

    if (createdPerson.getKey() != null) {
      linkPersonToEntity(newPerson, entities);
    }

    return createdPerson;
  }

  public <T extends CollectionEntity & Contactable> void linkPersonToEntity(
      Person newPerson, List<T> entities) {
    entities.forEach(
        e ->
            callExecutor.executeOrAddFail(
                () -> addPersonToEntity(e, newPerson),
                ex ->
                    new FailedAction(
                        newPerson,
                        "failed to add person to entity " + e.getKey() + ": " + ex.getMessage()),
                syncResultBuilder));
  }

  public <T extends CollectionEntity & Contactable> void unlinkPersonFromEntity(
      Person personToRemove, List<T> entities) {
    callExecutor.executeOrAddFail(
        () -> entities.forEach(e -> removePersonFromEntity(e, personToRemove)),
        e -> new FailedAction(personToRemove, "Failed to remove person: " + e.getMessage()),
        syncResultBuilder);
  }

  private <T extends CollectionEntity & Contactable> void addPersonToEntity(T e, Person p) {
    // they can be null in dry runs or if the creation of a collection/institution fails
    if (isPersonInContacts(p.getKey(), e.getContacts())) {
      return;
    }

    if (e instanceof Collection) {
      grSciCollHttpClient.addPersonToCollection(p.getKey(), e.getKey());
    } else if (e instanceof Institution) {
      grSciCollHttpClient.addPersonToInstitution(p.getKey(), e.getKey());
    }

    // we add it to the contacts to avoid adding it again if there are duplicates in IH
    e.getContacts().add(p);
  }

  private <T extends CollectionEntity & Contactable> void removePersonFromEntity(T e, Person p) {
    // they can be null in dry runs or if the creation of a collection/institution fails
    if (!isPersonInContacts(p.getKey(), e.getContacts())) {
      return;
    }

    if (e instanceof Collection) {
      grSciCollHttpClient.removePersonFromCollection(p.getKey(), e.getKey());
    } else if (e instanceof Institution) {
      grSciCollHttpClient.removePersonFromInstitution(p.getKey(), e.getKey());
    }
  }
}
