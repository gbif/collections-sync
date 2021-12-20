package org.gbif.collections.sync.common.handler;

import org.gbif.api.model.collections.*;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.proxy.CallExecutor;

import java.util.List;
import java.util.UUID;

import static org.gbif.collections.sync.common.Utils.isPersonInContacts;

public class PersonHandler extends BaseEntityHandler<Person> {

  private PersonHandler(CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    super(callExecutor, grSciCollHttpClient);
  }

  public static PersonHandler create(
      CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    return new PersonHandler(callExecutor, grSciCollHttpClient);
  }

  public <T extends CollectionEntity & Contactable> void linkPersonToEntity(
      Person person, List<T> entities) {
    if (person.getKey() == null) {
      return;
    }

    entities.forEach(
        e ->
            callExecutor.executeOrAddFail(
                () -> addPersonToEntity(e, person),
                exceptionHandler(person, "failed to add person to entity " + e.getKey())));
  }

  public <T extends CollectionEntity & Contactable> void unlinkPersonFromEntity(
      Person personToRemove, List<T> entities) {
    callExecutor.executeOrAddFail(
        () -> entities.forEach(e -> removePersonFromEntity(e, personToRemove)),
        exceptionHandler(personToRemove, "Failed to remove person"));
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

    // we add it to the contacts to avoid adding it again if there are duplicates in the
    // source(e.g.:IH)
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

    e.getContacts().remove(p);
  }

  @Override
  protected Person getCall(UUID key) {
    return grSciCollHttpClient.getPerson(key);
  }

  @Override
  protected void updateCall(Person entity) {
    grSciCollHttpClient.updatePerson(entity);
  }

  @Override
  protected UUID createCall(Person entity) {
    return grSciCollHttpClient.createPerson(entity);
  }

  @Override
  protected void addIdentifierToEntityCall(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToPerson(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntityCall(UUID entityKey, MachineTag machineTag) {
    // not implemented
  }
}
