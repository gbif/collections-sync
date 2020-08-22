package org.gbif.collections.sync.clients.proxy;

import java.util.List;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.common.handler.CollectionHandler;
import org.gbif.collections.sync.common.handler.InstitutionHandler;
import org.gbif.collections.sync.common.handler.PersonHandler;
import org.gbif.collections.sync.config.SyncConfig;

public abstract class BaseProxyClient implements GrSciCollProxyClient {

  protected GrSciCollHttpClient grSciCollHttpClient;
  protected final CallExecutor callExecutor;
  protected CollectionHandler collectionHandler;
  protected InstitutionHandler institutionHandler;
  protected PersonHandler personHandler;

  public BaseProxyClient(SyncConfig syncConfig) {
    this.callExecutor = new CallExecutor(syncConfig);
    if (syncConfig != null) {
      grSciCollHttpClient = GrSciCollHttpClient.getInstance(syncConfig);
    }
    this.collectionHandler = CollectionHandler.create(syncConfig);
    this.institutionHandler = InstitutionHandler.create(syncConfig);
    this.personHandler = PersonHandler.create(syncConfig);
  }

  public Collection createCollection(Collection collection) {
    return collectionHandler.create(collection);
  }

  public boolean updateCollection(Collection oldCollection, Collection newCollection) {
    return collectionHandler.update(oldCollection, newCollection);
  }

  public Institution createInstitution(Institution institution) {
    return institutionHandler.create(institution);
  }

  public boolean updateInstitution(Institution oldInstitution, Institution newInstitution) {
    return institutionHandler.update(oldInstitution, newInstitution);
  }

  public Person createPerson(Person person) {
    return personHandler.create(person);
  }

  public boolean updatePerson(Person oldPerson, Person newPerson) {
    return personHandler.update(oldPerson, newPerson);
  }

  public <T extends CollectionEntity & Contactable> void linkPersonToEntity(
      Person person, List<T> entities) {
    personHandler.linkPersonToEntity(person, entities);
  }

  public <T extends CollectionEntity & Contactable> void unlinkPersonFromEntity(
      Person personToRemove, List<T> entities) {
    personHandler.unlinkPersonFromEntity(personToRemove, entities);
  }
}
