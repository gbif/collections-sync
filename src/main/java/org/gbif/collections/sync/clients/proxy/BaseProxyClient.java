package org.gbif.collections.sync.clients.proxy;

import java.util.List;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.common.handler.ChangeSugesstionHandler;
import org.gbif.collections.sync.common.handler.CollectionHandler;
import org.gbif.collections.sync.common.handler.InstitutionHandler;
import org.gbif.collections.sync.config.SyncConfig;

import java.util.UUID;

public abstract class BaseProxyClient implements GrSciCollProxyClient {

  protected GrSciCollHttpClient grSciCollHttpClient;
  protected final CallExecutor callExecutor;
  protected CollectionHandler collectionHandler;
  protected InstitutionHandler institutionHandler;
  protected ChangeSugesstionHandler changeSugesstionHandler;

  public BaseProxyClient(SyncConfig syncConfig) {
    this.callExecutor = CallExecutor.getInstance(syncConfig);
    if (syncConfig != null
        && syncConfig.getRegistry() != null
        && syncConfig.getRegistry().getWsUrl() != null) {
      grSciCollHttpClient = GrSciCollHttpClient.getInstance(syncConfig.getRegistry());
    }
    this.collectionHandler = CollectionHandler.create(callExecutor, grSciCollHttpClient);
    this.institutionHandler = InstitutionHandler.create(callExecutor, grSciCollHttpClient);
    this.changeSugesstionHandler = ChangeSugesstionHandler.create(callExecutor,grSciCollHttpClient);
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

  public Integer addContactToInstitution(UUID entityKey, Contact contact) {
    return institutionHandler.addContactToEntityCall(entityKey, contact);
  }

  public boolean updateContactInInstitution(
      UUID entityKey, Contact oldContact, Contact newContact) {
    return institutionHandler.updateContactInEntityCall(entityKey, oldContact, newContact);
  }

  public void removeContactFromInstitution(UUID entityKey, int contactKey) {
    institutionHandler.removeContactFromEntityCall(entityKey, contactKey);
  }

  @Override
  public List<Institution> findInstitutionByName(String institutionName) {
    return institutionHandler.listInstitutionsByName(institutionName);
  }

  @Override
  public int createCollectionChangeSuggestion(CollectionChangeSuggestion createSuggestion) {
    return changeSugesstionHandler.createCollectionChangeSuggestion(createSuggestion);
  }

  public List<CollectionChangeSuggestion> getCollectionChangeSuggestion(String ihIdentifier){
    return changeSugesstionHandler.getCall(ihIdentifier);
  }

  public Integer addContactToCollection(UUID entityKey, Contact contact) {
    return collectionHandler.addContactToEntityCall(entityKey, contact);
  }

  public boolean updateContactInCollection(UUID entityKey, Contact oldContact, Contact newContact) {
    return collectionHandler.updateContactInEntityCall(entityKey, oldContact, newContact);
  }

  public void removeContactFromCollection(UUID entityKey, int contactKey) {
    collectionHandler.removeContactFromEntityCall(entityKey, contactKey);
  }
}
