package org.gbif.collections.sync.common.handler;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.MasterSourceMetadata;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.proxy.CallExecutor;

import java.util.UUID;

public class CollectionHandler extends BasePrimaryEntityHandler<Collection> {

  private CollectionHandler(CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    super(callExecutor, grSciCollHttpClient);
  }

  public static CollectionHandler create(
      CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    return new CollectionHandler(callExecutor, grSciCollHttpClient);
  }

  @Override
  protected Collection getCall(UUID key) {
    return grSciCollHttpClient.getCollection(key);
  }

  @Override
  protected void updateCall(Collection entity) {
    grSciCollHttpClient.updateCollection(entity);
  }

  @Override
  protected UUID createCall(Collection entity) {
    return grSciCollHttpClient.createCollection(entity);
  }

  @Override
  protected void addIdentifierToEntityCall(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToCollection(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntityCall(UUID entityKey, MachineTag machineTag) {
    grSciCollHttpClient.addMachineTagToCollection(entityKey, machineTag);
  }

  @Override
  protected void addMasterSourceMetadataToEntityCall(
      UUID entityKey, MasterSourceMetadata metadata) {
    grSciCollHttpClient.addMasterSourceMetadataToCollection(entityKey, metadata);
  }

  public Integer addContactToEntityCall(UUID entityKey, Contact contact) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> grSciCollHttpClient.addContactToCollection(entityKey, contact),
        exceptionHandler(contact, "Failed to create contact to collection " + entityKey));
  }

  public boolean updateContactInEntityCall(UUID entityKey, Contact oldContact, Contact newContact) {
    // check if we need to update the contact
    if (!newContact.lenientEquals(oldContact)) {
      callExecutor.executeOrAddFail(
          () -> grSciCollHttpClient.updateContactInCollection(entityKey, newContact),
          exceptionHandler(newContact, "Failed to update contact in collection " + entityKey));

      return true;
    }
    return false;
  }

  public void removeContactFromEntityCall(UUID entityKey, int contactKey) {
    callExecutor.executeOrAddFail(
        () -> grSciCollHttpClient.removeContactFromCollection(entityKey, contactKey),
        exceptionHandler(contactKey, "Failed to remove contact from collection " + entityKey));
  }
}
