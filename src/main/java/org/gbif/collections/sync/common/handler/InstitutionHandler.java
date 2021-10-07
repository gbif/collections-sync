package org.gbif.collections.sync.common.handler;

import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.proxy.CallExecutor;

import java.util.UUID;

public class InstitutionHandler extends BaseEntityHandler<Institution> {

  private InstitutionHandler(CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    super(callExecutor, grSciCollHttpClient);
  }

  public static InstitutionHandler create(
      CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    return new InstitutionHandler(callExecutor, grSciCollHttpClient);
  }

  @Override
  protected Institution getCall(UUID key) {
    return grSciCollHttpClient.getInstitution(key);
  }

  @Override
  protected void updateCall(Institution entity) {
    grSciCollHttpClient.updateInstitution(entity);
  }

  @Override
  protected UUID createCall(Institution entity) {
    return grSciCollHttpClient.createInstitution(entity);
  }

  @Override
  protected void addIdentifierToEntityCall(UUID entityKey, Identifier identifier) {
    grSciCollHttpClient.addIdentifierToInstitution(entityKey, identifier);
  }

  @Override
  protected void addMachineTagToEntityCall(UUID entityKey, MachineTag machineTag) {
    grSciCollHttpClient.addMachineTagToInstitution(entityKey, machineTag);
  }

  public Integer addContactToEntityCall(UUID entityKey, Contact contact) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> grSciCollHttpClient.addContactToInstitution(entityKey, contact),
        exceptionHandler(contact, "Failed to create contact to institution " + entityKey));
  }

  public boolean updateContactInEntityCall(UUID entityKey, Contact oldContact, Contact newContact) {
    // check if we need to update the contact
    if (!newContact.lenientEquals(oldContact)) {
      callExecutor.executeOrAddFail(
          () -> grSciCollHttpClient.updateContactInInstitution(entityKey, newContact),
          exceptionHandler(newContact, "Failed to update contact in institution " + entityKey));

      return true;
    }
    return false;
  }

  public void removeContactFromEntityCall(UUID entityKey, int contactKey) {
    callExecutor.executeOrAddFail(
        () -> grSciCollHttpClient.removeContactFromInstitution(entityKey, contactKey),
        exceptionHandler(contactKey, "Failed to remove contact from institution " + entityKey));
  }
}
