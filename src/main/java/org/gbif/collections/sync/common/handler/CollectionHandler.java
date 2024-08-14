package org.gbif.collections.sync.common.handler;

import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.MasterSourceMetadata;
import org.gbif.api.model.collections.descriptors.DescriptorGroup;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.proxy.CallExecutor;
import org.gbif.collections.sync.common.converter.ConvertedCollection;

public class CollectionHandler extends BasePrimaryEntityHandler<Collection> {

  private static final String IH_NS = "ih.gbif.org";
  private static final String COLL_SUMMARY_MT = "collectionSummaryDescriptorGroupKey";
  private static final String COLLECTORS_MT = "importantCollectorsDescriptorGroupKey";

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

  public Collection createConvertedCollection(ConvertedCollection convertedCollection) {
    Collection createdCollection = super.create(convertedCollection.getCollection());

    if (convertedCollection.getCollectionSummary() != null) {
      createDescriptorGroup(
          createdCollection.getKey(),
          convertedCollection.getCollectionSummary(),
          convertedCollection.getCollectionSummaryFile(),
          COLL_SUMMARY_MT);
    }

    if (convertedCollection.getImportantCollectors() != null) {
      createDescriptorGroup(
          createdCollection.getKey(),
          convertedCollection.getImportantCollectors(),
          convertedCollection.getImportantCollectorsFile(),
          COLLECTORS_MT);
    }

    return createdCollection;
  }

  public boolean updateConvertedCollection(
      Collection oldCollection, ConvertedCollection convertedCollection) {
    boolean result = super.update(oldCollection, convertedCollection.getCollection());

    Function<String, Optional<MachineTag>> mtFinder =
        name ->
            convertedCollection.getCollection().getMachineTags().stream()
                .filter(
                    mt ->
                        mt.getNamespace().equals(IH_NS)
                            && mt.getName().equals(name)
                            && mt.getValue() != null)
                .findFirst();

    if (convertedCollection.getCollectionSummary() != null) {
      Optional<MachineTag> collectionSummaryMt = mtFinder.apply(COLL_SUMMARY_MT);
      if (collectionSummaryMt.isPresent()) {
        updateCollectionDescriptor(
            convertedCollection.getCollection().getKey(),
            convertedCollection.getCollectionSummary(),
            convertedCollection.getCollectionSummaryFile(),
            Long.parseLong(collectionSummaryMt.get().getValue()));
      } else {
        createDescriptorGroup(
            oldCollection.getKey(),
            convertedCollection.getCollectionSummary(),
            convertedCollection.getCollectionSummaryFile(),
            COLL_SUMMARY_MT);
      }
    }

    if (convertedCollection.getImportantCollectors() != null) {
      Optional<MachineTag> importantCollectorsMt = mtFinder.apply(COLLECTORS_MT);
      if (importantCollectorsMt.isPresent()) {
        updateCollectionDescriptor(
            convertedCollection.getCollection().getKey(),
            convertedCollection.getImportantCollectors(),
            convertedCollection.getImportantCollectorsFile(),
            Long.parseLong(importantCollectorsMt.get().getValue()));
      } else {
        createDescriptorGroup(
            oldCollection.getKey(),
            convertedCollection.getImportantCollectors(),
            convertedCollection.getImportantCollectorsFile(),
            COLLECTORS_MT);
      }
    }

    return result;
  }

  private void updateCollectionDescriptor(
      UUID collectionKey,
      DescriptorGroup descriptorGroup,
      Path descriptorFile,
      long descriptorGroupKey) {
    callExecutor.executeOrAddFail(
        () ->
            grSciCollHttpClient.updateCollectionDescriptorGroup(
                collectionKey,
                descriptorGroupKey,
                descriptorGroup.getTitle(),
                descriptorGroup.getDescription(),
                descriptorFile),
        exceptionHandler(
            descriptorGroupKey,
            "Couldn't update descriptor group key "
                + descriptorGroupKey
                + " and collection "
                + collectionKey));
  }

  private void createDescriptorGroup(
      UUID collectionKey,
      DescriptorGroup descriptorGroup,
      Path descriptorFile,
      String machineTagName) {
    Long descriptorGroupKey =
        callExecutor.executeAndReturnOrAddFail(
            () ->
                grSciCollHttpClient.createCollectionDescriptorGroup(
                    collectionKey,
                    descriptorGroup.getTitle(),
                    descriptorGroup.getDescription(),
                    descriptorFile),
            exceptionHandler(
                descriptorGroup,
                "Couldn't create descriptor group for collection " + collectionKey),
            null);

    if (descriptorGroupKey != null) {
      callExecutor.executeOrAddFail(
          () ->
              grSciCollHttpClient.addMachineTagToCollection(
                  collectionKey,
                  new MachineTag(IH_NS, machineTagName, String.valueOf(descriptorGroupKey))),
          exceptionHandler(
              descriptorGroupKey,
              "Couldn't add machine tag for descriptor group "
                  + descriptorGroupKey
                  + " and collection "
                  + collectionKey));
    }
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
