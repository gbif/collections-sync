package org.gbif.collections.sync.clients.proxy;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.IDigBioDataLoader.IDigBioData;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import java.util.*;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Getter;

import static org.gbif.collections.sync.idigbio.IDigBioUtils.IS_IDIGBIO_COLLECTION_UUID_MT;

@Getter
public class IDigBioProxyClient extends BaseProxyClient {

  private final IDigBioConfig iDigBioConfig;
  private final DataLoader<IDigBioData> dataLoader;
  private List<IDigBioRecord> iDigBioRecords = new ArrayList<>();
  private Map<UUID, Institution> institutionsByKey = new HashMap<>();
  private Map<UUID, Collection> collectionsByKey = new HashMap<>();
  private Map<UUID, Set<Collection>> collectionsByInstitution = new HashMap<>();
  // institutions created when an IDigBio record has no match. We need to store them in order not to
  // duplicate them. For example, the institution with code CCBER has no match and it's present
  // multiple times because it has multiple collections.
  private final Set<Institution> newlyCreatedIDigBioInstitutions = new HashSet<>();
  private final Map<String, Collection> collectionsByIDigBioUuid = new HashMap<>();

  @Builder
  public IDigBioProxyClient(IDigBioConfig iDigBioConfig, DataLoader<IDigBioData> dataLoader) {
    super(iDigBioConfig.getSyncConfig());
    this.iDigBioConfig = iDigBioConfig;
    this.dataLoader = dataLoader;
    loadData();
  }

  private void loadData() {
    IDigBioData data = dataLoader.loadData();
    institutionsByKey =
        data.getInstitutions().stream().collect(Collectors.toMap(Institution::getKey, i -> i));
    collectionsByKey =
        data.getCollections().stream().collect(Collectors.toMap(Collection::getKey, c -> c));
    collectionsByInstitution =
        data.getCollections().stream()
            .filter(c -> c.getInstitutionKey() != null)
            .collect(
                Collectors.groupingBy(
                    Collection::getInstitutionKey, HashMap::new, Collectors.toSet()));
    this.iDigBioRecords = data.getIDigBioRecords();

    // map collections by the iDigBio UUID machine tag
    data.getCollections().stream()
        .filter(o -> o.getMachineTags() != null)
        .forEach(
            o ->
                o.getMachineTags().stream()
                    .filter(IS_IDIGBIO_COLLECTION_UUID_MT)
                    .forEach(
                        mt -> {
                          if (collectionsByIDigBioUuid.containsKey(mt.getValue())) {
                            throw new IllegalArgumentException(
                                "More than 1 collection is linked thru machine tags to the iDigBio collection UUID "
                                    + mt.getValue());
                          }
                          collectionsByIDigBioUuid.put(mt.getValue(), o);
                        }));
  }

  @Override
  public boolean updateCollection(Collection oldCollection, Collection newCollection) {
    boolean updated = super.updateCollection(oldCollection, newCollection);
    if (updated) {
      updateCollectionInMemory(oldCollection, newCollection);
    }
    return updated;
  }

  @Override
  public Institution createInstitution(Institution institution) {
    Institution createdInstitution = super.createInstitution(institution);
    addNewlyCreatedIDigBioInstitution(createdInstitution);
    return createdInstitution;
  }

  @Override
  public boolean updateInstitution(Institution oldInstitution, Institution newInstitution) {
    boolean updated = super.updateInstitution(oldInstitution, newInstitution);
    if (updated) {
      updateInstitutionInMemory(newInstitution);
    }
    return updated;
  }

  private void updateCollectionInMemory(Collection oldCollection, Collection newCollection) {
    Collection updatedCollection = collectionHandler.get(newCollection);
    if (updatedCollection != null && updatedCollection.getKey() != null) {
      collectionsByKey.put(oldCollection.getKey(), updatedCollection);

      if (updatedCollection.getInstitutionKey() != null
          && collectionsByInstitution.containsKey(oldCollection.getInstitutionKey())) {
        Set<Collection> collectionsInInstitution =
            collectionsByInstitution.get(updatedCollection.getInstitutionKey());
        collectionsInInstitution.removeIf(c -> c.getKey().equals(oldCollection.getKey()));
        collectionsInInstitution.add(updatedCollection);
      }
    }
  }

  public void updateInstitutionInMemory(Institution newInstitution) {
    Institution institution = institutionHandler.get(newInstitution);
    if (institution != null && institution.getKey() != null) {
      institutionsByKey.put(institution.getKey(), institution);
    }
  }

  public void addNewlyCreatedIDigBioInstitution(Institution institution) {
    if (institution != null) {
      newlyCreatedIDigBioInstitutions.add(institution);
    }
  }
}
