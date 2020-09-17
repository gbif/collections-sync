package org.gbif.collections.sync.clients.proxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.IDigBioDataLoader.IDigBioData;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

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
  private Map<UUID, Person> personsByKey = new HashMap<>();
  private Map<UUID, Set<Collection>> collectionsByInstitution = new HashMap<>();
  // institutions created when an IDigBio record has no match. We need to store them in order not to
  // duplicate them. For example, the institution with code CCBER has no match and it's present
  // multiple times because it has multiple collections.
  private final Set<Institution> newlyCreatedIDigBioInstitutions = new HashSet<>();
  private Set<Person> persons = new HashSet<>();
  private Map<String, Collection> collectionsByIDigBioUuid = new HashMap<>();

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
    personsByKey = data.getPersons().stream().collect(Collectors.toMap(Person::getKey, p -> p));
    collectionsByInstitution =
        data.getCollections().stream()
            .filter(c -> c.getInstitutionKey() != null)
            .collect(
                Collectors.groupingBy(
                    Collection::getInstitutionKey, HashMap::new, Collectors.toSet()));
    this.persons = new HashSet<>(data.getPersons());
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

  @Override
  public Person createPerson(Person person) {
    Person createdPerson = personHandler.create(person);
    addNewPersonInMemory(createdPerson);
    return createdPerson;
  }

  @Override
  public boolean updatePerson(Person oldPerson, Person newPerson) {
    boolean updated = personHandler.update(oldPerson, newPerson);
    if (updated) {
      updatePersonInMemory(oldPerson, newPerson);
    }
    return updated;
  }

  private void updateCollectionInMemory(Collection oldCollection, Collection newCollection) {
    Collection updatedCollection = collectionHandler.get(newCollection);
    if (updatedCollection != null && updatedCollection.getKey() != null) {
      collectionsByKey.replace(updatedCollection.getKey(), updatedCollection);

      if (updatedCollection.getInstitutionKey() != null
          && collectionsByInstitution.containsKey(oldCollection.getInstitutionKey())) {
        collectionsByInstitution.get(updatedCollection.getInstitutionKey()).remove(oldCollection);
        collectionsByInstitution.get(updatedCollection.getInstitutionKey()).add(updatedCollection);
      }
    }
  }

  public void updateInstitutionInMemory(Institution newInstitution) {
    Institution institution = institutionHandler.get(newInstitution);
    if (institution != null && institution.getKey() != null) {
      institutionsByKey.replace(institution.getKey(), institution);
    }
  }

  public void addNewPersonInMemory(Person person) {
    if (person != null) {
      persons.add(person);
      if (person.getKey() != null) {
        personsByKey.put(person.getKey(), person);
      }
    }
  }

  public void updatePersonInMemory(Person oldPerson, Person updatedPerson) {
    if (oldPerson != null) {
      persons.remove(oldPerson);
      if (oldPerson.getKey() != null) {
        personsByKey.remove(oldPerson.getKey());
      }
    }
    if (updatedPerson != null) {
      updatedPerson = personHandler.get(updatedPerson);
      persons.add(updatedPerson);
      if (updatedPerson.getKey() != null) {
        personsByKey.replace(updatedPerson.getKey(), updatedPerson);
      }
    }
  }

  public void addNewlyCreatedIDigBioInstitution(Institution institution) {
    if (institution != null) {
      newlyCreatedIDigBioInstitutions.add(institution);
    }
  }
}
