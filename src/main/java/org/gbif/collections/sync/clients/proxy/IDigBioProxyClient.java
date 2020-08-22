package org.gbif.collections.sync.clients.proxy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.DataLoader.GrSciCollData;
import org.gbif.collections.sync.config.IDigBioConfig;

import lombok.Builder;
import lombok.Getter;

@Getter
public class IDigBioProxyClient extends BaseProxyClient {

  private final IDigBioConfig iDigBioConfig;
  private final DataLoader dataLoader;
  private Map<UUID, Institution> institutionsByKey = new HashMap<>();
  private Map<UUID, Collection> collectionsByKey = new HashMap<>();
  private Map<UUID, Person> personsByKey = new HashMap<>();
  private Map<UUID, Set<Collection>> collectionsByInstitution = new HashMap<>();
  // institutions created when an IDigBio record has no match. We need to store them in order not to
  // duplicate them. For example, the institution with code CCBER has no match and it's present
  // multiple times because it has multiple collections.
  private final Set<Institution> newlyCreatedIDigBioInstitutions = new HashSet<>();
  private Set<Person> persons = new HashSet<>();

  @Builder
  public IDigBioProxyClient(IDigBioConfig iDigBioConfig, DataLoader dataLoader) {
    super(iDigBioConfig.getSyncConfig());
    this.iDigBioConfig = iDigBioConfig;
    if (dataLoader != null) {
      this.dataLoader = dataLoader;
    } else {
      this.dataLoader = DataLoader.create(iDigBioConfig.getSyncConfig());
    }
    loadData();
  }

  private void loadData() {
    GrSciCollData data = dataLoader.fetchGrSciCollData();
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
    this.persons = new HashSet<>(persons);
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
    }
  }

  public void updatePersonInMemory(Person oldPerson, Person updatedPerson) {
    if (oldPerson != null) {
      persons.remove(oldPerson);
    }
    if (updatedPerson != null) {
      updatedPerson = personHandler.get(updatedPerson);
      persons.add(updatedPerson);
    }
  }

  public void addNewlyCreatedIDigBioInstitution(Institution institution) {
    if (institution != null) {
      newlyCreatedIDigBioInstitutions.add(institution);
    }
  }
}
