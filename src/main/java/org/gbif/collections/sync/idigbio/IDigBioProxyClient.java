package org.gbif.collections.sync.idigbio;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.common.BaseProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.DataLoader.GrSciCollData;
import org.gbif.collections.sync.config.IDigBioConfig;

import lombok.Builder;
import lombok.Getter;

// TODO: el update q actualize siempre y el get q mire primero en memory??
@Getter
public class IDigBioProxyClient extends BaseProxyClient {

  private final DataLoader dataLoader;
  private Map<UUID, Institution> institutionsByKey = new HashMap<>();
  private Map<UUID, Collection> collectionsByKey = new HashMap<>();
  private Map<UUID, Person> personsByKey = new HashMap<>();
  private Map<UUID, Set<Collection>> collectionsByInstitution = new HashMap<>();
  // institutions created when an IDigBio record has no match. We need to store them in order not to
  // duplicate them. For example, the institution with code CCBER has no match and it's present
  // multiple times because it has multiple collections.
  private Set<Institution> newlyCreatedIDigBioInstitutions = new HashSet<>();
  private Set<Person> persons = new HashSet<>();

  // TODO: tiene q ser singleton??
  @Builder
  public IDigBioProxyClient(IDigBioConfig iDigBioConfig, DataLoader dataLoader) {
    super(iDigBioConfig.getSyncConfig());
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

  public Collection createCollection(Collection collection) {
    return collectionHandler.create(collection);
  }

  public boolean updateCollection(Collection oldCollection, Collection newCollection) {
    boolean updated = collectionHandler.update(oldCollection, newCollection);
    if (updated) {
      updateCollectionInMemory(oldCollection, newCollection);
    }
    return updated;
  }

  public Institution createInstitution(Institution institution) {
    return institutionHandler.create(institution);
  }

  public boolean updateInstitution(Institution oldInstitution, Institution newInstitution) {
    boolean updated = institutionHandler.update(oldInstitution, newInstitution);
    if (updated) {
      updateInstitutionInMemory(newInstitution);
    }
    return updated;
  }

  public Person createPerson(Person person) {
    Person createdPerson = personHandler.create(person);
    addNewPersonInMemory(createdPerson);
    return createdPerson;
  }

  public boolean updatePerson(Person oldPerson, Person newPerson) {
    boolean updated = personHandler.update(oldPerson, newPerson);
    if (updated) {
      updatePersonInMemory(oldPerson, newPerson);
    }
    return updated;
  }

  public <T extends CollectionEntity & Contactable> void linkPersonToEntity(
      Person person, List<T> entities) {
    personHandler.linkPersonToEntity(person, entities);
  }

  public <T extends CollectionEntity & Contactable> void unlinkPersonFromEntity(
      Person personToRemove, List<T> entities) {
    personHandler.unlinkPersonFromEntity(personToRemove, entities);
  }

  private void updateCollectionInMemory(Collection oldCollection, Collection newCollection) {
    Collection updatedCollection = collectionHandler.get(newCollection);
    if (updatedCollection != null && updatedCollection.getKey() != null) {
      collectionsByKey.replace(updatedCollection.getKey(), updatedCollection);

      if (updatedCollection.getInstitutionKey() != null
          // TODO
          && collectionsByInstitution.containsValue(oldCollection)) {
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
