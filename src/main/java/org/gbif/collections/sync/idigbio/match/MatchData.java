package org.gbif.collections.sync.idigbio.match;

import java.util.Collections;
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

import lombok.Builder;
import lombok.Getter;

/**
 * Encapsulates the data needed from GrSciColl to match IDigBio records. It allows to update data so
 * the next matches can use updated data.
 *
 * <p>This class is not thread-safe.
 */
@Getter
public class MatchData {

  private final Map<UUID, Institution> institutionsByKey;
  private final Map<UUID, Collection> collectionsByKey;
  private final Map<UUID, Set<Collection>> collectionsByInstitution;
  // institutions created when an IDigBio record has no match. We need to store them in order not to
  // duplicate them. For example, the institution with code CCBER has no match and it's present
  // multiple times because it has multiple collections.
  private final Set<Institution> newlyCreatedIDigBioInstitutions = new HashSet<>();
  private final Set<Person> persons;

  public MatchData() {
    institutionsByKey = Collections.emptyMap();
    collectionsByKey = Collections.emptyMap();
    collectionsByInstitution = Collections.emptyMap();
    persons = Collections.emptySet();
  }

  @Builder
  public MatchData(
      List<Institution> institutions, List<Collection> collections, List<Person> persons) {
    institutionsByKey =
        institutions.stream().collect(Collectors.toMap(Institution::getKey, i -> i));
    collectionsByKey = collections.stream().collect(Collectors.toMap(Collection::getKey, c -> c));
    collectionsByInstitution =
        collections.stream()
            .filter(c -> c.getInstitutionKey() != null)
            .collect(
                Collectors.groupingBy(
                    Collection::getInstitutionKey, HashMap::new, Collectors.toSet()));
    this.persons = new HashSet<>(persons);
  }

  public void addNewlyCreatedIDigBioInstitution(Institution institution) {
    if (institution != null) {
      newlyCreatedIDigBioInstitutions.add(institution);
    }
  }

  public void updateInstitution(Institution institution) {
    if (institution != null && institution.getKey() != null) {
      institutionsByKey.replace(institution.getKey(), institution);
    }
  }

  public void updateCollection(Collection oldCollection, Collection updatedCollection) {
    if (updatedCollection != null && updatedCollection.getKey() != null) {
      collectionsByKey.replace(updatedCollection.getKey(), updatedCollection);

      if (updatedCollection.getInstitutionKey() != null
          && collectionsByInstitution.containsValue(oldCollection)) {
        collectionsByInstitution.get(updatedCollection.getInstitutionKey()).remove(oldCollection);
        collectionsByInstitution.get(updatedCollection.getInstitutionKey()).add(updatedCollection);
      }
    }
  }

  public void addNewPerson(Person person) {
    if (person != null) {
      persons.add(person);
    }
  }

  public void updatePerson(Person oldPerson, Person updatedPerson) {
    if (oldPerson != null) {
      persons.remove(oldPerson);
    }
    if (updatedPerson != null) {
      persons.add(updatedPerson);
    }
  }
}
