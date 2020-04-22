package org.gbif.collections.sync.idigbio.match;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.idigbio.IDigBioRecord;
import org.gbif.collections.sync.staff.StaffNormalized;

import lombok.Builder;

import static org.gbif.collections.sync.staff.StaffUtils.compareLists;
import static org.gbif.collections.sync.staff.StaffUtils.compareStrings;

public class Matcher {

  final Map<UUID, Institution> institutionsByKey;
  final Map<UUID, Collection> collectionsByKey;

  @Builder
  private Matcher(List<Institution> institutions, List<Collection> collections) {
    institutionsByKey =
        institutions.stream().collect(Collectors.toMap(Institution::getKey, i -> i));
    collectionsByKey = collections.stream().collect(Collectors.toMap(Collection::getKey, c -> c));
  }

  public MatchResult match(IDigBioRecord iDigBioRecord) {
    return MatchResult.builder()
        .iDigBioRecord(iDigBioRecord)
        .institutionMatched(institutionsByKey.get(iDigBioRecord.getGrbioInstMatch()))
        .collectionMatched(collectionsByKey.get(iDigBioRecord.getGrbioCollMatch()))
        .build();
  }

  public static Set<Person> matchContact(IDigBioRecord record, Set<Person> grSciCollPersons) {
    if (grSciCollPersons == null) {
      return Collections.emptySet();
    }

    StaffNormalized idigbioContact = StaffNormalized.fromIDigBioContact(record);
    Set<Person> exactMatches = new HashSet<>();
    Set<Person> fuzzyMatches = new HashSet<>();
    for (Person person : grSciCollPersons) {
      StaffNormalized personNormalized = StaffNormalized.fromGrSciCollPerson(person);
      boolean emailMatch = compareLists(personNormalized.getEmails(), idigbioContact.getEmails());
      boolean nameMatch =
          compareStrings(personNormalized.getFullName(), idigbioContact.getFullName());
      boolean positionMatch =
          Objects.equals(personNormalized.getPosition(), idigbioContact.getPosition());
      boolean primaryInstMatch =
          Objects.equals(
              idigbioContact.getPrimaryInstitutionKey(),
              personNormalized.getPrimaryInstitutionKey());
      boolean primaryCollMatch =
          Objects.equals(
              idigbioContact.getPrimaryCollectionKey(), personNormalized.getPrimaryCollectionKey());

      if (!emailMatch || !nameMatch) {
        continue;
      }

      if (positionMatch && primaryInstMatch && primaryCollMatch) {
        exactMatches.add(person);
      } else if (person.getPosition() == null || positionMatch) {
        fuzzyMatches.add(person);
      }
    }

    if (!exactMatches.isEmpty()) {
      return exactMatches;
    } else {
      return fuzzyMatches;
    }
  }
}
