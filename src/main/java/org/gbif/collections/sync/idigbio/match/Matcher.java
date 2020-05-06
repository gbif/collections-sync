package org.gbif.collections.sync.idigbio.match;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.idigbio.IDigBioRecord;
import org.gbif.collections.sync.idigbio.IDigBioUtils;
import org.gbif.collections.sync.staff.StaffNormalized;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.staff.StaffUtils.compareLists;
import static org.gbif.collections.sync.staff.StaffUtils.compareStrings;

@Slf4j
public class Matcher {

  final Map<UUID, Institution> institutionsByKey;
  final Map<UUID, Collection> collectionsByKey;
  final Map<UUID, Set<Collection>> collectionsByInstitution;

  @Builder
  private Matcher(List<Institution> institutions, List<Collection> collections) {
    institutionsByKey =
        institutions.stream().collect(Collectors.toMap(Institution::getKey, i -> i));
    collectionsByKey = collections.stream().collect(Collectors.toMap(Collection::getKey, c -> c));
    collectionsByInstitution =
        collections.stream()
            .filter(c -> c.getInstitutionKey() != null)
            .collect(
                Collectors.groupingBy(
                    Collection::getInstitutionKey,
                    Collectors.mapping(Function.identity(), Collectors.toSet())));
  }

  public MatchResult match(IDigBioRecord iDigBioRecord) {
    MatchResult.MatchResultBuilder result = MatchResult.builder().iDigBioRecord(iDigBioRecord);

    Institution institutionMatch = institutionsByKey.get(iDigBioRecord.getGrbioInstMatch());
    result.institutionMatched(institutionMatch);
    Collection collectionMatch = collectionsByKey.get(iDigBioRecord.getGrbioCollMatch());
    result.collectionMatched(collectionMatch);

    if (collectionMatch == null && institutionMatch != null) {
      // if no match was provided manually we try to find it among the institution collections
      matchCollection(institutionMatch.getKey(), iDigBioRecord)
          .ifPresent(result::collectionMatched);
    }

    return result.build();
  }

  private Optional<Collection> matchCollection(UUID institutionKey, IDigBioRecord iDigBioRecord) {
    Set<Collection> collections = collectionsByInstitution.get(institutionKey);
    if (collections == null) {
      return Optional.empty();
    }

    Set<String> iDigBioCodes = IDigBioUtils.getIdigbioCode(iDigBioRecord.getCollectionCode());
    if (iDigBioCodes.isEmpty()) {
      return Optional.empty();
    }

    List<Collection> codeMatches =
        collections.stream()
            .filter(c -> iDigBioCodes.contains(c.getCode()))
            .collect(Collectors.toList());

    if (codeMatches.isEmpty()) {
      codeMatches =
          collections.stream()
              .filter(c -> c.getName().equalsIgnoreCase(iDigBioRecord.getCollection()))
              .collect(Collectors.toList());
    }

    if (codeMatches.size() > 1) {
      log.warn(
          "Idigbio record {} matches with more than 1 collection: {}", iDigBioRecord, codeMatches);
    }

    return codeMatches.isEmpty() ? Optional.empty() : Optional.of(codeMatches.get(0));
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
