package org.gbif.collections.sync.idigbio.match;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.Utils;
import org.gbif.collections.sync.idigbio.IDigBioRecord;
import org.gbif.collections.sync.idigbio.IDigBioUtils;
import org.gbif.collections.sync.staff.StaffNormalized;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.Utils.countNonNullValues;
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

    if (institutionMatch != null) {
      // we try to find a match among the institution collections
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

    List<String> iDigBioCodes = IDigBioUtils.getIdigbioCodes(iDigBioRecord.getCollectionCode());
    if (iDigBioCodes.isEmpty()) {
      return Optional.empty();
    }

    List<Collection> matches = null;
    if (!Strings.isNullOrEmpty(iDigBioRecord.getSameAs())
        && iDigBioRecord.getSameAs().contains("irn=")) {
      String irn = iDigBioRecord.getSameAs().split("irn=")[1];
      matches =
          collections.stream()
              .filter(c -> c.getIdentifiers() != null)
              .filter(
                  c ->
                      c.getIdentifiers().stream()
                          .anyMatch(i -> i.getIdentifier().equals(Utils.encodeIRN(irn))))
              .collect(Collectors.toList());
    } else {
      Predicate<Collection> hasSomeSimilarity =
          c -> {
            long score = stringSimilarity(iDigBioRecord.getCollection(), c.getName());
            score += stringSimilarity(iDigBioRecord.getPhysicalAddress().getCity(), c.getName());
            score += stringSimilarity(iDigBioRecord.getMailingAddress().getCity(), c.getName());
            score += countIdentifierMatches(iDigBioRecord, collections);
            return score > 0;
          };

      matches =
          collections.stream()
              .filter(c -> iDigBioCodes.contains(c.getCode()))
              .filter(hasSomeSimilarity)
              .collect(Collectors.toList());
    }

    if (matches.size() > 1) {
      log.warn("Idigbio record {} matches with more than 1 collection: {}", iDigBioRecord, matches);
      // we count multiple matches as no match since it's not clear which one to take and we prefer
      // to be cautious and duplicate collections
      return Optional.empty();
    }

    return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
  }

  public static Optional<Person> matchContact(
      IDigBioRecord record, Set<Person> grSciCollPersons, Set<Person> contacts) {
    if (grSciCollPersons == null) {
      return Optional.empty();
    }

    StaffNormalized idigbioContact = StaffNormalized.fromIDigBioContact(record);
    Person bestMatch = null;
    int maxScore = 0;
    for (Person person : grSciCollPersons) {
      StaffNormalized personNormalized = StaffNormalized.fromGrSciCollPerson(person);

      boolean emailMatch = compareLists(personNormalized.getEmails(), idigbioContact.getEmails());
      boolean nameMatch =
          compareStrings(personNormalized.getFullName(), idigbioContact.getFullName());
      boolean positionMatch =
          Objects.equals(personNormalized.getPosition(), idigbioContact.getPosition());
      boolean isContact = Utils.isPersonInContacts(person.getKey(), contacts);
      boolean primaryInstMatch =
          Objects.equals(
              idigbioContact.getPrimaryInstitutionKey(),
              personNormalized.getPrimaryInstitutionKey());

      if (!emailMatch || !nameMatch) {
        continue;
      }

      int score = 0;
      if (positionMatch) {
        score += 2;
      } else if (person.getPosition() == null) {
        score += 1;
      } else {
        // the position has to match
        continue;
      }

      score += isContact ? 2 : 0;
      score += primaryInstMatch ? 1 : 0;

      if (score > maxScore) {
        maxScore = score;
        bestMatch = person;
      } else if (score == maxScore) {
        bestMatch = comparePersonFieldCompleteness(bestMatch, person);
      }
    }

    return Optional.ofNullable(bestMatch);
  }

  private static Person comparePersonFieldCompleteness(Person p1, Person p2) {
    long count1 = countNonNullValues(Person.class, p1);
    long count2 = countNonNullValues(Person.class, p2);

    return count1 > count2 ? p1 : p2;
  }

  @VisibleForTesting
  static long stringSimilarity(String n1, String n2) {
    long common1 =
        Arrays.stream(n1.split(" ")).filter(v -> v.length() > 3).filter(n2::contains).count();
    long common2 =
        Arrays.stream(n2.split(" ")).filter(v -> v.length() > 3).filter(n1::contains).count();

    return common1 + common2;
  }

  @VisibleForTesting
  static long countIdentifierMatches(IDigBioRecord iDigBioRecord, Set<Collection> collections) {
    if (Strings.isNullOrEmpty(iDigBioRecord.getCollectionLsid())) {
      return 0;
    }

    return collections.stream()
        .filter(
            c ->
                c.getIdentifiers().stream()
                    .anyMatch(
                        i ->
                            i.getIdentifier().contains(iDigBioRecord.getCollectionLsid())
                                || iDigBioRecord.getCollectionLsid().contains(i.getIdentifier())))
        .count();
  }
}
