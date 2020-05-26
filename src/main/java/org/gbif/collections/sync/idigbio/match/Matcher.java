package org.gbif.collections.sync.idigbio.match;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifier;
import org.gbif.collections.sync.Utils;
import org.gbif.collections.sync.idigbio.IDigBioRecord;
import org.gbif.collections.sync.staff.StaffNormalized;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.Utils.countNonNullValues;
import static org.gbif.collections.sync.Utils.removeUuidNamespace;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.getIdigbioCodes;
import static org.gbif.collections.sync.staff.StaffUtils.compareLists;
import static org.gbif.collections.sync.staff.StaffUtils.compareStrings;

@Slf4j
public class Matcher {

  private final MatchData matchData;

  @Builder
  private Matcher(MatchData matchData) {
    this.matchData = matchData;
  }

  public MatchResult match(IDigBioRecord iDigBioRecord) {
    MatchResult.MatchResultBuilder result = MatchResult.builder().iDigBioRecord(iDigBioRecord);

    Institution institutionMatch =
        matchData.getInstitutionsByKey().get(iDigBioRecord.getGrbioInstMatch());
    if (institutionMatch == null) {
      institutionMatch = matchWithNewInstitutions(iDigBioRecord);
    }
    result.institutionMatched(institutionMatch);

    if (institutionMatch != null) {
      // we try to find a match among the institution collections
      matchCollection(institutionMatch.getKey(), iDigBioRecord)
          .ifPresent(result::collectionMatched);
    }

    return result.build();
  }

  private Institution matchWithNewInstitutions(IDigBioRecord iDigBioRecord) {
    // we try with the newly created institutions
    List<String> iDigBioCodes = getIdigbioCodes(iDigBioRecord.getInstitutionCode());
    String instUniqueNameUuid = removeUuidNamespace(iDigBioRecord.getUniqueNameUuid());
    Predicate<List<Identifier>> containsIdentifier =
        ids ->
            Strings.isNullOrEmpty(iDigBioRecord.getUniqueNameUuid())
                || ids.stream()
                    .anyMatch(identifier -> identifier.getIdentifier().equals(instUniqueNameUuid));

    List<Institution> institutionsMatched =
        matchData.getNewlyCreatedIDigBioInstitutions().stream()
            .filter(
                i ->
                    iDigBioCodes.contains(i.getCode())
                        && i.getName().equals(iDigBioRecord.getInstitution())
                        && containsIdentifier.test(i.getIdentifiers()))
            .collect(Collectors.toList());

    if (institutionsMatched.size() > 1) {
      log.warn("Multiple candidates for record {}: {}", iDigBioRecord, institutionsMatched);
    } else if (institutionsMatched.size() == 1) {
      return institutionsMatched.get(0);
    }
    return null;
  }

  private Optional<Collection> matchCollection(UUID institutionKey, IDigBioRecord iDigBioRecord) {
    Set<Collection> collections = matchData.getCollectionsByInstitution().get(institutionKey);
    if (collections == null) {
      return Optional.empty();
    }

    List<String> iDigBioCodes = getIdigbioCodes(iDigBioRecord.getCollectionCode());
    List<Collection> matches = null;
    if (!Strings.isNullOrEmpty(iDigBioRecord.getSameAs())
        && iDigBioRecord.getSameAs().contains("irn=")) {

      if (iDigBioCodes.isEmpty()) {
        iDigBioCodes.addAll(getIdigbioCodes(iDigBioRecord.getInstitutionCode()));
      }

      String irn = iDigBioRecord.getSameAs().split("irn=")[1];
      matches =
          collections.stream()
              .filter(c -> countIdentifierMatches(Utils.encodeIRN(irn), c) > 0)
              .filter(c -> iDigBioCodes.isEmpty() || iDigBioCodes.contains(c.getCode()))
              .collect(Collectors.toList());
    } else {
      if (!iDigBioCodes.isEmpty()) {
        return Optional.empty();
      }

      Predicate<Collection> hasSomeSimilarity =
          c -> {
            long score = stringSimilarity(iDigBioRecord.getCollection(), c.getName());
            score += stringSimilarity(iDigBioRecord.getPhysicalAddress().getCity(), c.getName());
            score += stringSimilarity(iDigBioRecord.getMailingAddress().getCity(), c.getName());
            score += countIdentifierMatches(iDigBioRecord.getCollectionLsid(), c);
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

  public Optional<Person> matchContact(IDigBioRecord record, Set<Person> contacts) {
    if (matchData.getPersons() == null) {
      return Optional.empty();
    }

    StaffNormalized idigbioContact = StaffNormalized.fromIDigBioContact(record);
    Person bestMatch = null;
    int maxScore = 0;
    for (Person person : matchData.getPersons()) {
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

      if (!emailMatch && !nameMatch) {
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
  static long countIdentifierMatches(String identifier, Collection collection) {
    if (Strings.isNullOrEmpty(identifier) || collection.getIdentifiers() == null) {
      return 0;
    }

    return collection.getIdentifiers().stream()
        .filter(
            i -> i.getIdentifier().contains(identifier) || identifier.contains(i.getIdentifier()))
        .count();
  }
}
