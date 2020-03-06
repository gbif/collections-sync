package org.gbif.collections.sync.ih.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.ih.parsers.CountryParser;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;

import static org.gbif.collections.sync.ih.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.ih.Utils.encodeIRN;
import static org.gbif.collections.sync.ih.Utils.mapByIrn;

/** Matches IH entities to GrSciColl ones. */
public class Matcher {

  private final CountryParser countryParser;
  private final Map<String, Set<Institution>> institutionsMapByIrn;
  private final Map<String, Set<Collection>> collectionsMapByIrn;
  private final Map<String, Set<Person>> grSciCollPersonsByIrn;
  private final Set<Person> allGrSciCollPersons;
  private final Map<String, List<IHStaff>> ihStaffMapByCode;

  @Builder
  private Matcher(
      CountryParser countryParser,
      List<Institution> institutions,
      List<Collection> collections,
      Set<Person> allGrSciCollPersons,
      List<IHStaff> ihStaff) {
    this.countryParser = countryParser;
    institutionsMapByIrn = mapByIrn(institutions);
    collectionsMapByIrn = mapByIrn(collections);
    grSciCollPersonsByIrn = mapByIrn(allGrSciCollPersons);
    this.allGrSciCollPersons = allGrSciCollPersons;
    ihStaffMapByCode =
        ihStaff.stream()
            .collect(Collectors.groupingBy(IHStaff::getCode, HashMap::new, Collectors.toList()));
  }

  public MatchResult match(IHInstitution ihInstitution) {
    String irn = encodeIRN(ihInstitution.getIrn());

    // find matches
    Set<Institution> institutionsMatched =
        institutionsMapByIrn.getOrDefault(irn, Collections.emptySet());
    Set<Collection> collectionsMatched =
        collectionsMapByIrn.getOrDefault(irn, Collections.emptySet());

    return MatchResult.builder()
        .ihInstitution(ihInstitution)
        .ihStaff(ihStaffMapByCode.getOrDefault(ihInstitution.getCode(), Collections.emptyList()))
        .institutions(institutionsMatched)
        .collections(collectionsMatched)
        .staffMatcher(
            createStaffMatcher(getMatchKey(institutionsMatched), getMatchKey(collectionsMatched)))
        .build();
  }

  private <T extends CollectionEntity> UUID getMatchKey(Set<T> matches) {
    if (matches.iterator().hasNext()) {
      return matches.iterator().next().getKey();
    }
    return null;
  }

  private BiFunction<IHStaff, Set<Person>, Set<Person>> createStaffMatcher(
      UUID institutionKey, UUID collectionKey) {
    return (ihStaff, contacts) -> {
      IHStaffToMatch ihStaffToMatch = new IHStaffToMatch(ihStaff, institutionKey, collectionKey);

      // try to find a match in the GrSciColl contacts
      Set<Person> matches = matchWithContacts(ihStaffToMatch, contacts);

      if (matches.isEmpty()) {
        // no match among the contacts. We check now in all the GrSciColl persons.
        matches = matchGlobally(ihStaffToMatch);
      }
      return matches;
    };
  }

  private Set<Person> matchGlobally(IHStaffToMatch ihStaffToMatch) {
    // first try with IRNs
    Set<Person> matchesWithIrn =
        grSciCollPersonsByIrn.getOrDefault(
            encodeIRN(ihStaffToMatch.ihStaff.getIrn()), Collections.emptySet());

    if (!matchesWithIrn.isEmpty()) {
      return matchesWithIrn;
    }

    // we try to match with fields
    return matchWithFields(ihStaffToMatch, allGrSciCollPersons, 13);
  }

  private Set<Person> matchWithContacts(IHStaffToMatch ihStaffToMatch, Set<Person> contacts) {
    if (contacts == null || contacts.isEmpty()) {
      return Collections.emptySet();
    }

    // try to find a match by using the IRN identifiers
    String irn = encodeIRN(ihStaffToMatch.ihStaff.getIrn());
    Set<Person> irnMatches =
        contacts.stream()
            .filter(
                p ->
                    p.getIdentifiers().stream()
                        .anyMatch(i -> Objects.equals(irn, i.getIdentifier())))
            .collect(Collectors.toSet());

    if (!irnMatches.isEmpty()) {
      return irnMatches;
    }

    // no irn matches, we try to match with the fields
    return matchWithFields(ihStaffToMatch, contacts, 11);
  }

  @VisibleForTesting
  Set<Person> matchWithFields(
      IHStaffToMatch ihStaffToMatch, Set<Person> persons, int minimumScore) {
    if (persons.isEmpty()) {
      return Collections.emptySet();
    }

    StaffNormalized ihStaffNorm =
        StaffNormalized.fromIHStaff(
            ihStaffToMatch.ihStaff,
            ihStaffToMatch.institutionMatched,
            ihStaffToMatch.collectionMatched,
            countryParser);

    int maxScore = 0;
    Set<Person> bestMatches = new HashSet<>();
    for (Person person : persons) {
      if (containsIrnIdentifier(person)) {
        // already matched to a IH staff
        continue;
      }

      StaffNormalized personNorm = StaffNormalized.fromGrSciCollPerson(person);
      int equalityScore = getEqualityScore(ihStaffNorm, personNorm);

      if (equalityScore >= minimumScore && equalityScore > maxScore) {
        bestMatches.clear();
        bestMatches.add(person);
        maxScore = equalityScore;
      } else if (equalityScore > 0 && equalityScore == maxScore) {
        bestMatches.add(person);
      }
    }

    return bestMatches;
  }

  @VisibleForTesting
  static int getEqualityScore(StaffNormalized staff1, StaffNormalized staff2) {
    BiPredicate<List<String>, List<String>> compareLists =
        (l1, l2) -> {
          if (l1 != null && !l1.isEmpty() && l2 != null && !l2.isEmpty()) {
            for (String v1 : l1) {
              for (String v2 : l2) {
                if (v1.equals(v2)) {
                  return true;
                }
              }
            }
          }
          return false;
        };

    BiPredicate<String, String> compareStrings =
        (s1, s2) -> !Strings.isNullOrEmpty(s1) && !Strings.isNullOrEmpty(s2) && s1.equals(s2);

    BiPredicate<String, String> compareStringsPartially =
        (s1, s2) ->
            !Strings.isNullOrEmpty(s1)
                && !Strings.isNullOrEmpty(s2)
                && ((s1.startsWith(s2) || s2.startsWith(s1))
                    || (s1.endsWith(s2) || s2.endsWith(s1)));

    BiPredicate<String, String> compareFullNamePartially =
        (s1, s2) -> {
          if (!Strings.isNullOrEmpty(s1) && !Strings.isNullOrEmpty(s2)) {
            String[] parts1 = s1.split(" ");
            String[] parts2 = s2.split(" ");

            for (String p1 : parts1) {
              for (String p2 : parts2) {
                if (p1.length() >= 5 && p1.equals(p2)) {
                  return true;
                }
              }
            }
          }
          return false;
        };

    int emailScore = 0;
    if (compareLists.test(staff1.emails, staff2.emails)) {
      emailScore = 10;
    }

    int nameScore = 0;
    if (compareStrings.test(staff1.fullName, staff2.fullName)) {
      nameScore = 10;
    } else if (compareStringsPartially.test(staff1.fullName, staff2.fullName)) {
      nameScore = 5;
    } else if (compareFullNamePartially.test(staff1.fullName, staff2.fullName)) {
      nameScore = 3;
    }

    // case when 2 persons have the same corporate email but different name
    if (emailScore > 0 && staff1.fullName != null && staff2.fullName != null && nameScore == 0) {
      return 0;
    }

    int score = emailScore + nameScore;
    // a minimum match of the email or the name are required
    if (score < 5) {
      return score;
    }

    if (compareLists.test(staff1.phones, staff2.phones)) {
      score += 3;
    }
    if (staff2.country != null && staff1.country == staff2.country) {
      score += 3;
    }
    if (compareStrings.test(staff1.city, staff2.city)) {
      score += 2;
    }
    if (compareStrings.test(staff1.position, staff2.position)) {
      score += 2;
    } else if (compareStringsPartially.test(staff1.position, staff2.position)) {
      score += 1;
    }
    if (staff1.primaryInstitutionKey != null
        && Objects.equals(staff1.primaryInstitutionKey, staff2.primaryInstitutionKey)) {
      score += 2;
    }
    if (staff1.primaryCollectionKey != null
        && Objects.equals(staff1.primaryCollectionKey, staff2.primaryCollectionKey)) {
      score += 2;
    }
    if (compareStrings.test(staff1.fax, staff2.fax)) {
      score += 1;
    }
    if (compareStrings.test(staff1.street, staff2.street)) {
      score += 1;
    }
    if (compareStrings.test(staff1.state, staff2.state)) {
      score += 1;
    }
    if (compareStrings.test(staff1.zipCode, staff2.zipCode)) {
      score += 1;
    }

    return score;
  }

  @AllArgsConstructor
  static class IHStaffToMatch {
    IHStaff ihStaff;
    UUID institutionMatched;
    UUID collectionMatched;

    IHStaffToMatch(IHStaff ihStaff) {
      this.ihStaff = ihStaff;
    }
  }
}
