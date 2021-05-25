package org.gbif.collections.sync.ih.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.common.staff.StaffNormalized;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.*;
import java.util.function.BiFunction;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;

import static org.gbif.collections.sync.common.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.common.Utils.encodeIRN;
import static org.gbif.collections.sync.common.Utils.isPersonInContacts;
import static org.gbif.collections.sync.common.staff.StaffUtils.compareFullNamePartially;
import static org.gbif.collections.sync.common.staff.StaffUtils.compareLists;
import static org.gbif.collections.sync.common.staff.StaffUtils.compareStrings;
import static org.gbif.collections.sync.common.staff.StaffUtils.compareStringsPartially;

/** Matches IH entities to GrSciColl ones. */
public class Matcher {

  private final IHProxyClient proxyClient;
  private final CountryParser countryParser;

  private Matcher(IHProxyClient proxyClient) {
    this.countryParser = CountryParser.from(proxyClient.getCountries());
    this.proxyClient = proxyClient;
  }

  public static Matcher create(IHProxyClient proxyClient) {
    return new Matcher(proxyClient);
  }

  public IHMatchResult match(IHInstitution ihInstitution) {
    // find matches
    Set<Institution> institutionsMatched =
        proxyClient
            .getInstitutionsMapByIrn()
            .getOrDefault(ihInstitution.getIrn(), Collections.emptySet());
    Set<Collection> collectionsMatched =
        proxyClient
            .getCollectionsMapByIrn()
            .getOrDefault(ihInstitution.getIrn(), Collections.emptySet());

    return IHMatchResult.builder()
        .ihInstitution(ihInstitution)
        .ihStaff(
            proxyClient
                .getIhStaffMapByCode()
                .getOrDefault(ihInstitution.getCode(), Collections.emptyList()))
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

      // first try with IRNs
      Set<Person> matchesWithIrn =
          proxyClient
              .getGrSciCollPersonsByIrn()
              .getOrDefault(encodeIRN(ihStaffToMatch.ihStaff.getIrn()), Collections.emptySet());

      if (!matchesWithIrn.isEmpty()) {
        return matchesWithIrn;
      }

      // if couldn't find any we try to match by comparing the fields of each person
      return matchWithFields(ihStaffToMatch, proxyClient.getAllGrSciCollPersons(), contacts, 14);
    };
  }

  @VisibleForTesting
  Set<Person> matchWithFields(
      IHStaffToMatch ihStaffToMatch, Set<Person> persons, Set<Person> contacts, int minimumScore) {
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
      // calculate matching score
      int equalityScore =
          getEqualityScore(ihStaffNorm, personNorm, isPersonInContacts(person.getKey(), contacts));
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

  /**
   * It calculates the matching score between 2 staff normalized. The fields have different weights
   * based on their importance to contribute to a better match. Matching at least the email or the
   * name is required. Also, contacts of the entity have an extra score since they are more likely
   * to be a good match.
   */
  @VisibleForTesting
  static int getEqualityScore(StaffNormalized staff1, StaffNormalized staff2, boolean isContact) {
    int emailScore = 0;
    if (compareLists(staff1.getEmails(), staff2.getEmails())) {
      emailScore = 10;
    }

    int nameScore = 0;
    if (compareStrings(staff1.getFullName(), staff2.getFullName())) {
      nameScore = 10;
    } else if (compareStringsPartially(staff1.getFullName(), staff2.getFullName())) {
      nameScore = 5;
    } else if (compareFullNamePartially(staff1.getFullName(), staff2.getFullName())) {
      nameScore = 3;
    }

    // case when 2 persons have the same corporate email but different name
    if (emailScore > 0
        && staff1.getFullName() != null
        && staff2.getFullName() != null
        && nameScore == 0) {
      return 0;
    }

    int score = emailScore + nameScore;
    // a minimum match of the email or the name are required
    if (score < 5) {
      return score;
    }

    if (compareLists(staff1.getPhones(), staff2.getPhones())) {
      score += 3;
    }
    if (staff2.getCountry() != null && staff1.getCountry() == staff2.getCountry()) {
      score += 3;
    }
    if (compareStrings(staff1.getCity(), staff2.getCity())) {
      score += 2;
    }
    if (compareStrings(staff1.getPosition(), staff2.getPosition())) {
      score += 2;
    } else if (compareStringsPartially(staff1.getPosition(), staff2.getPosition())) {
      score += 1;
    }
    if (staff1.getPrimaryInstitutionKey() != null
        && Objects.equals(staff1.getPrimaryInstitutionKey(), staff2.getPrimaryInstitutionKey())) {
      score += 2;
    }
    if (staff1.getPrimaryCollectionKey() != null
        && Objects.equals(staff1.getPrimaryCollectionKey(), staff2.getPrimaryCollectionKey())) {
      score += 2;
    }
    if (compareStrings(staff1.getFax(), staff2.getFax())) {
      score += 1;
    }
    if (compareStrings(staff1.getStreet(), staff2.getStreet())) {
      score += 1;
    }
    if (compareStrings(staff1.getState(), staff2.getState())) {
      score += 1;
    }
    if (compareStrings(staff1.getZipCode(), staff2.getZipCode())) {
      score += 1;
    }
    if (isContact) {
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
