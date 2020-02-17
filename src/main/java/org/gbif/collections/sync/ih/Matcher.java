package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.*;

import static org.gbif.collections.sync.ih.Utils.encodeIRN;

public class Matcher {

  private final EntityConverter entityConverter;
  private final Map<String, Set<Institution>> institutionsMapByIrn;
  private final Map<String, Set<Collection>> collectionsMapByIrn;
  private final Map<String, Set<Person>> grSciCollPersonsByIrn;
  private final Set<Person> allGrSciCollPersons;
  private final Map<String, List<IHStaff>> ihStaffMapByCode;

  @Builder
  private Matcher(
      EntityConverter entityConverter,
      List<Institution> institutions,
      List<Collection> collections,
      Set<Person> allGrSciCollPersons,
      List<IHStaff> ihStaff) {
    this.entityConverter = entityConverter;
    institutionsMapByIrn = mapByIrn(institutions);
    collectionsMapByIrn = mapByIrn(collections);
    grSciCollPersonsByIrn = mapByIrn(allGrSciCollPersons);
    this.allGrSciCollPersons = allGrSciCollPersons;
    ihStaffMapByCode =
        ihStaff.stream()
            .collect(Collectors.groupingBy(IHStaff::getCode, HashMap::new, Collectors.toList()));
  }

  private static final Function<IHStaff, String> CONCAT_IH_NAME =
      s -> {
        StringBuilder fullNameBuilder = new StringBuilder();
        if (!Strings.isNullOrEmpty(s.getFirstName())) {
          fullNameBuilder.append(s.getFirstName().trim());
          fullNameBuilder.append(" ");
        }
        if (!Strings.isNullOrEmpty(s.getMiddleName())) {
          fullNameBuilder.append(s.getMiddleName().trim());
          fullNameBuilder.append(" ");
        }
        if (!Strings.isNullOrEmpty(s.getLastName())) {
          fullNameBuilder.append(s.getLastName().trim());
        }

        String fullName = fullNameBuilder.toString();
        if (Strings.isNullOrEmpty(fullName)) {
          return null;
        }

        return fullName.trim();
      };

  private static final Function<IHStaff, String> CONCAT_IH_FIRST_NAME =
      s -> {
        StringBuilder firstNameBuilder = new StringBuilder();
        if (!Strings.isNullOrEmpty(s.getFirstName())) {
          firstNameBuilder.append(s.getFirstName().trim());
          firstNameBuilder.append(" ");
        }
        if (!Strings.isNullOrEmpty(s.getMiddleName())) {
          firstNameBuilder.append(s.getMiddleName().trim());
        }

        String firstName = firstNameBuilder.toString();
        if (Strings.isNullOrEmpty(firstName)) {
          return null;
        }

        return firstName.trim();
      };

  private static final Function<Person, String> CONCAT_PERSON_NAME =
      p -> {
        StringBuilder fullNameBuilder = new StringBuilder();
        if (!Strings.isNullOrEmpty(p.getFirstName())) {
          fullNameBuilder.append(p.getFirstName().trim());
          fullNameBuilder.append(" ");
        }
        if (!Strings.isNullOrEmpty(p.getLastName())) {
          fullNameBuilder.append(p.getLastName().trim());
        }

        String fullName = fullNameBuilder.toString();
        if (Strings.isNullOrEmpty(fullName)) {
          return null;
        }

        return fullName.trim();
      };

  public Match match(IHInstitution ihInstitution) {
    String irn = encodeIRN(ihInstitution.getIrn());
    return Match.builder()
        .ihInstitution(ihInstitution)
        .ihStaff(ihStaffMapByCode.getOrDefault(ihInstitution.getCode(), Collections.emptyList()))
        .institutions(institutionsMapByIrn.getOrDefault(irn, Collections.emptySet()))
        .collections(collectionsMapByIrn.getOrDefault(irn, Collections.emptySet()))
        .staffMatcher(this::matchStaff)
        .build();
  }

  private Set<Person> matchStaff(IHStaff ihStaff, Set<Person> contacts) {
    // try to find a match in the GrSciColl contacts
    Set<Person> matches = matchWithContacts(ihStaff, contacts);

    if (matches.isEmpty()) {
      // no match among the contacts. We check now in all the GrSciColl persons.
      matches = matchGlobally(ihStaff);
    }
    return matches;
  }

  private Set<Person> matchGlobally(IHStaff ihStaff) {
    // first try with IRNs
    Set<Person> matchesWithIrn =
        grSciCollPersonsByIrn.getOrDefault(encodeIRN(ihStaff.getIrn()), Collections.emptySet());

    if (!matchesWithIrn.isEmpty()) {
      return matchesWithIrn;
    }

    // we try to match with fields
    return matchWithFields(ihStaff, allGrSciCollPersons, 11);
  }

  private Set<Person> matchWithContacts(IHStaff ihStaff, Set<Person> grSciCollPersons) {
    if (grSciCollPersons == null || grSciCollPersons.isEmpty()) {
      return Collections.emptySet();
    }

    // try to find a match by using the IRN identifiers
    String irn = encodeIRN(ihStaff.getIrn());
    Set<Person> irnMatches =
        grSciCollPersons.stream()
            .filter(
                p ->
                    p.getIdentifiers().stream()
                        .anyMatch(i -> Objects.equals(irn, i.getIdentifier())))
            .collect(Collectors.toSet());

    if (!irnMatches.isEmpty()) {
      return irnMatches;
    }

    // no irn matches, we try to match with the fields
    return matchWithFields(ihStaff, grSciCollPersons, 9);
  }

  @VisibleForTesting
  Set<Person> matchWithFields(IHStaff ihStaff, Set<Person> persons, int minimumScore) {
    if (persons.isEmpty()) {
      return Collections.emptySet();
    }

    StaffNormalized ihStaffNorm = buildIHStaffNormalized(ihStaff, entityConverter);

    int maxScore = 0;
    Set<Person> bestMatches = new HashSet<>();
    for (Person person : persons) {
      StaffNormalized personNorm = buildGrSciCollPersonNormalized(person);
      int equalityScore = getEqualityScore(ihStaffNorm, personNorm);

      if (equalityScore < minimumScore) {
        continue;
      }

      if (equalityScore > maxScore) {
        bestMatches.clear();
        bestMatches.add(person);
        maxScore = equalityScore;
      } else if (equalityScore > 0 && equalityScore == maxScore) {
        bestMatches.add(person);
      }
    }

    return bestMatches;
  }

  private static StaffNormalized buildIHStaffNormalized(
      IHStaff ihStaff, EntityConverter entityConverter) {
    StaffNormalized.StaffNormalizedBuilder ihBuilder =
        StaffNormalized.builder()
            .fullName(CONCAT_IH_NAME.apply(ihStaff))
            .firstName(CONCAT_IH_FIRST_NAME.apply(ihStaff))
            .lastName(ihStaff.getLastName())
            .position(ihStaff.getPosition());

    if (ihStaff.getContact() != null) {
      ihBuilder
          .email(ihStaff.getContact().getEmail())
          .phone(ihStaff.getContact().getPhone())
          .fax(ihStaff.getContact().getFax());
    }

    if (ihStaff.getAddress() != null) {
      ihBuilder
          .street(ihStaff.getAddress().getStreet())
          .city(ihStaff.getAddress().getCity())
          .state(ihStaff.getAddress().getState())
          .zipCode(ihStaff.getAddress().getZipCode())
          .country(entityConverter.convertCountry(ihStaff.getAddress().getCountry()));
    }

    return ihBuilder.build();
  }

  private static StaffNormalized buildGrSciCollPersonNormalized(Person person) {
    StaffNormalized.StaffNormalizedBuilder personBuilder =
        StaffNormalized.builder()
            .fullName(CONCAT_PERSON_NAME.apply(person))
            .firstName(person.getFirstName())
            .lastName(person.getLastName())
            .position(person.getPosition())
            .email(person.getEmail())
            .phone(person.getPhone())
            .fax(person.getFax());

    if (person.getMailingAddress() != null) {
      personBuilder
          .street(person.getMailingAddress().getAddress())
          .city(person.getMailingAddress().getCity())
          .state(person.getMailingAddress().getProvince())
          .zipCode(person.getMailingAddress().getPostalCode())
          .country(person.getMailingAddress().getCountry());
    }

    return personBuilder.build();
  }

  private static int getEqualityScore(StaffNormalized staff1, StaffNormalized staff2) {
    BiPredicate<String, String> compareStrings =
        (s1, s2) -> {
          if (!Strings.isNullOrEmpty(s1) && !Strings.isNullOrEmpty(s2)) {
            return s1.equalsIgnoreCase(s2);
          }
          return false;
        };

    BiPredicate<String, String> compareStringsPartially =
        (s1, s2) -> {
          if (!Strings.isNullOrEmpty(s1) && !Strings.isNullOrEmpty(s2)) {
            return (s1.startsWith(s2) || s2.startsWith(s1)) || (s1.endsWith(s2) || s2.endsWith(s1));
          }
          return false;
        };

    BiPredicate<String, String> compareFullNamePartially =
        (s1, s2) -> {
          if (!Strings.isNullOrEmpty(s1) && !Strings.isNullOrEmpty(s2)) {
            String[] parts1 = s1.split(" ");
            String[] parts2 = s2.split(" ");

            for (String p1 : parts1) {
              for (String p2 : parts2) {
                if (p1.length() >= 5 && p1.equalsIgnoreCase(p2)) {
                  return true;
                }
              }
            }
          }
          return false;
        };

    int score = 0;
    if (compareStrings.test(staff1.email, staff2.email)) {
      score += 10;
    }

    if (compareStrings.test(staff1.fullName, staff2.fullName)) {
      score += 10;
    } else if (compareStringsPartially.test(staff1.fullName, staff2.fullName)) {
      score += 5;
    } else if (compareFullNamePartially.test(staff1.fullName, staff2.fullName)) {
      score += 4;
    }

    // at least the name or the email should match
    if (score == 0) {
      return score;
    }

    if (compareStrings.test(staff1.phone, staff2.phone)) {
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
    }
    if (compareStringsPartially.test(staff1.position, staff2.position)) {
      score += 1;
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

  /** Contains all the common field between IH staff and GrSciColl persons. */
  @Data
  @Builder
  private static class StaffNormalized {
    private String fullName;
    private String firstName;
    private String lastName;
    private String email;
    private String phone;
    private String fax;
    private String position;
    private String street;
    private String city;
    private String state;
    private String zipCode;
    private Country country;
  }

  @Builder
  public static class Match {
    IHInstitution ihInstitution;

    @Singular(value = "ihStaff")
    List<IHStaff> ihStaff;

    @Singular(value = "institution")
    Set<Institution> institutions;

    @Singular(value = "collection")
    Set<Collection> collections;

    BiFunction<IHStaff, Set<Person>, Set<Person>> staffMatcher;

    boolean onlyOneInstitutionMatch() {
      return institutions.size() == 1 && collections.isEmpty();
    }

    boolean onlyOneCollectionMatch() {
      return collections.size() == 1 && institutions.isEmpty();
    }

    boolean noMatches() {
      return institutions.isEmpty() && collections.isEmpty();
    }

    boolean institutionAndCollectionMatch() {
      if (institutions.size() != 1 || collections.size() != 1) {
        return false;
      }

      // check that the collection belongs to the institution
      Institution institution = institutions.iterator().next();
      Collection collection = collections.iterator().next();
      return institution.getKey().equals(collection.getInstitutionKey());
    }

    List<CollectionEntity> getAllMatches() {
      List<CollectionEntity> all = new ArrayList<>();
      all.addAll(institutions);
      all.addAll(collections);
      return all;
    }
  }

  private static <T extends CollectionEntity & Identifiable> Map<String, Set<T>> mapByIrn(
      java.util.Collection<T> entities) {
    Map<String, Set<T>> mapByIrn = new HashMap<>();
    if (entities == null) {
      return mapByIrn;
    }

    entities.forEach(
        o ->
            o.getIdentifiers().stream()
                .filter(i -> i.getType() == IdentifierType.IH_IRN)
                .forEach(
                    i -> mapByIrn.computeIfAbsent(i.getIdentifier(), s -> new HashSet<>()).add(o)));
    return mapByIrn;
  }
}
