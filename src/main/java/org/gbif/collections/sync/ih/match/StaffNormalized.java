package org.gbif.collections.sync.ih.match;

import org.gbif.api.model.collections.Person;
import org.gbif.api.vocabulary.Country;
import org.gbif.collections.sync.ih.parsers.CountryParser;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Data;

import static org.gbif.collections.sync.ih.parsers.IHParser.normalizeString;
import static org.gbif.collections.sync.ih.parsers.IHParser.parseStringList;

/** Contains all the common field between IH staff and GrSciColl persons. */
@Data
@Builder
class StaffNormalized {
  String fullName;
  String firstName;
  String lastName;
  List<String> emails;
  List<String> phones;
  String fax;
  String position;
  String street;
  String city;
  String state;
  String zipCode;
  Country country;
  UUID primaryInstitutionKey;
  UUID primaryCollectionKey;

  static StaffNormalized fromIHStaff(
      IHStaff ihStaff,
      UUID institutionMatched,
      UUID collectionMatched,
      CountryParser countryParser) {

    Function<IHStaff, String> concatIHFirstName =
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

          return normalizeString(firstName);
        };

    Function<IHStaff, String> concatIHName =
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

          return normalizeString(fullName);
        };

    StaffNormalized.StaffNormalizedBuilder ihBuilder =
        StaffNormalized.builder()
            .fullName(concatIHName.apply(ihStaff))
            .firstName(concatIHFirstName.apply(ihStaff))
            .lastName(normalizeString(ihStaff.getLastName()))
            .position(normalizeString(ihStaff.getPosition()))
            .primaryInstitutionKey(institutionMatched)
            .primaryCollectionKey(collectionMatched);

    if (ihStaff.getContact() != null) {
      ihBuilder
          .emails(parseStringList(ihStaff.getContact().getEmail()))
          .phones(parseStringList(ihStaff.getContact().getPhone()))
          .fax(normalizeString(ihStaff.getContact().getFax()));
    }

    if (ihStaff.getAddress() != null) {
      ihBuilder
          .street(normalizeString(ihStaff.getAddress().getStreet()))
          .city(normalizeString(ihStaff.getAddress().getCity()))
          .state(normalizeString(ihStaff.getAddress().getState()))
          .zipCode(normalizeString(ihStaff.getAddress().getZipCode()))
          .country(countryParser.parse(ihStaff.getAddress().getCountry()));
    }

    return ihBuilder.build();
  }

  static StaffNormalized fromGrSciCollPerson(Person person) {
    Function<Person, String> concatPersonName =
        p -> {
          StringBuilder fullNameBuilder = new StringBuilder();
          if (!Strings.isNullOrEmpty(p.getFirstName())) {
            // persons in our registry usually have the full name in this field and can contain
            // multiple whitespaces, so we need to normalize them
            fullNameBuilder.append(p.getFirstName());
            fullNameBuilder.append(" ");
          }
          if (!Strings.isNullOrEmpty(p.getLastName())) {
            fullNameBuilder.append(p.getLastName().trim());
          }

          String fullName = fullNameBuilder.toString();
          if (Strings.isNullOrEmpty(fullName)) {
            return null;
          }

          return normalizeString(fullName);
        };

    StaffNormalized.StaffNormalizedBuilder personBuilder =
        StaffNormalized.builder()
            .fullName(concatPersonName.apply(person))
            .firstName(normalizeString(person.getFirstName()))
            .lastName(normalizeString(person.getLastName()))
            .position(normalizeString(person.getPosition()))
            .emails(parseStringList(person.getEmail()))
            .phones(parseStringList(person.getPhone()))
            .fax(normalizeString(person.getFax()))
            .primaryInstitutionKey(person.getPrimaryInstitutionKey())
            .primaryCollectionKey(person.getPrimaryCollectionKey());

    if (person.getMailingAddress() != null) {
      personBuilder
          .street(normalizeString(person.getMailingAddress().getAddress()))
          .city(normalizeString(person.getMailingAddress().getCity()))
          .state(normalizeString(person.getMailingAddress().getProvince()))
          .zipCode(normalizeString(person.getMailingAddress().getPostalCode()))
          .country(person.getMailingAddress().getCountry());
    }

    return personBuilder.build();
  }
}
