package org.gbif.collections.sync.staff;

import org.gbif.api.model.collections.Person;
import org.gbif.api.vocabulary.Country;
import org.gbif.collections.sync.idigbio.IDigBioRecord;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.parsers.CountryParser;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;

import static org.gbif.collections.sync.parsers.DataParser.hasValue;
import static org.gbif.collections.sync.parsers.DataParser.normalizeString;
import static org.gbif.collections.sync.parsers.DataParser.parseStringList;
import static org.gbif.collections.sync.staff.StaffUtils.concatIHFirstName;
import static org.gbif.collections.sync.staff.StaffUtils.concatIHName;
import static org.gbif.collections.sync.staff.StaffUtils.concatPersonName;

/** Normalized representation of a staff to be able to compare staff between different systems. */
@Data
@Builder
public class StaffNormalized {
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

  public static StaffNormalized fromIHStaff(
      IHStaff ihStaff,
      UUID institutionMatched,
      UUID collectionMatched,
      CountryParser countryParser) {

    StaffNormalized.StaffNormalizedBuilder ihBuilder =
        StaffNormalized.builder()
            .fullName(concatIHName(ihStaff))
            .firstName(concatIHFirstName(ihStaff))
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

  public static StaffNormalized fromGrSciCollPerson(Person person) {
    StaffNormalized.StaffNormalizedBuilder personBuilder =
        StaffNormalized.builder()
            .fullName(concatPersonName(person))
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

  public static StaffNormalized fromIDigBioContact(IDigBioRecord record) {
    StaffNormalizedBuilder builder = StaffNormalized.builder();

    if (hasValue(record.getContact())) {
      builder.fullName(normalizeString(record.getContact().toLowerCase()));
    }

    if (hasValue(record.getContactEmail())) {
      builder.emails(Collections.singletonList(normalizeString(record.getContactEmail())));
    }

    if (hasValue(record.getContactEmail())) {
      builder.position(normalizeString(record.getContactRole()));
    }

    if (record.getGrbioInstMatch() != null) {
      builder.primaryInstitutionKey(record.getGrbioInstMatch());
    }

    if (record.getGrbioCollMatch() != null) {
      builder.primaryCollectionKey(record.getGrbioCollMatch());
    }

    return builder.build();
  }
}
