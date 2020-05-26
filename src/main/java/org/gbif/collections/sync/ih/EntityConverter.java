package org.gbif.collections.sync.ih;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.Utils;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.parsers.CountryParser;
import org.gbif.collections.sync.parsers.DataParser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.Utils.cloneCollection;
import static org.gbif.collections.sync.Utils.cloneInstitution;
import static org.gbif.collections.sync.Utils.clonePerson;
import static org.gbif.collections.sync.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.ih.model.IHInstitution.CollectionSummary;
import static org.gbif.collections.sync.ih.model.IHInstitution.Location;
import static org.gbif.collections.sync.parsers.DataParser.TO_BIGDECIMAL;
import static org.gbif.collections.sync.parsers.DataParser.cleanString;
import static org.gbif.collections.sync.parsers.DataParser.getFirstString;
import static org.gbif.collections.sync.parsers.DataParser.getListValue;
import static org.gbif.collections.sync.parsers.DataParser.getStringValue;
import static org.gbif.collections.sync.parsers.DataParser.normalizeString;
import static org.gbif.collections.sync.parsers.DataParser.parseDate;
import static org.gbif.collections.sync.parsers.DataParser.parseStringList;
import static org.gbif.collections.sync.parsers.DataParser.parseUri;

/** Converts IH insitutions to the GrSciColl entities {@link Institution} and {@link Collection}. */
@Slf4j
public class EntityConverter {

  private final CountryParser countryParser;
  private final String creationUser;

  @Builder
  private EntityConverter(CountryParser countryParser, String creationUser) {
    this.countryParser = countryParser;
    this.creationUser = creationUser;
  }

  public Institution convertToInstitution(IHInstitution ihInstitution) {
    return convertToInstitution(ihInstitution, null);
  }

  public Institution convertToInstitution(IHInstitution ihInstitution, Institution existing) {
    Institution institution = cloneInstitution(existing);

    institution.setName(cleanString(ihInstitution.getOrganization()));
    institution.setCode(cleanString(ihInstitution.getCode()));
    institution.setIndexHerbariorumRecord(true);
    institution.setActive(isActive(ihInstitution.getCurrentStatus()));

    setLocation(ihInstitution, institution);
    setAddress(institution, ihInstitution);
    institution.setEmail(getIhEmails(ihInstitution));
    institution.setPhone(getIhPhones(ihInstitution));
    institution.setHomepage(getIhHomepage(ihInstitution));
    institution.setFoundingDate(
        parseDate(
            ihInstitution.getDateFounded(),
            "Invalid date for institution " + ihInstitution.getIrn()));

    addIdentifierIfNotExists(institution, Utils.encodeIRN(ihInstitution.getIrn()), creationUser);

    return institution;
  }

  private static boolean isActive(String status) {
    return "Active".equalsIgnoreCase(status);
  }

  static void setLocation(IHInstitution ihInstitution, Institution institution) {
    if (ihInstitution.getLocation() == null
        || (Objects.equals(ihInstitution.getLocation().getLat(), 0d)
            && Objects.equals(ihInstitution.getLocation().getLon(), 0d))) {
      // we usually receive both coordinates as 0 when they are actually null
      institution.setLatitude(null);
      institution.setLongitude(null);
      return;
    }

    Location location = ihInstitution.getLocation();
    if (location.getLat() != null) {
      BigDecimal lat = TO_BIGDECIMAL.apply(location.getLat());
      if (lat.compareTo(BigDecimal.valueOf(-90)) >= 0
          && lat.compareTo(BigDecimal.valueOf(90)) <= 0) {
        institution.setLatitude(lat);
      } else {
        log.warn(
            "Invalid lat coordinate {} for institution with IRN {}",
            location.getLat(),
            ihInstitution.getIrn());
      }
    } else {
      institution.setLatitude(null);
    }

    if (location.getLon() != null) {
      BigDecimal lon = TO_BIGDECIMAL.apply(location.getLon());
      if (lon.compareTo(BigDecimal.valueOf(-180)) >= 0
          && lon.compareTo(BigDecimal.valueOf(180)) <= 0) {
        institution.setLongitude(lon);
      } else {
        log.warn(
            "Invalid lon coordinate {} for institution with IRN {}",
            location.getLon(),
            ihInstitution.getIrn());
      }
    } else {
      institution.setLongitude(null);
    }
  }

  public Collection convertToCollection(IHInstitution ihInstitution, Collection existing) {
    return convertToCollection(ihInstitution, existing, null);
  }

  public Collection convertToCollection(IHInstitution ihInstitution) {
    return convertToCollection(ihInstitution, null, null);
  }

  public Collection convertToCollection(IHInstitution ihInstitution, UUID institutionKey) {
    return convertToCollection(ihInstitution, null, institutionKey);
  }

  public Collection convertToCollection(
      IHInstitution ihInstitution, Collection existing, UUID institutionKey) {
    Collection collection = cloneCollection(existing);

    if (institutionKey != null) {
      collection.setInstitutionKey(institutionKey);
    }

    collection.setName(cleanString(ihInstitution.getOrganization()));
    collection.setCode(cleanString(ihInstitution.getCode()));
    collection.setIndexHerbariorumRecord(true);
    collection.setActive(isActive(ihInstitution.getCurrentStatus()));
    collection.setTaxonomicCoverage(getStringValue(ihInstitution.getTaxonomicCoverage()));
    collection.setGeography(getStringValue(ihInstitution.getGeography()));
    collection.setNotes(getStringValue(ihInstitution.getNotes()));
    collection.setNumberSpecimens(ihInstitution.getSpecimenTotal());
    collection.setCollectionSummary(getCollectionSummary(ihInstitution.getCollectionsSummary()));
    collection.setIncorporatedCollections(getListValue(ihInstitution.getIncorporatedHerbaria()));
    collection.setImportantCollectors(getListValue(ihInstitution.getImportantCollectors()));

    setAddress(collection, ihInstitution);
    collection.setEmail(getIhEmails(ihInstitution));
    collection.setPhone(getIhPhones(ihInstitution));
    collection.setHomepage(getIhHomepage(ihInstitution));

    addIdentifierIfNotExists(collection, Utils.encodeIRN(ihInstitution.getIrn()), creationUser);

    return collection;
  }

  private static Map<String, Integer> getCollectionSummary(CollectionSummary collectionSummary) {
    if (collectionSummary != null) {
      return Arrays.stream(CollectionSummary.class.getDeclaredFields())
          .filter(f -> f.getType().isAssignableFrom(int.class))
          .collect(
              Collectors.toMap(
                  Field::getName,
                  f -> {
                    try {
                      return (int)
                          CollectionSummary.class
                              .getMethod(
                                  "get"
                                      + f.getName().substring(0, 1).toUpperCase()
                                      + f.getName().substring(1))
                              .invoke(collectionSummary);
                    } catch (Exception e) {
                      log.warn(
                          "Couldn't parse field {} in collectionSummary: {}",
                          f,
                          collectionSummary,
                          e);
                      return 0;
                    }
                  }));
    }

    return Collections.emptyMap();
  }

  public Person convertToPerson(IHStaff ihStaff) {
    return convertToPerson(ihStaff, null);
  }

  public Person convertToPerson(IHStaff ihStaff, Person existing) {
    Person person = clonePerson(existing);

    buildFirstName(ihStaff).ifPresent(person::setFirstName);
    person.setLastName(getStringValue(ihStaff.getLastName()));
    person.setPosition(getStringValue(ihStaff.getPosition()));

    if (ihStaff.getContact() != null) {
      setFirstValue(
          ihStaff.getContact().getEmail(),
          DataParser::isValidEmail,
          person::setEmail,
          "Invalid email of IH Staff " + ihStaff.getIrn());
      setFirstValue(
          ihStaff.getContact().getPhone(),
          DataParser::isValidPhone,
          person::setPhone,
          "Invalid phone of IH Staff " + ihStaff.getIrn());
      setFirstValue(
          ihStaff.getContact().getFax(),
          DataParser::isValidFax,
          person::setFax,
          "Invalid fax of IH Staff " + ihStaff.getIrn());
    } else {
      person.setEmail(null);
      person.setPhone(null);
      person.setFax(null);
    }

    if (ihStaff.getAddress() != null) {
      if (person.getMailingAddress() == null) {
        person.setMailingAddress(new Address());
      }

      person.getMailingAddress().setAddress(getStringValue(ihStaff.getAddress().getStreet()));
      person.getMailingAddress().setCity(getStringValue(ihStaff.getAddress().getCity()));
      person.getMailingAddress().setProvince(getStringValue(ihStaff.getAddress().getState()));
      person.getMailingAddress().setPostalCode(getStringValue(ihStaff.getAddress().getZipCode()));

      Country mailingAddressCountry = null;
      if (!Strings.isNullOrEmpty(ihStaff.getAddress().getCountry())) {
        mailingAddressCountry = countryParser.parse(ihStaff.getAddress().getCountry());
        if (mailingAddressCountry == null) {
          log.warn(
              "Country not found for {} and IH staff {}",
              ihStaff.getAddress().getCountry(),
              ihStaff.getIrn());
        }
      }
      person.getMailingAddress().setCountry(mailingAddressCountry);
    }

    addIdentifierIfNotExists(person, Utils.encodeIRN(ihStaff.getIrn()), creationUser);

    return person;
  }

  private Optional<String> buildFirstName(IHStaff ihStaff) {
    StringBuilder firstNameBuilder = new StringBuilder();
    if (!Strings.isNullOrEmpty(ihStaff.getFirstName())) {
      firstNameBuilder.append(ihStaff.getFirstName()).append(" ");
    }
    if (!Strings.isNullOrEmpty(ihStaff.getMiddleName())) {
      firstNameBuilder.append(ihStaff.getMiddleName());
    }

    String firstName = firstNameBuilder.toString();
    if (org.gbif.common.shaded.com.google.common.base.Strings.isNullOrEmpty(firstName)) {
      return Optional.empty();
    }

    return Optional.of(firstName.trim());
  }

  @VisibleForTesting
  void setAddress(Contactable contactable, IHInstitution ih) {
    if (ih.getAddress() == null) {
      contactable.setAddress(null);
      contactable.setMailingAddress(null);
      return;
    }

    if (contactable.getAddress() == null) {
      contactable.setAddress(new Address());
    }

    contactable.getAddress().setAddress(getStringValue(ih.getAddress().getPhysicalStreet()));
    contactable.getAddress().setCity(getStringValue(ih.getAddress().getPhysicalCity()));
    contactable.getAddress().setProvince(getStringValue(ih.getAddress().getPhysicalState()));
    contactable.getAddress().setPostalCode(getStringValue(ih.getAddress().getPhysicalZipCode()));

    Country physicalAddressCountry = null;
    if (!org.gbif.common.shaded.com.google.common.base.Strings.isNullOrEmpty(
        ih.getAddress().getPhysicalCountry())) {
      physicalAddressCountry = countryParser.parse(ih.getAddress().getPhysicalCountry());
      if (physicalAddressCountry == null) {
        log.warn(
            "Country not found for {} and IH institution {}",
            ih.getAddress().getPhysicalCountry(),
            ih.getIrn());
      }
    }
    contactable.getAddress().setCountry(physicalAddressCountry);

    if (contactable.getMailingAddress() == null) {
      contactable.setMailingAddress(new Address());
    }

    contactable.getMailingAddress().setAddress(getStringValue(ih.getAddress().getPostalStreet()));
    contactable.getMailingAddress().setCity(getStringValue(ih.getAddress().getPostalCity()));
    contactable.getMailingAddress().setProvince(getStringValue(ih.getAddress().getPostalState()));
    contactable
        .getMailingAddress()
        .setPostalCode(getStringValue(ih.getAddress().getPostalZipCode()));

    Country mailingAddressCountry = null;
    if (!Strings.isNullOrEmpty(ih.getAddress().getPostalCountry())) {
      mailingAddressCountry = countryParser.parse(ih.getAddress().getPostalCountry());
      if (mailingAddressCountry == null) {
        log.warn(
            "Country not found for {} and IH institution {}",
            ih.getAddress().getPostalCountry(),
            ih.getIrn());
      }
    }
    contactable.getMailingAddress().setCountry(mailingAddressCountry);
  }

  @VisibleForTesting
  static List<String> getIhEmails(IHInstitution ih) {
    if (ih.getContact() != null && ih.getContact().getEmail() != null) {
      return parseStringList(ih.getContact().getEmail()).stream()
          .filter(DataParser::isValidEmail)
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  static List<String> getIhPhones(IHInstitution ih) {
    if (ih.getContact() != null && ih.getContact().getPhone() != null) {
      return parseStringList(ih.getContact().getPhone()).stream()
          .filter(DataParser::isValidPhone)
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  static URI getIhHomepage(IHInstitution ih) {
    if (ih.getContact() == null || ih.getContact().getWebUrl() == null) {
      return null;
    }
    // when there are multiple URLs we try to get the first one
    Optional<String> webUrlOpt = getFirstString(ih.getContact().getWebUrl());

    return webUrlOpt
        .flatMap(v -> parseUri(v, "Invalid URL for institution " + ih.getIrn()))
        .orElse(null);
  }

  private static void addIdentifierIfNotExists(Identifiable entity, String irn, String user) {
    if (!containsIrnIdentifier(entity)) {
      // add identifier
      Identifier ihIdentifier = new Identifier(IdentifierType.IH_IRN, irn);
      ihIdentifier.setCreatedBy(user);
      entity.getIdentifiers().add(ihIdentifier);
    }
  }

  private static void setFirstValue(
      String value, Predicate<String> validator, Consumer<String> setter, String errorMsg) {
    Optional<String> first = getFirstString(value);

    if (first.isPresent()) {
      if (validator.test(first.get())) {
        setter.accept(first.get());
        return;
      } else {
        log.warn("{}: {}", errorMsg, value);
      }
    }

    setter.accept(null);
  }
}
