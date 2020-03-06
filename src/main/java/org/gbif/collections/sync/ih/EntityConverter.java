package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.*;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.ih.parsers.CountryParser;
import org.gbif.collections.sync.ih.parsers.IHParser;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;

import static org.gbif.collections.sync.ih.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.ih.model.IHInstitution.CollectionSummary;
import static org.gbif.collections.sync.ih.model.IHInstitution.Location;
import static org.gbif.collections.sync.ih.parsers.IHParser.*;

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
    Institution institution = new Institution();

    if (existing != null) {
      try {
        BeanUtils.copyProperties(institution, existing);
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn("Couldn't copy institution properties from bean: {}", existing);
      }
    }

    getStringValue(ihInstitution.getOrganization()).ifPresent(institution::setName);
    institution.setCode(ihInstitution.getCode());
    institution.setIndexHerbariorumRecord(true);
    institution.setActive(isActive(ihInstitution.getCurrentStatus()));

    setLocation(ihInstitution, institution);
    setAddress(institution, ihInstitution);
    getIhEmails(ihInstitution).ifPresent(institution::setEmail);
    getIhPhones(ihInstitution).ifPresent(institution::setPhone);
    getIhHomepage(ihInstitution).ifPresent(institution::setHomepage);
    parseDate(ihInstitution.getDateFounded()).ifPresent(institution::setFoundingDate);

    addIdentifierIfNotExists(institution, Utils.encodeIRN(ihInstitution.getIrn()), creationUser);

    return institution;
  }

  private static boolean isActive(String status) {
    return "Active".equalsIgnoreCase(status);
  }

  private static void setLocation(IHInstitution ihInstitution, Institution institution) {
    if (ihInstitution.getLocation() != null) {
      Location location = ihInstitution.getLocation();
      if (location.getLat() != null) {
        BigDecimal lat = BigDecimal.valueOf(location.getLat());
        if (lat.compareTo(BigDecimal.valueOf(-90)) >= 0
            && lat.compareTo(BigDecimal.valueOf(90)) <= 0) {
          institution.setLatitude(lat);
        } else {
          log.warn(
              "Invalid lat coordinate {} for institution with IRN {}",
              location.getLat(),
              ihInstitution.getIrn());
        }
      }

      if (location.getLon() != null) {
        BigDecimal lon = BigDecimal.valueOf(location.getLon());
        if (lon.compareTo(BigDecimal.valueOf(-180)) >= 0
            && lon.compareTo(BigDecimal.valueOf(180)) <= 0) {
          institution.setLongitude(lon);
        } else {
          log.warn(
              "Invalid lon coordinate {} for institution with IRN {}",
              location.getLon(),
              ihInstitution.getIrn());
        }
      }
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
    Collection collection = new Collection();

    if (existing != null) {
      try {
        BeanUtils.copyProperties(collection, existing);
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn("Couldn't copy collection properties from bean: {}", existing);
      }
    }

    if (institutionKey != null) {
      collection.setInstitutionKey(institutionKey);
    }

    getStringValue(ihInstitution.getOrganization()).ifPresent(collection::setName);
    collection.setCode(ihInstitution.getCode());
    collection.setIndexHerbariorumRecord(true);
    collection.setActive(isActive(ihInstitution.getCurrentStatus()));
    getStringValue(ihInstitution.getTaxonomicCoverage())
        .ifPresent(collection::setTaxonomicCoverage);
    getStringValue(ihInstitution.getGeography()).ifPresent(collection::setGeography);
    getStringValue(ihInstitution.getNotes()).ifPresent(collection::setNotes);
    collection.setNumberSpecimens(ihInstitution.getSpecimenTotal());
    collection.setCollectionSummary(getCollectionSummary(ihInstitution.getCollectionsSummary()));

    if (ihInstitution.getIncorporatedHerbaria() != null) {
      collection.setIncorporatedCollections(ihInstitution.getIncorporatedHerbaria());
    }
    if (ihInstitution.getImportantCollectors() != null) {
      collection.setImportantCollectors(ihInstitution.getImportantCollectors());
    }

    setAddress(collection, ihInstitution);
    getIhEmails(ihInstitution).ifPresent(collection::setEmail);
    getIhPhones(ihInstitution).ifPresent(collection::setPhone);
    getIhHomepage(ihInstitution).ifPresent(collection::setHomepage);

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
    Person person = new Person();

    if (existing != null) {
      try {
        BeanUtils.copyProperties(person, existing);
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn("Couldn't copy person properties from bean: {}", existing);
      }
    }

    buildFirstName(ihStaff).ifPresent(person::setFirstName);
    getStringValue(ihStaff.getLastName()).ifPresent(person::setLastName);
    getStringValue(ihStaff.getPosition()).ifPresent(person::setPosition);

    if (ihStaff.getContact() != null) {
      getFirstString(ihStaff.getContact().getEmail())
          .filter(IHParser::isValidEmail)
          .ifPresent(person::setEmail);
      getFirstString(ihStaff.getContact().getPhone())
          .filter(IHParser::isValidPhone)
          .ifPresent(person::setPhone);
      getFirstString(ihStaff.getContact().getFax())
          .filter(IHParser::isValidFax)
          .ifPresent(person::setFax);
    }

    if (ihStaff.getAddress() != null) {
      Address mailingAddress = new Address();
      getStringValue(ihStaff.getAddress().getStreet()).ifPresent(mailingAddress::setAddress);
      getStringValue(ihStaff.getAddress().getCity()).ifPresent(mailingAddress::setCity);
      getStringValue(ihStaff.getAddress().getState()).ifPresent(mailingAddress::setProvince);
      getStringValue(ihStaff.getAddress().getZipCode()).ifPresent(mailingAddress::setPostalCode);

      if (!Strings.isNullOrEmpty(ihStaff.getAddress().getCountry())) {
        Country mailingAddressCountry = countryParser.parse(ihStaff.getAddress().getCountry());
        mailingAddress.setCountry(mailingAddressCountry);
        if (mailingAddressCountry == null) {
          log.warn(
              "Country not found for {} and IH staff {}",
              ihStaff.getAddress().getCountry(),
              ihStaff.getIrn());
        }
      }

      person.setMailingAddress(mailingAddress);
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
    if (Strings.isNullOrEmpty(firstName)) {
      return Optional.empty();
    }

    return Optional.of(firstName.trim());
  }

  private void setAddress(Contactable contactable, IHInstitution ih) {
    Address physicalAddress = null;
    Address mailingAddress = null;
    if (ih.getAddress() != null) {
      physicalAddress = new Address();
      getStringValue(ih.getAddress().getPhysicalStreet()).ifPresent(physicalAddress::setAddress);
      getStringValue(ih.getAddress().getPhysicalCity()).ifPresent(physicalAddress::setCity);
      getStringValue(ih.getAddress().getPhysicalState()).ifPresent(physicalAddress::setProvince);
      getStringValue(ih.getAddress().getPhysicalZipCode())
          .ifPresent(physicalAddress::setPostalCode);

      if (!Strings.isNullOrEmpty(ih.getAddress().getPhysicalCountry())) {
        Country physicalAddressCountry = countryParser.parse(ih.getAddress().getPhysicalCountry());
        physicalAddress.setCountry(physicalAddressCountry);
        if (physicalAddressCountry == null) {
          log.warn(
              "Country not found for {} and IH institution {}",
              ih.getAddress().getPhysicalCountry(),
              ih.getIrn());
        }
      }

      mailingAddress = new Address();
      getStringValue(ih.getAddress().getPostalStreet()).ifPresent(mailingAddress::setAddress);
      getStringValue(ih.getAddress().getPostalCity()).ifPresent(mailingAddress::setCity);
      getStringValue(ih.getAddress().getPostalState()).ifPresent(mailingAddress::setProvince);
      getStringValue(ih.getAddress().getPostalZipCode()).ifPresent(mailingAddress::setPostalCode);

      if (!Strings.isNullOrEmpty(ih.getAddress().getPostalCountry())) {
        Country mailingAddressCountry = countryParser.parse(ih.getAddress().getPostalCountry());
        mailingAddress.setCountry(mailingAddressCountry);
        if (mailingAddressCountry == null) {
          log.warn(
              "Country not found for {} and IH institution {}",
              ih.getAddress().getPostalCountry(),
              ih.getIrn());
        }
      }
    }
    contactable.setAddress(physicalAddress);
    contactable.setMailingAddress(mailingAddress);
  }

  private static Optional<List<String>> getIhEmails(IHInstitution ih) {
    if (ih.getContact() != null && ih.getContact().getEmail() != null) {
      return Optional.of(
          parseStringList(ih.getContact().getEmail()).stream()
              .filter(IHParser::isValidEmail)
              .collect(Collectors.toList()));
    }
    return Optional.empty();
  }

  private static Optional<List<String>> getIhPhones(IHInstitution ih) {
    if (ih.getContact() != null && ih.getContact().getPhone() != null) {
      return Optional.of(
          parseStringList(ih.getContact().getPhone()).stream()
              .filter(IHParser::isValidPhone)
              .collect(Collectors.toList()));
    }
    return Optional.empty();
  }

  @VisibleForTesting
  static Optional<URI> getIhHomepage(IHInstitution ih) {
    if (ih.getContact() == null || ih.getContact().getWebUrl() == null) {
      return Optional.empty();
    }
    // when there are multiple URLs we try to get the first one
    Optional<String> webUrlOpt = getFirstString(ih.getContact().getWebUrl());

    if (!webUrlOpt.isPresent()) {
      return Optional.empty();
    }

    return parseUri(webUrlOpt.get());
  }

  private static void addIdentifierIfNotExists(Identifiable entity, String irn, String user) {
    if (!containsIrnIdentifier(entity)) {
      // add identifier
      Identifier ihIdentifier = new Identifier(IdentifierType.IH_IRN, irn);
      ihIdentifier.setCreatedBy(user);
      entity.getIdentifiers().add(ihIdentifier);
    }
  }

  private static Optional<String> getStringValue(String value) {
    return isValidString(value) ? Optional.of(value) : Optional.empty();
  }
}
