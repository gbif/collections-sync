package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.*;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;

import static org.gbif.collections.sync.ih.model.IHInstitution.CollectionSummary;
import static org.gbif.collections.sync.ih.model.IHInstitution.Location;

/** Converts IH insitutions to the GrSciColl entities {@link Institution} and {@link Collection}. */
@Slf4j
public class EntityConverter {

  private static final Pattern WHITESPACE = Pattern.compile("[\\s+]");
  private static final Map<String, Country> COUNTRY_MANUAL_MAPPINGS = new HashMap<>();
  private final Map<String, Country> countryLookup;
  private String creationUser;

  static {
    COUNTRY_MANUAL_MAPPINGS.put("U.K.", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("UK", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("Scotland", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("Alderney", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("England", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("Congo Republic (Congo-Brazzaville)", Country.CONGO);
    COUNTRY_MANUAL_MAPPINGS.put("Republic of Congo-Brazzaville", Country.CONGO);
    COUNTRY_MANUAL_MAPPINGS.put(
        "Democratic Republic of the Congo", Country.CONGO_DEMOCRATIC_REPUBLIC);
    COUNTRY_MANUAL_MAPPINGS.put("Democratic Republic of Congo", Country.CONGO_DEMOCRATIC_REPUBLIC);
    COUNTRY_MANUAL_MAPPINGS.put("Zaire", Country.CONGO_DEMOCRATIC_REPUBLIC);
    COUNTRY_MANUAL_MAPPINGS.put("Italia", Country.ITALY);
    COUNTRY_MANUAL_MAPPINGS.put("Ivory Coast", Country.CÔTE_DIVOIRE);
    COUNTRY_MANUAL_MAPPINGS.put("Laos", Country.LAO);
    COUNTRY_MANUAL_MAPPINGS.put("Republic of Korea", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("Republic of South Korea", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("Korea, South", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("Korea (South)", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("South Korea", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("São Tomé e Príncipe", Country.SAO_TOME_PRINCIPE);
    COUNTRY_MANUAL_MAPPINGS.put(
        "Slovak Republic",
        Country
            .SLOVAKIA); //    COUNTRY_MANUAL_MAPPINGS.put("Brunei Darussalam",
                        // Country.BRUNEI_DARUSSALAM);
    COUNTRY_MANUAL_MAPPINGS.put("México", Country.MEXICO);
    COUNTRY_MANUAL_MAPPINGS.put("French Guiana (France)", Country.FRENCH_GUIANA);
    COUNTRY_MANUAL_MAPPINGS.put("Reunion", Country.RÉUNION);
    COUNTRY_MANUAL_MAPPINGS.put("Palestinian Territories", Country.PALESTINIAN_TERRITORY);
    COUNTRY_MANUAL_MAPPINGS.put("Espanya", Country.SPAIN);
    COUNTRY_MANUAL_MAPPINGS.put("Virgin Islands, U.S.A.", Country.VIRGIN_ISLANDS);
    COUNTRY_MANUAL_MAPPINGS.put("Brasil", Country.BRAZIL);
    COUNTRY_MANUAL_MAPPINGS.put("Türkiye", Country.TURKEY);
    COUNTRY_MANUAL_MAPPINGS.put("Panamá", Country.PANAMA);
  }

  @Builder
  private EntityConverter(List<String> countries, String creationUser) {
    this.creationUser = creationUser;
    countryLookup = mapCountries(countries);

    if (countryLookup.size() < countries.size()) {
      log.warn("We couldn't match all the countries to our enum");
    }
  }

  @VisibleForTesting
  static Map<String, Country> mapCountries(List<String> countries) {
    // build map with the titles of the Country enum
    Map<String, Country> titleLookup =
        Maps.uniqueIndex(Lists.newArrayList(Country.values()), Country::getTitle);

    Map<String, Country> mappings = new HashMap<>();

    countries.forEach(
        c -> {
          Country country = titleLookup.get(c);

          if (country == null) {
            country = Country.fromIsoCode(c);
          }
          if (country == null) {
            country = Country.fromIsoCode(c.replaceAll("\\.", ""));
          }
          if (country == null && c.contains(",")) {
            country = titleLookup.get(c.split(",")[0]);
          }
          if (country == null) {
            country =
                Arrays.stream(Country.values())
                    .filter(v -> c.contains(v.getTitle()))
                    .findFirst()
                    .orElse(null);
          }
          if (country == null) {
            country =
                Arrays.stream(Country.values())
                    .filter(v -> v.getTitle().contains(c))
                    .findFirst()
                    .orElse(null);
          }
          if (country == null) {
            country =
                Arrays.stream(Country.values())
                    .filter(v -> v.name().replaceAll("_", " ").equalsIgnoreCase(c))
                    .findFirst()
                    .orElse(null);
          }

          if (country != null) {
            mappings.put(c.toLowerCase(), country);
          }
        });

    COUNTRY_MANUAL_MAPPINGS.forEach((k, v) -> mappings.put(k.toLowerCase(), v));

    return mappings;
  }

  public Country convertCountry(String country) {
    return countryLookup.get(country.toLowerCase().trim());
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

    // founding date
    try {
      getStringValue(ihInstitution.getDateFounded())
          .map(v -> v.endsWith(".") ? v.substring(0, v.length() - 1) : v)
          .map(IsoDateParsingUtils::parseDate)
          .ifPresent(institution::setFoundingDate);
    } catch (Exception e) {
      log.warn(
          "Couldn't parse founding date {} for institution {}",
          ihInstitution.getDateFounded(),
          ihInstitution.getIrn(),
          e);
    }

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
          log.info(
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
          log.info(
              "Invalid lon coordinate {} for institution with IRN {}",
              location.getLon(),
              ihInstitution.getIrn());
        }
      }
    }
  }

  public Collection convertToCollection(IHInstitution ihInstitution) {
    return convertToCollection(ihInstitution, null);
  }

  public Collection convertToCollection(IHInstitution ihInstitution, Collection existing) {
    Collection collection = new Collection();

    if (existing != null) {
      try {
        BeanUtils.copyProperties(collection, existing);
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn("Couldn't copy collection properties from bean: {}", existing);
      }
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
                      throw new IllegalStateException(e);
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
      getFirstString(ihStaff.getContact().getEmail()).ifPresent(person::setEmail);
      getFirstString(ihStaff.getContact().getPhone()).ifPresent(person::setPhone);
      getFirstString(ihStaff.getContact().getFax()).ifPresent(person::setFax);
    }

    if (ihStaff.getAddress() != null) {
      Address mailingAddress = new Address();
      getStringValue(ihStaff.getAddress().getStreet()).ifPresent(mailingAddress::setAddress);
      getStringValue(ihStaff.getAddress().getCity()).ifPresent(mailingAddress::setCity);
      getStringValue(ihStaff.getAddress().getState()).ifPresent(mailingAddress::setProvince);
      getStringValue(ihStaff.getAddress().getZipCode()).ifPresent(mailingAddress::setPostalCode);

      if (!Strings.isNullOrEmpty(ihStaff.getAddress().getCountry())) {
        Country mailingAddressCountry = convertCountry(ihStaff.getAddress().getCountry());
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
        Country physicalAddressCountry = convertCountry(ih.getAddress().getPhysicalCountry());
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
        Country mailingAddressCountry = convertCountry(ih.getAddress().getPostalCountry());
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
      return Optional.of(parseStringList(ih.getContact().getEmail()));
    }
    return Optional.empty();
  }

  private static Optional<List<String>> getIhPhones(IHInstitution ih) {
    if (ih.getContact() != null && ih.getContact().getPhone() != null) {
      return Optional.of(parseStringList(ih.getContact().getPhone()));
    }
    return Optional.empty();
  }

  private static List<String> parseStringList(String stringList) {
    String listNormalized = stringList.replaceAll("\n", ",");
    return Arrays.asList(listNormalized.split(","));
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

    // we try to clean the URL...
    String webUrl = WHITESPACE.matcher(webUrlOpt.get()).replaceAll("");

    try {
      return Optional.of(URI.create(webUrl));
    } catch (Exception ex) {
      log.warn("Couldn't parse the contact webUrl {} for IH institution {}", webUrl, ih.getIrn());
      return Optional.empty();
    }
  }

  private static Optional<String> getFirstString(String stringList) {
    if (Strings.isNullOrEmpty(stringList)) {
      return Optional.empty();
    }

    String firstValue = null;
    if (stringList.contains(",")) {
      firstValue = stringList.split(",")[0];
    } else if (stringList.contains(";")) {
      firstValue = stringList.split(";")[0];
    } else if (stringList.contains("\n")) {
      firstValue = stringList.split("\n")[0];
    }

    if (Strings.isNullOrEmpty(firstValue)) {
      return Optional.of(stringList);
    }

    return Optional.of(firstValue.trim());
  }

  private static void addIdentifierIfNotExists(Identifiable entity, String irn, String user) {
    if (!containsIrnAsIdentifier(entity, irn)) {
      // add identifier
      Identifier ihIdentifier = new Identifier(IdentifierType.IH_IRN, irn);
      ihIdentifier.setCreatedBy(user);
      entity.getIdentifiers().add(ihIdentifier);
    }
  }

  private static boolean containsIrnAsIdentifier(Identifiable entity, String irn) {
    return entity.getIdentifiers().stream().anyMatch(i -> Objects.equals(irn, i.getIdentifier()));
  }

  private static Optional<String> getStringValue(String value) {
    if (Strings.isNullOrEmpty(value)) {
      return Optional.empty();
    }
    return Optional.of(value);
  }
}
