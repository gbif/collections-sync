package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.*;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.clients.http.IHHttpClient;
import org.gbif.collections.sync.common.converter.EntityConverter;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.common.parsers.DataParser;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.common.CloneUtils.cloneCollection;
import static org.gbif.collections.sync.common.CloneUtils.cloneContact;
import static org.gbif.collections.sync.common.CloneUtils.cloneInstitution;
import static org.gbif.collections.sync.common.CloneUtils.clonePerson;
import static org.gbif.collections.sync.common.Utils.*;
import static org.gbif.collections.sync.common.parsers.DataParser.*;
import static org.gbif.collections.sync.ih.model.IHInstitution.CollectionSummary;
import static org.gbif.collections.sync.ih.model.IHInstitution.Location;

/** Converts IH insitutions to the GrSciColl entities {@link Institution} and {@link Collection}. */
@Slf4j
public class IHEntityConverter implements EntityConverter<IHInstitution, IHStaff> {

  public static final String DEFAULT_COLLECTION_NAME = "Herbarium";

  private final CountryParser countryParser;

  private IHEntityConverter(CountryParser countryParser) {
    this.countryParser = countryParser;
  }

  public static IHEntityConverter create(IHConfig config) {
    return new IHEntityConverter(
        CountryParser.from(IHHttpClient.getInstance(config.getIhWsUrl()).getCountries()));
  }

  public static IHEntityConverter create(CountryParser countryParser) {
    return new IHEntityConverter(countryParser);
  }

  @Override
  public Institution convertToInstitution(IHInstitution ihInstitution) {
    return convertToInstitution(ihInstitution, null);
  }

  @Override
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

    addIrnIfNotExists(institution, ihInstitution.getIrn());

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

  @Override
  public Collection convertToCollection(IHInstitution ihInstitution, Collection existing) {
    return convertToCollection(ihInstitution, existing, null);
  }

  @Override
  public Collection convertToCollection(IHInstitution ihInstitution, Institution institution) {
    return convertToCollection(ihInstitution, null, institution);
  }

  @Override
  public Collection convertToCollection(
      IHInstitution ihInstitution, Collection existing, Institution institution) {
    Collection collection = cloneCollection(existing);

    if (institution != null && institution.getKey() != null) {
      collection.setInstitutionKey(institution.getKey());
    }

    // we don't overwrite the name
    if (collection.getName() == null) {
      collection.setName(DEFAULT_COLLECTION_NAME);
    }

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

    addIrnIfNotExists(collection, ihInstitution.getIrn());

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

  @Override
  public Person convertToPerson(IHStaff ihStaff) {
    return convertToPerson(ihStaff, null);
  }

  @Override
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

    addIrnIfNotExists(person, ihStaff.getIrn());

    return person;
  }

  @Override
  public Contact convertToContact(IHStaff ihStaff) {
    return convertToContact(ihStaff, new Contact());
  }

  @Override
  public Contact convertToContact(IHStaff ihStaff, Contact existing) {
    Contact contact = cloneContact(existing);

    buildFirstName(ihStaff).ifPresent(contact::setFirstName);
    contact.setLastName(getStringValue(ihStaff.getLastName()));
    contact.setPosition(getStringValueAsList(ihStaff.getPosition()));

    if (ihStaff.getContact() != null) {
      setListValue(
          ihStaff.getContact().getEmail(),
          DataParser::isValidEmail,
          contact::setEmail,
          "Invalid email of IH Staff " + ihStaff.getIrn());
      setListValue(
          ihStaff.getContact().getPhone(),
          DataParser::isValidPhone,
          contact::setPhone,
          "Invalid phone of IH Staff " + ihStaff.getIrn());
      setListValue(
          ihStaff.getContact().getFax(),
          DataParser::isValidFax,
          contact::setFax,
          "Invalid fax of IH Staff " + ihStaff.getIrn());
    } else {
      contact.setEmail(null);
      contact.setPhone(null);
      contact.setFax(null);
    }

    if (ihStaff.getAddress() != null) {
      getStringValueOpt(ihStaff.getAddress().getStreet())
          .map(Collections::singletonList)
          .ifPresent(contact::setAddress);
      getStringValueOpt(ihStaff.getAddress().getCity()).ifPresent(contact::setCity);
      getStringValueOpt(ihStaff.getAddress().getState()).ifPresent(contact::setProvince);
      getStringValueOpt(ihStaff.getAddress().getZipCode()).ifPresent(contact::setPostalCode);

      Country addressCountry = null;
      if (!Strings.isNullOrEmpty(ihStaff.getAddress().getCountry())) {
        addressCountry = countryParser.parse(ihStaff.getAddress().getCountry());
        if (addressCountry == null) {
          log.warn(
              "Country not found for {} and IH staff {}",
              ihStaff.getAddress().getCountry(),
              ihStaff.getIrn());
        }
      }
      contact.setCountry(addressCountry);
    }

    if (contact.getUserIds().stream().noneMatch(userId -> userId.getType() == IdType.IH_IRN)) {
      contact.setUserIds(Collections.singletonList(new UserId(IdType.IH_IRN, ihStaff.getIrn())));
    }

    return contact;
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
    if (!Strings.isNullOrEmpty(ih.getAddress().getPhysicalCountry())) {
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

  private static <T extends CollectionEntity & Identifiable & MachineTaggable>
      void addIrnIfNotExists(T entity, String irn) {
    if (!containsIrnIdentifier(entity)) {
      // add identifier
      entity.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, encodeIRN(irn)));
    }

    if (!containsIrnMachineTag(entity)) {
      entity.getMachineTags().add(new MachineTag(IH_NAMESPACE, IRN_TAG, irn));
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

  private static void setListValue(
      String value, Predicate<String> validator, Consumer<List<String>> setter, String errorMsg) {
    List<String> listValue = parseStringList(value);

    if (!listValue.isEmpty()) {
      List<String> validValues = new ArrayList<>();
      for (String val : listValue) {
        if (validator.test(val)) {
          validValues.add(val);
        } else {
          log.warn("{}: {}", errorMsg, val);
        }
      }
      setter.accept(validValues);
      return;
    }

    setter.accept(Collections.emptyList());
  }
}
