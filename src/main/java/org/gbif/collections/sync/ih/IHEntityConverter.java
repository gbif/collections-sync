package org.gbif.collections.sync.ih;

import static org.gbif.collections.sync.common.CloneUtils.cloneCollection;
import static org.gbif.collections.sync.common.CloneUtils.cloneContact;
import static org.gbif.collections.sync.common.CloneUtils.cloneInstitution;
import static org.gbif.collections.sync.common.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.common.Utils.encodeIRN;
import static org.gbif.collections.sync.common.parsers.DataParser.TO_BIGDECIMAL;
import static org.gbif.collections.sync.common.parsers.DataParser.cleanString;
import static org.gbif.collections.sync.common.parsers.DataParser.getFirstString;
import static org.gbif.collections.sync.common.parsers.DataParser.getListValue;
import static org.gbif.collections.sync.common.parsers.DataParser.getStringValue;
import static org.gbif.collections.sync.common.parsers.DataParser.getStringValueAsList;
import static org.gbif.collections.sync.common.parsers.DataParser.getStringValueOpt;
import static org.gbif.collections.sync.common.parsers.DataParser.parseDateYear;
import static org.gbif.collections.sync.common.parsers.DataParser.parseStringList;
import static org.gbif.collections.sync.common.parsers.DataParser.parseUri;
import static org.gbif.collections.sync.ih.model.IHInstitution.CollectionSummary;
import static org.gbif.collections.sync.ih.model.IHInstitution.Location;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.MasterSourceMetadata;
import org.gbif.api.model.collections.UserId;
import org.gbif.api.model.collections.descriptors.DescriptorGroup;
import org.gbif.api.model.common.export.ExportFormat;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.api.util.IdentifierUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.collections.IdType;
import org.gbif.api.vocabulary.collections.Source;
import org.gbif.collections.sync.clients.http.IHHttpClient;
import org.gbif.collections.sync.common.Utils;
import org.gbif.collections.sync.common.converter.ConvertedCollection;
import org.gbif.collections.sync.common.converter.EntityConverter;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.common.parsers.DataParser;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.model.IHEntity;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

/** Converts IH insitutions to the GrSciColl entities {@link Institution} and {@link Collection}. */
@Slf4j
public class IHEntityConverter implements EntityConverter<IHInstitution, IHStaff> {

  public static final String DEFAULT_COLLECTION_NAME_FORMAT = "Herbarium - %s";

  private static final String INVALID_EMAIL_MSG = "`%s` is an invalid email.";
  private static final String INVALID_PHONE_MSG = "`%s` is an invalid phone.";
  private static final String INVALID_FAX_MSG = "`%s` is an invalid fax.";
  private static final String INVALID_COUNTRY_MSG =
      "The country `%s` couldn't be found in the IH countries API.";

  private final CountryParser countryParser;
  private final IHIssueNotifier issueNotifier;

  private IHEntityConverter(CountryParser countryParser, IHIssueNotifier issueNotifier) {
    this.countryParser = countryParser;
    this.issueNotifier = issueNotifier;
  }

  public static IHEntityConverter create(IHConfig config) {
    return new IHEntityConverter(
        CountryParser.from(IHHttpClient.getInstance(config.getIhWsUrl()).getCountries()),
        IHIssueNotifier.getInstance(config));
  }

  public static IHEntityConverter create(
      CountryParser countryParser, IHIssueNotifier issueNotifier) {
    return new IHEntityConverter(countryParser, issueNotifier);
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
    institution.setActive(isActive(ihInstitution.getCurrentStatus()));

    setLocation(ihInstitution, institution);
    setAddress(institution, ihInstitution);
    institution.setEmail(getIhEmails(ihInstitution));
    institution.setPhone(getIhPhones(ihInstitution));
    institution.setHomepage(getIhHomepage(ihInstitution));
    institution.setFoundingDate(
        parseDateYear(
            ihInstitution.getDateFounded(),
            () ->
                notifyIssue(
                    "Invalid founding date for institution " + ihInstitution.getIrn(),
                    "`" + ihInstitution.getDateFounded() + "` is an invalid founding date",
                    ihInstitution)));

    addIrnIfNotExists(institution, ihInstitution.getIrn());
    addCitesIfNotExists(institution, ihInstitution.getCites());

    return institution;
  }

  private static boolean isActive(String status) {
    return "Active".equalsIgnoreCase(status);
  }

  void setLocation(IHInstitution ihInstitution, Institution institution) {
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
        notifyIssue(
            "Invalid latitude for institution " + ihInstitution.getIrn(),
            "`" + lat + "` is outside the valid range for a latitude coordinate.",
            ihInstitution);
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
        notifyIssue(
            "Invalid longitude for institution " + ihInstitution.getIrn(),
            "`" + lon + "` is outside the valid range for a longitude coordinate.",
            ihInstitution);
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
  public ConvertedCollection convertToCollection(IHInstitution ihInstitution, Collection existing) {
    return convertToCollection(ihInstitution, existing, null);
  }

  @Override
  public ConvertedCollection convertToCollection(
      IHInstitution ihInstitution, Institution institution) {
    return convertToCollection(ihInstitution, null, institution);
  }

  @Override
  public ConvertedCollection convertToCollection(
      IHInstitution ihInstitution, Collection existing, Institution institution) {
    Collection collection = cloneCollection(existing);

    if (institution != null && institution.getKey() != null) {
      collection.setInstitutionKey(institution.getKey());
    }

    // we don't overwrite the name
    if (collection.getName() == null) {
      collection.setName(String.format(DEFAULT_COLLECTION_NAME_FORMAT, institution.getName()));
    }

    collection.setCode(cleanString(ihInstitution.getCode()));
    collection.setActive(isActive(ihInstitution.getCurrentStatus()));
    collection.setTaxonomicCoverage(getStringValue(ihInstitution.getTaxonomicCoverage()));
    collection.setGeographicCoverage(getStringValue(ihInstitution.getGeography()));
    collection.setNotes(getStringValue(ihInstitution.getNotes()));
    collection.setNumberSpecimens(ihInstitution.getSpecimenTotal());
    collection.setIncorporatedCollections(getListValue(ihInstitution.getIncorporatedHerbaria()));
    collection.setDivision(ihInstitution.getDivision());
    collection.setDepartment(ihInstitution.getDepartment());

    setAddress(collection, ihInstitution);
    collection.setEmail(getIhEmails(ihInstitution));
    collection.setPhone(getIhPhones(ihInstitution));
    collection.setHomepage(getIhHomepage(ihInstitution));

    addIrnIfNotExists(collection, ihInstitution.getIrn());

    ConvertedCollection convertedCollection =
        ConvertedCollection.builder().collection(collection).build();
    createCollectionSummaryDescriptorGroup(
        ihInstitution.getCollectionsSummary(), convertedCollection);
    createImportantCollectorsDescriptorGroup(
        getListValue(ihInstitution.getImportantCollectors()), convertedCollection);

    return convertedCollection;
  }

  @SneakyThrows
  private static void createCollectionSummaryDescriptorGroup(
      CollectionSummary collectionSummary, ConvertedCollection convertedCollection) {
    if (collectionSummary == null || collectionSummary.isEmpty()) {
      return;
    }

    DescriptorGroup descriptorGroup = new DescriptorGroup();
    descriptorGroup.setTitle("IH collection summary");
    descriptorGroup.setDescription("Collection summary imported from IH as a descriptor group.");
    convertedCollection.setCollectionSummary(descriptorGroup);

    Path summaryPath =
        Files.createTempFile("collection_summary", "." + ExportFormat.CSV.name().toLowerCase());
    convertedCollection.setCollectionSummaryFile(summaryPath);

    try (CSVWriter writer = new CSVWriter(new FileWriter(summaryPath.toFile()))) {
      String[] headers = {
        "ltc:objectClassificationName",
        "dwc:scientificName",
        "dwc:individualCount",
        "Num. Databased",
        "Num. Imaged"
      };

      writer.writeNext(headers);

      if (collectionSummary.getNumAlgae() > 0) {
        String[] algae = {
          "Algae",
          null,
          String.valueOf(collectionSummary.getNumAlgae()),
          String.valueOf(collectionSummary.getNumAlgaeDatabased()),
          String.valueOf(collectionSummary.getNumAlgaeImaged())
        };
        writer.writeNext(algae);
      }

      if (collectionSummary.getNumBryos() > 0) {
        String[] bryos = {
          "Bryophytes",
          "Bryophyta",
          String.valueOf(collectionSummary.getNumBryos()),
          String.valueOf(collectionSummary.getNumBryosDatabased()),
          String.valueOf(collectionSummary.getNumBryosImaged())
        };
        writer.writeNext(bryos);
      }

      if (collectionSummary.getNumFungi() > 0) {
        String[] fungi = {
          "Fungi/Lichens",
          "Fungi",
          String.valueOf(collectionSummary.getNumFungi()),
          String.valueOf(collectionSummary.getNumFungiDatabased()),
          String.valueOf(collectionSummary.getNumFungiImaged())
        };
        writer.writeNext(fungi);
      }

      if (collectionSummary.getNumPteridos() > 0) {
        String[] pteridos = {
          "Pteridophytes",
          "Pteridophyta",
          String.valueOf(collectionSummary.getNumPteridos()),
          String.valueOf(collectionSummary.getNumPteridosDatabased()),
          String.valueOf(collectionSummary.getNumPteridosImaged())
        };
        writer.writeNext(pteridos);
      }

      if (collectionSummary.getNumSeedPl() > 0) {
        String[] seedPl = {
          "Seed Plants",
          null,
          String.valueOf(collectionSummary.getNumSeedPl()),
          String.valueOf(collectionSummary.getNumSeedPlDatabased()),
          String.valueOf(collectionSummary.getNumSeedPlImaged())
        };
        writer.writeNext(seedPl);
      }
    }
  }

  @SneakyThrows
  private static void createImportantCollectorsDescriptorGroup(
      List<String> importantCollectors, ConvertedCollection convertedCollection) {
    if (importantCollectors == null || importantCollectors.isEmpty()) {
      return;
    }

    DescriptorGroup descriptorGroup = new DescriptorGroup();
    descriptorGroup.setTitle("IH important collectors");
    descriptorGroup.setDescription("Important collectors imported from IH as a descriptor group.");
    convertedCollection.setImportantCollectors(descriptorGroup);

    Path collectorsPath =
        Files.createTempFile("important_collectors", "." + ExportFormat.CSV.name().toLowerCase());
    convertedCollection.setImportantCollectorsFile(collectorsPath);

    try (CSVWriter writer = new CSVWriter(new FileWriter(collectorsPath.toFile()))) {
      String[] headers = {"dwc:recordedBy"};
      writer.writeNext(headers);
      importantCollectors.forEach(c -> writer.writeNext(new String[] {c}));
    }
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
          v ->
              notifyIssue(
                  "Invalid email of IH Staff " + ihStaff.getIrn(),
                  String.format(INVALID_EMAIL_MSG, v),
                  ihStaff));
      setListValue(
          ihStaff.getContact().getPhone(),
          DataParser::isValidPhone,
          contact::setPhone,
          v ->
              notifyIssue(
                  "Invalid phone of IH Staff " + ihStaff.getIrn(),
                  String.format(INVALID_PHONE_MSG, v),
                  ihStaff));
      setListValue(
          ihStaff.getContact().getFax(),
          DataParser::isValidFax,
          contact::setFax,
          v ->
              notifyIssue(
                  "Invalid fax of IH Staff " + ihStaff.getIrn(),
                  String.format(INVALID_FAX_MSG, v),
                  ihStaff));
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
          notifyIssue(
              "Invalid address country in staff " + ihStaff.getIrn(),
              String.format(INVALID_COUNTRY_MSG, ihStaff.getAddress().getCountry()),
              ihStaff);
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
        notifyIssue(
            "Invalid physical address country in institution " + ih.getIrn(),
            String.format(INVALID_COUNTRY_MSG, ih.getAddress().getPhysicalCountry()),
            ih);
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
        notifyIssue(
            "Invalid postal address country in institution " + ih.getIrn(),
            String.format(INVALID_COUNTRY_MSG, ih.getAddress().getPostalCountry()),
            ih);
        log.warn(
            "Country not found for {} and IH institution {}",
            ih.getAddress().getPostalCountry(),
            ih.getIrn());
      }
    }
    contactable.getMailingAddress().setCountry(mailingAddressCountry);
  }

  @VisibleForTesting
  List<String> getIhEmails(IHInstitution ih) {
    if (ih.getContact() != null && ih.getContact().getEmail() != null) {
      List<String> emails = new ArrayList<>();
      for (String parsedEmail : parseStringList(ih.getContact().getEmail())) {
        if (DataParser.isValidEmail(parsedEmail)) {
          emails.add(parsedEmail);
        } else {
          notifyIssue(
              "Invalid email for institution " + ih.getIrn(),
              String.format(INVALID_EMAIL_MSG, parsedEmail),
              ih);
        }
      }

      return emails;
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  List<String> getIhPhones(IHInstitution ih) {
    if (ih.getContact() != null && ih.getContact().getPhone() != null) {
      List<String> phones = new ArrayList<>();
      for (String parsedPhone : parseStringList(ih.getContact().getPhone())) {
        if (DataParser.isValidPhone(parsedPhone)) {
          phones.add(parsedPhone);
        } else {
          notifyIssue(
              "Invalid phone for institution " + ih.getIrn(),
              String.format(INVALID_PHONE_MSG, parsedPhone),
              ih);
        }
      }
      return phones;
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  URI getIhHomepage(IHInstitution ih) {
    if (ih.getContact() == null || ih.getContact().getWebUrl() == null) {
      return null;
    }
    // when there are multiple URLs we try to get the first one
    Optional<String> webUrlOpt = getFirstString(ih.getContact().getWebUrl());

    return webUrlOpt
        .flatMap(
            v ->
                parseUri(
                    v,
                    ex ->
                        notifyIssue(
                            "Invalid homepage URL for institution " + ih.getIrn(),
                            "`" + v + "` is an invalid URL.",
                            ih)))
        .orElse(null);
  }

  private static <T extends CollectionEntity & Identifiable & MachineTaggable>
      void addIrnIfNotExists(T entity, String irn) {
    if (!containsIrnIdentifier(entity)) {
      // add identifier
      entity.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, encodeIRN(irn)));
    }

    if (entity.getMasterSourceMetadata() == null) {
      entity.setMasterSourceMetadata(new MasterSourceMetadata(Source.IH_IRN, irn));
    }
  }

  private static void addCitesIfNotExists(Institution institution, String cites) {
    if (cites != null && !cites.isEmpty() && IdentifierUtils.isValidCitesIdentifier(cites)) {
      Identifier identifier = new Identifier(IdentifierType.CITES, cites);
      if (!Utils.containsIdentifier(institution, identifier)) {
        institution.getIdentifiers().add(identifier);
      }
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
      String value,
      Predicate<String> validator,
      Consumer<List<String>> setter,
      Consumer<String> errorHandler) {
    List<String> listValue = parseStringList(value);

    if (!listValue.isEmpty()) {
      List<String> validValues = new ArrayList<>();
      for (String val : listValue) {
        if (validator.test(val)) {
          validValues.add(val);
        } else {
          log.warn("{}: {}", "Invalid value", val);
          errorHandler.accept(val);
        }
      }
      setter.accept(validValues);
      return;
    }

    setter.accept(Collections.emptyList());
  }

  private void notifyIssue(String title, String description, IHEntity ihEntity) {
    // done this way so the tests can pass the issueNotifier as null
    if (issueNotifier != null) {
      issueNotifier.createFailedIssueNotification(title, description, ihEntity);
    }
  }
}
