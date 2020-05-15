package org.gbif.collections.sync.idigbio;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.parsers.DataParser;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.Utils.cloneCollection;
import static org.gbif.collections.sync.Utils.cloneInstitution;
import static org.gbif.collections.sync.Utils.clonePerson;
import static org.gbif.collections.sync.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.Utils.removeUuidNamespace;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.IDIGBIO_NAMESPACE;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.getIdigbioCodes;
import static org.gbif.collections.sync.parsers.DataParser.TO_BIGDECIMAL;
import static org.gbif.collections.sync.parsers.DataParser.TO_LOCAL_DATE_TIME_UTC;
import static org.gbif.collections.sync.parsers.DataParser.cleanString;
import static org.gbif.collections.sync.parsers.DataParser.getStringValue;
import static org.gbif.collections.sync.parsers.DataParser.getStringValueOpt;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class EntityConverter {

  public static Institution convertToInstitution(IDigBioRecord record) {
    return convertToInstitution(null, record);
  }

  public static Institution convertToInstitution(Institution existing, IDigBioRecord record) {
    Institution institution = cloneInstitution(existing);

    getStringValueOpt(record.getUniqueNameUuid())
        .ifPresent(
            v -> {
              addIdentifierIfNotExists(
                  institution, new Identifier(IdentifierType.UUID, removeUuidNamespace(v)));

              addMachineTagIfNotExists(
                  institution, new MachineTag(IDIGBIO_NAMESPACE, "UniqueNameUUID", v));
            });

    List<String> idigbioCodes = getIdigbioCodes(record.getInstitutionCode());
    // non-IH and more updated in iDigBio
    if (!containsIrnIdentifier(institution)
        && shouldUpdateRecord(record, institution.getModified())) {
      // codes
      setCodes(
          idigbioCodes,
          institution.getCode(),
          institution.getAlternativeCodes(),
          institution::setCode);

      if (institution.getCode() == null) {
        // if the code is still null we use the one from the collection
        getStringValueOpt(record.getCollectionCode()).ifPresent(institution::setCode);
      }

      Optional<String> instName = getStringValueOpt(record.getInstitution());
      if (instName.isPresent()) {
        institution.setName(instName.get());
      } else if (institution.getName() == null) {
        getStringValueOpt(record.getCollectionCode()).ifPresent(institution::setName);
      }

      if (record.getLat() != null) {
        institution.setLatitude(TO_BIGDECIMAL.apply(record.getLat()));
      }
      if (record.getLon() != null) {
        institution.setLongitude(TO_BIGDECIMAL.apply(record.getLon()));
      }
    } else {
      idigbioCodes.stream()
          .filter(c -> !c.equalsIgnoreCase(institution.getCode()))
          .forEach(c -> institution.getAlternativeCodes().put(c, "Code migrated from iDigBio"));
    }

    return institution;
  }

  public static Collection convertToCollection(IDigBioRecord record, Institution institution) {
    return convertToCollection(null, record, institution);
  }

  public static Collection convertToCollection(Collection existing, IDigBioRecord record) {
    return convertToCollection(existing, record, null);
  }

  public static Collection convertToCollection(
      Collection existing, IDigBioRecord record, Institution institution) {
    Collection collection = cloneCollection(existing);

    if (institution != null && institution.getKey() != null) {
      collection.setInstitutionKey(institution.getKey());
    }

    // codes
    List<String> idigbioCodes = getIdigbioCodes(record.getCollectionCode());
    if (containsIrnIdentifier(collection)
        || !shouldUpdateRecord(record, collection.getModified())) {
      // if they don't match we keep the IH one and add the other to the alternatives
      idigbioCodes.stream()
          .filter(c -> !c.equalsIgnoreCase(collection.getCode()))
          .forEach(c -> collection.getAlternativeCodes().put(c, "Code migrated from iDigBio"));
    } else {
      if (!idigbioCodes.isEmpty()) {
        setCodes(
            idigbioCodes,
            collection.getCode(),
            collection.getAlternativeCodes(),
            collection::setCode);
      } else if (collection.getCode() == null) {
        // we try to find a code. At this point we only care about the main code
        collection.setCode(
            Optional.ofNullable(getIdigbioCodes(record.getInstitutionCode()))
                .filter(v -> !v.isEmpty())
                .map(v -> v.get(0))
                .orElse(institution != null ? institution.getCode() : null));
      }
    }

    // machine tags and identifiers
    getStringValueOpt(record.getRecordSets())
        .ifPresent(
            v ->
                addMachineTagIfNotExists(
                    collection, new MachineTag(IDIGBIO_NAMESPACE, "recordsets", v)));
    getStringValueOpt(record.getRecordsetQuery())
        .ifPresent(
            v ->
                addMachineTagIfNotExists(
                    collection, new MachineTag(IDIGBIO_NAMESPACE, "recordsetQuery", v)));
    getStringValueOpt(record.getCollectionLsid())
        .ifPresent(
            v -> addIdentifierIfNotExists(collection, new Identifier(IdentifierType.LSID, v)));
    getStringValueOpt(record.getCollectionUuid())
        .ifPresent(
            v -> {
              addIdentifierIfNotExists(
                  collection, new Identifier(IdentifierType.UUID, removeUuidNamespace(v)));
              addMachineTagIfNotExists(
                  collection, new MachineTag(IDIGBIO_NAMESPACE, "CollectionUUID", v));
            });

    if (shouldUpdateRecord(record, collection.getModified())) {
      // common fields that have to be updated as long as idigbio is more up to date, even for IH
      // entities
      getStringValueOpt(record.getDescription()).ifPresent(collection::setDescription);
      getStringValueOpt(record.getDescriptionForSpecialists())
          .ifPresent(v -> collection.setDescription(collection.getDescription() + "\n" + v));
      getStringValueOpt(record.getCollectionCatalogUrl())
          .flatMap(DataParser::parseUri)
          .ifPresent(collection::setCatalogUrl);

      if (!containsIrnIdentifier(collection)) {
        // only for non-IH
        Optional<String> collectionName = getStringValueOpt(record.getCollection());
        if (collectionName.isPresent()) {
          collection.setName(collectionName.get());
        } else if (collection.getName() == null) {
          collection.setName(
              getStringValueOpt(record.getInstitution())
                  .orElse(institution != null ? institution.getName() : null));
        }

        getStringValueOpt(record.getCollectionUrl())
            .flatMap(DataParser::parseUri)
            .ifPresent(collection::setHomepage);
        if (record.getCataloguedSpecimens() != null) {
          collection.setNumberSpecimens(record.getCataloguedSpecimens());
        } else {
          try {
            getStringValueOpt(record.getCollectionExtent())
                .map(v -> v.replaceAll("[,]", ""))
                .map(v -> v.replace("objects", ""))
                .map(v -> v.replace("specimens", "").trim())
                .map(Integer::valueOf)
                .ifPresent(collection::setNumberSpecimens);
          } catch (NumberFormatException e) {
            log.warn("Couldn't parse collection extent: " + record.getCollectionExtent());
          }
        }
        getStringValueOpt(record.getTaxonCoverage()).ifPresent(collection::setTaxonomicCoverage);
        getStringValueOpt(record.getGeographicRange()).ifPresent(collection::setGeography);

        // addresses
        collection.setAddress(convertAddress(record.getPhysicalAddress(), collection.getAddress()));
        collection.setMailingAddress(
            convertAddress(record.getMailingAddress(), collection.getMailingAddress()));
      }
    }

    return collection;
  }

  private static void setCodes(
      List<String> idigbioCodes,
      String currentCode,
      Map<String, String> alternativeCodes,
      Consumer<String> codeSetter) {
    if (idigbioCodes.isEmpty()
        || idigbioCodes.stream().allMatch(c -> c.equalsIgnoreCase(currentCode))) {
      // if there are no new codes we don't do anything
      return;
    }

    Iterator<String> iDigBioCodesIterator = idigbioCodes.iterator();
    String iDigBioMainCode = iDigBioCodesIterator.next();
    if (!iDigBioMainCode.equalsIgnoreCase(currentCode)) {
      codeSetter.accept(iDigBioMainCode);

      if (currentCode != null) {
        alternativeCodes.put(
            currentCode, "code replaced by the one migrated from iDigBio: " + iDigBioMainCode);
      }
    }

    while (iDigBioCodesIterator.hasNext()) {
      String code = iDigBioCodesIterator.next();
      if (!code.equalsIgnoreCase(currentCode)) {
        alternativeCodes.put(code, "Code migrated from iDigBio");
      }
    }
  }

  private static Address convertAddress(IDigBioRecord.Address iDigBioAddress, Address address) {
    if (!existsAddress(iDigBioAddress)) {
      return address;
    }

    if (address == null) {
      address = new Address();
    }
    address.setAddress(getStringValue(iDigBioAddress.getAddress()));
    address.setCity(getStringValue(iDigBioAddress.getCity()));
    address.setPostalCode(getStringValue(iDigBioAddress.getZip()));
    address.setProvince(getStringValue(iDigBioAddress.getState()));
    address.setCountry(Country.UNITED_STATES);

    return address;
  }

  public static Person convertToPerson(IDigBioRecord iDigBioRecord) {
    return convertToPerson(null, iDigBioRecord);
  }

  public static Person convertToPerson(Person existing, IDigBioRecord iDigBioRecord) {
    if (existing != null && containsIrnIdentifier(existing)) {
      return existing;
    }

    Person person = clonePerson(existing);
    person.setFirstName(cleanString(iDigBioRecord.getContact()));
    person.setEmail(cleanString(iDigBioRecord.getContactEmail()));
    person.setPosition(cleanString(iDigBioRecord.getContactRole()));

    return person;
  }

  private static boolean existsAddress(IDigBioRecord.Address idigbioAddress) {
    return idigbioAddress != null
        && (!Strings.isNullOrEmpty(idigbioAddress.getAddress())
            || !Strings.isNullOrEmpty(idigbioAddress.getCity())
            || !Strings.isNullOrEmpty(idigbioAddress.getState())
            || !Strings.isNullOrEmpty(idigbioAddress.getZip()));
  }

  private static boolean shouldUpdateRecord(IDigBioRecord record, Date grSciCollEntityDate) {
    return record.getModifiedDate() == null
        || grSciCollEntityDate == null
        || record.getModifiedDate().isAfter(TO_LOCAL_DATE_TIME_UTC.apply(grSciCollEntityDate));
  }

  static <T extends Identifiable> boolean addIdentifierIfNotExists(
      T entity, Identifier identifier) {
    if (entity.getIdentifiers().stream().noneMatch(i -> i.lenientEquals(identifier))) {
      entity.getIdentifiers().add(identifier);
      return true;
    }
    return false;
  }

  static <T extends MachineTaggable> boolean addMachineTagIfNotExists(
      T entity, MachineTag machineTag) {
    if (entity.getMachineTags().stream().noneMatch(mt -> mt.lenientEquals(machineTag))) {
      entity.getMachineTags().add(machineTag);
      return true;
    }
    return false;
  }
}
