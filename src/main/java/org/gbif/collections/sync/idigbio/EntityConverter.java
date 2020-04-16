package org.gbif.collections.sync.idigbio;

import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.parsers.DataParser;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;

import static org.gbif.collections.sync.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.isIDigBioMoreRecent;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.removeUuidNamespace;
import static org.gbif.collections.sync.parsers.DataParser.TO_BIGDECIMAL;
import static org.gbif.collections.sync.parsers.DataParser.getStringValueOpt;
import static org.gbif.collections.sync.parsers.DataParser.parseStringList;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class EntityConverter {

  static final String IDIGBIO_NAMESPACE = "iDigBio.org";
  private static final String IH_SUFFIX_IDIGBIO = "<IH>";

  public static Institution convertToInstitution(IDigBioRecord record) {
    return convertToInstitution(null, record);
  }

  public static Institution convertToInstitution(Institution existing, IDigBioRecord record) {
    Institution institution = new Institution();

    if (existing != null) {
      // copy fields
      try {
        BeanUtils.copyProperties(institution, existing);
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn("Couldn't copy institution properties from bean: {}", existing);
      }
    }

    setInstitutionCodes(institution, getIdigbioCode(record.getInstitutionCode()));

    getStringValueOpt(record.getUniqueNameUuid())
        .ifPresent(
            v -> {
              institution
                  .getIdentifiers()
                  .add(new Identifier(IdentifierType.UUID, removeUuidNamespace(v)));
              institution
                  .getMachineTags()
                  .add(new MachineTag(IDIGBIO_NAMESPACE, "UniqueNameUUID", v));
            });

    // non-IH and more updated in iDigBio
    if (!containsIrnIdentifier(institution)
        && isIDigBioMoreRecent(record, institution.getModified())) {
      getStringValueOpt(record.getInstitution()).ifPresent(institution::setName);

      if (record.getLat() != null) {
        institution.setLatitude(TO_BIGDECIMAL.apply(record.getLat()));
      }
      if (record.getLon() != null) {
        institution.setLongitude(TO_BIGDECIMAL.apply(record.getLon()));
      }
    }

    return institution;
  }

  private static void setInstitutionCodes(Institution institution, Set<String> idigbioCodes) {
    if (!idigbioCodes.isEmpty() && !idigbioCodes.contains(institution.getCode())) {
      if (containsIrnIdentifier(institution)) {
        // if they don't match we keep the IH one and add the other to the alternatives
        idigbioCodes.forEach(
            c -> institution.getAlternativeCodes().put(c, "Code migrated from iDigBio"));
      } else {
        // we set the iDigBio one as main code and the others as alternative
        Iterator<String> codesIterator = idigbioCodes.iterator();
        String newCode = codesIterator.next();
        institution
            .getAlternativeCodes()
            .put(
                institution.getCode(),
                "code replaced by the one migrated from iDigBio: " + newCode);
        institution.setCode(newCode);

        while (codesIterator.hasNext()) {
          institution.getAlternativeCodes().put(codesIterator.next(), "Code migrated from iDigBio");
        }
      }
    }
  }

  public static Collection convertToCollection(IDigBioRecord record, UUID institutionKey) {
    return convertToCollection(null, record, institutionKey);
  }

  public static Collection convertToCollection(Collection existing, IDigBioRecord record) {
    return convertToCollection(existing, record, null);
  }

  public static Collection convertToCollection(
      Collection existing, IDigBioRecord record, UUID institutionKey) {
    Collection collection = new Collection();

    if (existing != null) {
      // copy fields
      try {
        BeanUtils.copyProperties(collection, existing);
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn("Couldn't copy institution properties from bean: {}", existing);
      }
    }

    if (institutionKey != null) {
      collection.setInstitutionKey(institutionKey);
    }

    // machine tags and identifiers
    getStringValueOpt(record.getRecordSets())
        .ifPresent(
            v -> collection.addMachineTag(new MachineTag(IDIGBIO_NAMESPACE, "recordsets", v)));
    getStringValueOpt(record.getRecordsetQuery())
        .ifPresent(
            v -> collection.addMachineTag(new MachineTag(IDIGBIO_NAMESPACE, "recordsetQuery", v)));
    getStringValueOpt(record.getCollectionLsid())
        .ifPresent(v -> collection.getIdentifiers().add(new Identifier(IdentifierType.LSID, v)));
    getStringValueOpt(record.getCollectionUuid())
        .ifPresent(
            v -> {
              collection
                  .getIdentifiers()
                  .add(new Identifier(IdentifierType.UUID, removeUuidNamespace(v)));
              collection
                  .getMachineTags()
                  .add(new MachineTag(IDIGBIO_NAMESPACE, "CollectionUUID", v));
            });

    if (isIDigBioMoreRecent(record, collection.getModified())) {
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
        getStringValueOpt(record.getCollectionCode()).ifPresent(collection::setCode);
        getStringValueOpt(record.getCollection()).ifPresent(collection::setName);
        getStringValueOpt(record.getCollectionUrl())
            .flatMap(DataParser::parseUri)
            .ifPresent(collection::setHomepage);
        if (record.getCataloguedSpecimens() != null) {
          collection.setNumberSpecimens(record.getCataloguedSpecimens());
        } else {
          try {
            getStringValueOpt(record.getCollectionExtent())
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

  private static Address convertAddress(IDigBioRecord.Address iDigBioAddress, Address address) {
    if (!existsAddress(iDigBioAddress)) {
      return address;
    }

    if (address == null) {
      address = new Address();
    }
    address.setAddress(iDigBioAddress.getAddress());
    address.setCity(iDigBioAddress.getCity());
    address.setPostalCode(iDigBioAddress.getZip());
    address.setProvince(iDigBioAddress.getState());
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

    Person person = new Person();
    if (existing != null) {
      try {
        BeanUtils.copyProperties(person, existing);
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn("Couldn't copy person properties from bean: {}", existing);
      }
    }

    person.setFirstName(iDigBioRecord.getContact());
    person.setEmail(iDigBioRecord.getContactEmail());
    person.setPosition(iDigBioRecord.getContactRole());

    return person;
  }

  private static Set<String> getIdigbioCode(String idigbioCode) {
    return parseStringList(idigbioCode).stream()
        .map(s -> s.replace(IH_SUFFIX_IDIGBIO, ""))
        .collect(Collectors.toSet());
  }

  private static boolean existsAddress(IDigBioRecord.Address idigbioAddress) {
    return idigbioAddress != null
        && (!Strings.isNullOrEmpty(idigbioAddress.getAddress())
            || !Strings.isNullOrEmpty(idigbioAddress.getCity())
            || !Strings.isNullOrEmpty(idigbioAddress.getState())
            || !Strings.isNullOrEmpty(idigbioAddress.getZip()));
  }
}
