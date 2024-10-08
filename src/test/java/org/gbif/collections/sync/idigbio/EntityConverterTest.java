package org.gbif.collections.sync.idigbio;

import static org.gbif.collections.sync.common.parsers.DataParser.TO_BIGDECIMAL;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.IDIGBIO_COLLECTION_UUID;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.IDIGBIO_NAMESPACE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.AlternativeCode;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;
import org.junit.Test;

/** Tests the {@link IDigBioEntityConverter}. */
public class EntityConverterTest {

  private final IDigBioEntityConverter entityConverter = IDigBioEntityConverter.create();

  @Test
  public void convertToInstitutionTest() {
    IDigBioRecord iDigBioRecord = createIDigBioInstitution();
    Institution institutionConverted = entityConverter.convertToInstitution(iDigBioRecord);

    assertEquals(iDigBioRecord.getInstitutionCode(), institutionConverted.getCode());
    assertEquals(iDigBioRecord.getInstitution(), institutionConverted.getName());
    assertEquals(TO_BIGDECIMAL.apply(iDigBioRecord.getLat()), institutionConverted.getLatitude());

    // assert identifiers
    assertIdentifiersAndTagsInstitution(iDigBioRecord, institutionConverted, false);
  }

  @Test
  public void convertToInstitutionFromExistingTest() {
    IDigBioRecord iDigBioRecord = createIDigBioInstitution();
    Institution existing = createInstitution();

    Institution institutionConverted =
        entityConverter.convertToInstitution(iDigBioRecord, existing);

    assertEquals(iDigBioRecord.getInstitutionCode(), institutionConverted.getCode());
    assertEquals(iDigBioRecord.getInstitution(), institutionConverted.getName());
    assertEquals(TO_BIGDECIMAL.apply(iDigBioRecord.getLat()), institutionConverted.getLatitude());
    assertEquals(existing.getLongitude(), institutionConverted.getLongitude());
    assertEquals(existing.getDescription(), institutionConverted.getDescription());
    assertTrue(
        containsAlternativeCode(institutionConverted.getAlternativeCodes(), existing.getCode()));
    assertIdentifiersAndTagsInstitution(iDigBioRecord, institutionConverted, false);

    // make iDigBio less recent than the existing one
    existing = createInstitution();
    existing.setModified(Date.from(LocalDateTime.now().toInstant(ZoneOffset.UTC)));
    iDigBioRecord = createIDigBioInstitution();
    iDigBioRecord.setModifiedDate(LocalDateTime.now().minusDays(1));

    institutionConverted = entityConverter.convertToInstitution(iDigBioRecord, existing);
    assertEquals(existing.getName(), institutionConverted.getName());
    assertEquals(existing.getLongitude(), institutionConverted.getLongitude());
    assertEquals(existing.getLatitude(), institutionConverted.getLatitude());
    assertEquals(1, institutionConverted.getIdentifiers().size());
    assertEquals(1, institutionConverted.getMachineTags().size());
    assertIdentifiersAndTagsInstitution(iDigBioRecord, institutionConverted, false);
  }

  @Test
  public void convertToInstitutionFromExistingIHTest() {
    IDigBioRecord iDigBioRecord = createIDigBioInstitution();
    Institution existing = createInstitution();
    existing.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, "1234"));

    Institution institutionConverted =
        entityConverter.convertToInstitution(iDigBioRecord, existing);

    assertEquals(existing.getCode(), institutionConverted.getCode());
    assertEquals(existing.getName(), institutionConverted.getName());
    assertEquals(existing.getLatitude(), institutionConverted.getLatitude());
    assertEquals(existing.getLongitude(), institutionConverted.getLongitude());
    assertEquals(existing.getDescription(), institutionConverted.getDescription());
    assertTrue(
        containsAlternativeCode(
            institutionConverted.getAlternativeCodes(), iDigBioRecord.getInstitutionCode()));

    assertIdentifiersAndTagsInstitution(iDigBioRecord, institutionConverted, true);
  }

  @Test
  public void convertCollectionTest() {
    IDigBioRecord iDigBioRecord = createIDigBioCollection();

    Institution inst = new Institution();
    inst.setKey(UUID.randomUUID());
    Collection collectionConverted =
        entityConverter.convertToCollection(iDigBioRecord, inst).getCollection();

    assertEquals(inst.getKey(), collectionConverted.getInstitutionKey());
    assertTrue(collectionConverted.getDescription().contains(iDigBioRecord.getDescription()));
    assertTrue(
        collectionConverted
            .getDescription()
            .contains(iDigBioRecord.getDescriptionForSpecialists()));
    assertEquals(
        URI.create(iDigBioRecord.getCollectionCatalogUrl()),
        collectionConverted.getCatalogUrls().get(0));
    assertEquals(iDigBioRecord.getCollectionCode(), collectionConverted.getCode());
    assertEquals(iDigBioRecord.getCollection(), collectionConverted.getName());
    assertEquals(URI.create(iDigBioRecord.getCollectionUrl()), collectionConverted.getHomepage());
    assertEquals(
        Integer.valueOf(iDigBioRecord.getCollectionExtent()),
        collectionConverted.getNumberSpecimens());
    assertEquals(iDigBioRecord.getTaxonCoverage(), collectionConverted.getTaxonomicCoverage());
    assertEquals(iDigBioRecord.getGeographicRange(), collectionConverted.getGeographicCoverage());
    assertAddress(iDigBioRecord.getMailingAddress(), collectionConverted.getMailingAddress());
    assertAddress(iDigBioRecord.getPhysicalAddress(), collectionConverted.getAddress());
    assertIdentifiersAndTagsCollection(iDigBioRecord, collectionConverted, false);
  }

  @Test
  public void convertCollectionFromExistingTest() {
    IDigBioRecord iDigBioRecord = createIDigBioCollection();
    Collection existing = createCollection();

    Institution inst = new Institution();
    inst.setKey(UUID.randomUUID());
    Collection collectionConverted =
        entityConverter.convertToCollection(iDigBioRecord, existing, inst).getCollection();

    assertEquals(inst.getKey(), collectionConverted.getInstitutionKey());
    assertTrue(collectionConverted.getDescription().contains(iDigBioRecord.getDescription()));
    assertTrue(
        collectionConverted
            .getDescription()
            .contains(iDigBioRecord.getDescriptionForSpecialists()));
    assertEquals(
        URI.create(iDigBioRecord.getCollectionCatalogUrl()),
        collectionConverted.getCatalogUrls().get(0));
    assertEquals(iDigBioRecord.getCollectionCode(), collectionConverted.getCode());
    assertEquals(iDigBioRecord.getCollection(), collectionConverted.getName());
    assertEquals(URI.create(iDigBioRecord.getCollectionUrl()), collectionConverted.getHomepage());
    assertEquals(
        Integer.valueOf(iDigBioRecord.getCollectionExtent()),
        collectionConverted.getNumberSpecimens());
    assertEquals(iDigBioRecord.getTaxonCoverage(), collectionConverted.getTaxonomicCoverage());
    assertEquals(iDigBioRecord.getGeographicRange(), collectionConverted.getGeographicCoverage());
    assertAddress(iDigBioRecord.getMailingAddress(), collectionConverted.getMailingAddress());
    assertAddress(iDigBioRecord.getPhysicalAddress(), collectionConverted.getAddress());
    assertIdentifiersAndTagsCollection(iDigBioRecord, collectionConverted, false);

    // make iDigBio less recent than the existing one
    existing = createCollection();
    existing.setModified(Date.from(LocalDateTime.now().toInstant(ZoneOffset.UTC)));
    iDigBioRecord = createIDigBioCollection();
    iDigBioRecord.setModifiedDate(LocalDateTime.now().minusDays(1));
    collectionConverted =
        entityConverter.convertToCollection(iDigBioRecord, existing).getCollection();

    assertEquals(existing.getDescription(), collectionConverted.getDescription());
    assertEquals(existing.getCatalogUrls(), collectionConverted.getCatalogUrls());
    assertEquals(existing.getCode(), collectionConverted.getCode());
    assertEquals(existing.getName(), collectionConverted.getName());
    assertEquals(existing.getHomepage(), collectionConverted.getHomepage());
    assertEquals(existing.getNumberSpecimens(), collectionConverted.getNumberSpecimens());
    assertEquals(existing.getTaxonomicCoverage(), collectionConverted.getTaxonomicCoverage());
    assertEquals(existing.getGeographicCoverage(), collectionConverted.getGeographicCoverage());
    assertEquals(existing.getAddress(), collectionConverted.getAddress());
    assertEquals(existing.getMailingAddress(), collectionConverted.getMailingAddress());
    assertIdentifiersAndTagsCollection(iDigBioRecord, collectionConverted, false);
  }

  @Test
  public void convertCollectionFromExistingIHTest() {
    IDigBioRecord iDigBioRecord = createIDigBioCollection();
    Collection existing = createCollection();
    existing.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, "1234"));

    Institution inst = new Institution();
    inst.setKey(UUID.randomUUID());
    Collection collectionConverted =
        entityConverter.convertToCollection(iDigBioRecord, existing, inst).getCollection();

    assertEquals(inst.getKey(), collectionConverted.getInstitutionKey());
    assertTrue(collectionConverted.getDescription().contains(iDigBioRecord.getDescription()));
    assertTrue(
        collectionConverted
            .getDescription()
            .contains(iDigBioRecord.getDescriptionForSpecialists()));
    assertEquals(
        URI.create(iDigBioRecord.getCollectionCatalogUrl()),
        collectionConverted.getCatalogUrls().get(0));
    assertEquals(existing.getCode(), collectionConverted.getCode());
    assertEquals(existing.getName(), collectionConverted.getName());
    assertEquals(existing.getHomepage(), collectionConverted.getHomepage());
    assertEquals(existing.getNumberSpecimens(), collectionConverted.getNumberSpecimens());
    assertEquals(existing.getTaxonomicCoverage(), collectionConverted.getTaxonomicCoverage());
    assertEquals(existing.getGeographicCoverage(), collectionConverted.getGeographicCoverage());
    assertEquals(existing.getAddress(), collectionConverted.getAddress());
    assertEquals(existing.getMailingAddress(), collectionConverted.getMailingAddress());
    assertIdentifiersAndTagsCollection(iDigBioRecord, collectionConverted, true);
  }

  private void assertAddress(IDigBioRecord.Address iDigBioAddress, Address address) {
    assertEquals(iDigBioAddress.getAddress(), address.getAddress());
    assertEquals(iDigBioAddress.getCity(), address.getCity());
    assertEquals(iDigBioAddress.getState(), address.getProvince());
    assertEquals(iDigBioAddress.getZip(), address.getPostalCode());
    assertEquals(Country.UNITED_STATES, address.getCountry());
  }

  private Institution createInstitution() {
    Institution institution = new Institution();
    institution.setLatitude(BigDecimal.valueOf(10));
    institution.setLongitude(BigDecimal.valueOf(1));
    institution.setCode("c");
    institution.setName("inst");
    institution.setDescription("description");
    return institution;
  }

  private IDigBioRecord createIDigBioInstitution() {
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setInstitutionCode("c1");
    iDigBioRecord.setInstitution("inst1");
    iDigBioRecord.setUniqueNameUuid("uuid");
    iDigBioRecord.setLat(10d);
    return iDigBioRecord;
  }

  private Collection createCollection() {
    Collection collection = new Collection();
    collection.setCode("c");
    collection.setName("inst");
    collection.setDescription("description");
    collection.setHomepage(URI.create("http://test.com"));
    collection.setTaxonomicCoverage("taxon coverage");

    Address address = new Address();
    address.setProvince("province");
    address.setAddress("aa");
    address.setPostalCode("122");
    collection.setMailingAddress(address);

    return collection;
  }

  private IDigBioRecord createIDigBioCollection() {
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setCollectionCode("collCode1");
    iDigBioRecord.setCollection("inst1");
    iDigBioRecord.setDescription("descr");
    iDigBioRecord.setDescriptionForSpecialists("specialists");
    iDigBioRecord.setRecordSets("recordSets");
    iDigBioRecord.setRecordsetQuery("query");
    iDigBioRecord.setCollectionLsid("lsid");
    iDigBioRecord.setCollectionUuid("uuid");
    iDigBioRecord.setCollectionCatalogUrl("http://catalog.com");
    iDigBioRecord.setCollectionUrl("http://coll.com");
    iDigBioRecord.setCollectionExtent("1");
    iDigBioRecord.setTaxonCoverage("tc");
    iDigBioRecord.setGeographicRange("gr");

    IDigBioRecord.Address mailingAddress = new IDigBioRecord.Address();
    mailingAddress.setAddress("address");
    mailingAddress.setCity("city");
    mailingAddress.setState("state");
    mailingAddress.setZip("123");
    iDigBioRecord.setMailingAddress(mailingAddress);

    IDigBioRecord.Address physicalAddress = new IDigBioRecord.Address();
    physicalAddress.setAddress("address2");
    physicalAddress.setCity("city2");
    physicalAddress.setState("state2");
    physicalAddress.setZip("12345");
    iDigBioRecord.setPhysicalAddress(physicalAddress);

    return iDigBioRecord;
  }

  private void assertIdentifiersAndTagsInstitution(
      IDigBioRecord iDigBioRecord, Institution institutionConverted, boolean isIH) {
    // assert identifiers
    assertEquals(isIH ? 2 : 1, institutionConverted.getIdentifiers().size());
    if (isIH) {
      assertTrue(
          institutionConverted.getIdentifiers().stream()
              .anyMatch(i -> i.getType() == IdentifierType.IH_IRN));
    }
    Identifier uuidIdentifier =
        institutionConverted.getIdentifiers().stream()
            .filter(i -> i.getType() == IdentifierType.UUID)
            .findFirst()
            .orElse(null);
    assertEquals(iDigBioRecord.getUniqueNameUuid(), uuidIdentifier.getIdentifier());
    assertSame(IdentifierType.UUID, uuidIdentifier.getType());

    // assert machine tags
    assertEquals(1, institutionConverted.getMachineTags().size());
    MachineTag machineTag = institutionConverted.getMachineTags().get(0);
    assertEquals(iDigBioRecord.getUniqueNameUuid(), machineTag.getValue());
    assertEquals("UniqueNameUUID", machineTag.getName());
    assertEquals(IDIGBIO_NAMESPACE, machineTag.getNamespace());
  }

  private void assertIdentifiersAndTagsCollection(
      IDigBioRecord iDigBioRecord, Collection collectionConverted, boolean isIH) {
    // assert identifiers
    assertEquals(isIH ? 3 : 2, collectionConverted.getIdentifiers().size());
    if (isIH) {
      assertTrue(
          collectionConverted.getIdentifiers().stream()
              .anyMatch(i -> i.getType() == IdentifierType.IH_IRN));
    }
    Optional<Identifier> lsidIdentifier =
        collectionConverted.getIdentifiers().stream()
            .filter(i -> i.getType() == IdentifierType.LSID)
            .findFirst();
    assertTrue(lsidIdentifier.isPresent());
    assertEquals(iDigBioRecord.getCollectionLsid(), lsidIdentifier.get().getIdentifier());
    Optional<Identifier> uuidIdentifier =
        collectionConverted.getIdentifiers().stream()
            .filter(i -> i.getType() == IdentifierType.UUID)
            .findFirst();
    assertTrue(uuidIdentifier.isPresent());
    assertEquals(iDigBioRecord.getCollectionUuid(), uuidIdentifier.get().getIdentifier());

    // assert machine tags
    assertEquals(3, collectionConverted.getMachineTags().size());
    Optional<MachineTag> mtRecordSets =
        collectionConverted.getMachineTags().stream()
            .filter(mt -> mt.getName().equals("recordsets"))
            .findFirst();
    assertTrue(mtRecordSets.isPresent());
    assertEquals(IDIGBIO_NAMESPACE, mtRecordSets.get().getNamespace());
    assertEquals(iDigBioRecord.getRecordSets(), mtRecordSets.get().getValue());

    Optional<MachineTag> mtRecordSetQuery =
        collectionConverted.getMachineTags().stream()
            .filter(mt -> mt.getName().equals("recordsetQuery"))
            .findFirst();
    assertTrue(mtRecordSetQuery.isPresent());
    assertEquals(IDIGBIO_NAMESPACE, mtRecordSetQuery.get().getNamespace());
    assertEquals(iDigBioRecord.getRecordsetQuery(), mtRecordSetQuery.get().getValue());

    Optional<MachineTag> mtCollUuid =
        collectionConverted.getMachineTags().stream()
            .filter(mt -> mt.getName().equals(IDIGBIO_COLLECTION_UUID))
            .findFirst();
    assertTrue(mtCollUuid.isPresent());
    assertEquals(IDIGBIO_NAMESPACE, mtCollUuid.get().getNamespace());
    assertEquals(iDigBioRecord.getCollectionUuid(), mtCollUuid.get().getValue());
  }

  @Test
  public void addIdentifierIfNotExistsTest() {
    Identifier i1 = new Identifier(IdentifierType.LSID, "lsid");
    Identifier i2 = new Identifier(IdentifierType.URI, "uri");

    Collection col = new Collection();
    col.getIdentifiers().add(i1);
    col.getIdentifiers().add(i2);

    assertFalse(
        IDigBioEntityConverter.addIdentifierIfNotExists(
            col, new Identifier(IdentifierType.LSID, "lsid")));
    assertTrue(
        IDigBioEntityConverter.addIdentifierIfNotExists(
            col, new Identifier(IdentifierType.URI, "lsid")));
    assertTrue(
        IDigBioEntityConverter.addIdentifierIfNotExists(
            col, new Identifier(IdentifierType.URI, "other")));
  }

  @Test
  public void addMachineTagIfNotExistsTest() {
    MachineTag mt1 = new MachineTag("ns", "name", "value");
    MachineTag mt2 = new MachineTag("ns", "name2", "value2");

    Collection col = new Collection();
    col.getMachineTags().add(mt1);
    col.getMachineTags().add(mt2);

    assertFalse(
        IDigBioEntityConverter.addMachineTagIfNotExists(
            col, new MachineTag("ns", "name", "value")));
    assertTrue(
        IDigBioEntityConverter.addMachineTagIfNotExists(
            col, new MachineTag("ns", "other", "value")));
    assertTrue(
        IDigBioEntityConverter.addMachineTagIfNotExists(
            col, new MachineTag("ns", "name", "value3")));
  }

  @Test
  public void multipleCodesTest() {
    IDigBioRecord r1 = new IDigBioRecord();
    r1.setCollectionCode("A,B,C");
    r1.setCollection("Coll 1");

    Institution i = new Institution();
    i.setKey(UUID.randomUUID());

    Collection result = entityConverter.convertToCollection(r1, i).getCollection();
    assertEquals("A", result.getCode());
    assertEquals(2, result.getAlternativeCodes().size());
  }

  private boolean containsAlternativeCode(List<AlternativeCode> altCodes, String code) {
    return altCodes.stream().anyMatch(ac -> ac.getCode().equals(code));
  }
}
