package org.gbif.collections.sync.ih;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.LenientEquals;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.collections.InstitutionType;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.model.IHEntity;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import lombok.Builder;
import lombok.Data;

import static org.gbif.collections.sync.TestUtils.createTestSyncConfig;
import static org.gbif.collections.sync.common.Utils.encodeIRN;
import static org.gbif.collections.sync.common.parsers.DataParser.TO_BIGDECIMAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BaseIHTest {

  protected static final String IRN_TEST = "1";
  protected static final String TEST_USER = "test-user";
  protected static final List<String> COUNTRIES =
      Arrays.asList("UK", "U.K.", "U.S.A.", "United Kingdom", "United States");

  protected static final CountryParser countryParser =
      CountryParser.from(Arrays.asList("U.K.", "U.S.A.", "United Kingdom", "United States"));
  protected static final IHConfig ihConfig = createConfig();
  protected final IHSynchronizer synchronizer =
      IHSynchronizer.builder()
          .dataLoader(TestDataLoader.builder().countries(COUNTRIES).build())
          .ihConfig(ihConfig)
          .build();

  protected <T extends CollectionEntity & LenientEquals<T>, R extends IHEntity>
      void assertEntityMatch(
          SyncResult.EntityMatch<T> entityMatch, TestEntity<T, R> testEntity, boolean update) {
    assertEquals(update, entityMatch.isUpdate());
    assertTrue(entityMatch.getMatched().lenientEquals(testEntity.entity));
    if (update) {
      assertTrue(entityMatch.getMerged().lenientEquals(testEntity.expected));
    } else {
      assertTrue(entityMatch.getMerged().lenientEquals(testEntity.entity));
    }
  }

  protected TestEntity<Institution, IHInstitution> createInstitutionNoChange() {
    Institution i = new Institution();
    i.setKey(UUID.randomUUID());
    i.setCode("bar");
    i.setName("bar");
    i.setIndexHerbariorumRecord(true);
    i.setNumberSpecimens(1000);
    i.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST)));

    IHInstitution ih = new IHInstitution();
    ih.setIrn(IRN_TEST);
    ih.setCode("bar");
    ih.setOrganization("bar");
    ih.setSpecimenTotal(1000);

    return TestEntity.<Institution, IHInstitution>builder().entity(i).ih(ih).build();
  }

  protected IHInstitution createIHInstitution() {
    IHInstitution ih = new IHInstitution();
    ih.setIrn(IRN_TEST);
    ih.setCode("CODE");
    ih.setOrganization("Test Organization");
    ih.setSpecimenTotal(1000);
    ih.setCurrentStatus("active");
    ih.setTaxonomicCoverage("taxonomic");
    ih.setGeography("geography");
    ih.setNotes("notes");
    ih.setIncorporatedHerbaria(Collections.singletonList("incorporated herbaria"));
    ih.setImportantCollectors(Collections.singletonList("dummy collector"));
    IHInstitution.CollectionSummary summary = new IHInstitution.CollectionSummary();
    summary.setNumAlgae(1);
    summary.setNumBryos(10);
    ih.setCollectionsSummary(summary);
    ih.setDateFounded("2000");

    IHInstitution.Address ihAddress = new IHInstitution.Address();
    ihAddress.setPhysicalCity("city1");
    ihAddress.setPhysicalCountry("U.S.A.");
    ihAddress.setPostalCity("City2");
    ihAddress.setPostalCountry("UK");
    ih.setAddress(ihAddress);

    IHInstitution.Location location = new IHInstitution.Location();
    location.setLat(30d);
    location.setLon(-80d);
    ih.setLocation(location);

    IHInstitution.Contact contact = new IHInstitution.Contact();
    contact.setEmail("a@aa.com");
    ih.setContact(contact);

    return ih;
  }

  protected TestEntity<Institution, IHInstitution> createInstitutionToUpdate() {
    Institution i = new Institution();
    i.setKey(UUID.randomUUID());
    i.setCode("COD");
    i.setName("University OLD");
    i.setType(InstitutionType.HERBARIUM);
    i.setLatitude(TO_BIGDECIMAL.apply(36.0424));
    i.setLongitude(TO_BIGDECIMAL.apply(-94.1624));

    Address address = new Address();
    address.setCity("FAYETTEVILLE");
    address.setProvince("Arkansas");
    address.setCountry(Country.UNITED_STATES);
    i.setMailingAddress(address);

    IHInstitution ih = createIHInstitution();

    Institution expected = new Institution();
    expected.setKey(i.getKey());
    expected.setCode(ih.getCode());
    expected.setName(ih.getOrganization());
    expected.setType(i.getType());
    expected.setIndexHerbariorumRecord(true);
    expected.setLatitude(TO_BIGDECIMAL.apply(ih.getLocation().getLat()));
    expected.setLongitude(TO_BIGDECIMAL.apply(ih.getLocation().getLon()));
    expected.setEmail(Collections.singletonList(ih.getContact().getEmail()));
    expected.setActive(true);
    expected.setFoundingDate(IsoDateParsingUtils.parseDate(ih.getDateFounded()));

    Address expectedMailingAddress = new Address();
    expectedMailingAddress.setCity(ih.getAddress().getPostalCity());
    expectedMailingAddress.setCountry(Country.UNITED_KINGDOM);
    expected.setMailingAddress(expectedMailingAddress);

    Address physicalAddress = new Address();
    physicalAddress.setCity(ih.getAddress().getPhysicalCity());
    physicalAddress.setCountry(Country.UNITED_STATES);
    expected.setAddress(physicalAddress);

    Identifier newIdentifier = new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST));
    newIdentifier.setCreatedBy(TEST_USER);
    expected.getIdentifiers().add(newIdentifier);

    return TestEntity.<Institution, IHInstitution>builder()
        .entity(i)
        .ih(ih)
        .expected(expected)
        .build();
  }

  protected TestEntity<Collection, IHInstitution> createCollectionNoChange() {
    Collection c = new Collection();
    c.setKey(UUID.randomUUID());
    c.setCode("A");
    c.setIndexHerbariorumRecord(true);
    c.setEmail(Collections.singletonList("aa@aa.com"));
    c.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST)));

    IHInstitution ih = new IHInstitution();
    ih.setIrn(IRN_TEST);
    ih.setCode("A");
    IHInstitution.Contact contact = new IHInstitution.Contact();
    contact.setEmail("aa@aa.com");
    ih.setContact(contact);

    return TestEntity.<Collection, IHInstitution>builder().entity(c).ih(ih).build();
  }

  protected TestEntity<Collection, IHInstitution> createCollectionToUpdate() {
    Collection c = new Collection();
    c.setKey(UUID.randomUUID());
    c.setCode("B");
    c.setEmail(Collections.singletonList("bb@bb.com"));
    c.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST)));

    IHInstitution ih = createIHInstitution();

    Collection expected = new Collection();
    expected.setKey(c.getKey());
    expected.setCode(ih.getCode());
    expected.setName(ih.getOrganization());
    expected.setIndexHerbariorumRecord(true);
    expected.setNumberSpecimens(ih.getSpecimenTotal());
    expected.setEmail(Collections.singletonList(ih.getContact().getEmail()));
    expected.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST)));
    expected.setActive(true);
    expected.setNumberSpecimens(ih.getSpecimenTotal());
    expected.setTaxonomicCoverage(ih.getTaxonomicCoverage());
    expected.setGeography(ih.getGeography());
    expected.setNotes(ih.getNotes());
    expected.setIncorporatedCollections(ih.getIncorporatedHerbaria());
    expected.setImportantCollectors(ih.getImportantCollectors());
    expected.setCollectionSummary(convertCollectionSummary(ih.getCollectionsSummary()));

    Address expectedMailingAddress = new Address();
    expectedMailingAddress.setCity(ih.getAddress().getPostalCity());
    expectedMailingAddress.setCountry(Country.UNITED_KINGDOM);
    expected.setMailingAddress(expectedMailingAddress);

    Address physicalAddress = new Address();
    physicalAddress.setCity(ih.getAddress().getPhysicalCity());
    physicalAddress.setCountry(Country.UNITED_STATES);
    expected.setAddress(physicalAddress);

    return TestEntity.<Collection, IHInstitution>builder()
        .entity(c)
        .ih(ih)
        .expected(expected)
        .build();
  }

  protected TestEntity<Person, IHStaff> createTestStaffToUpdate() {
    Person p = new Person();
    p.setKey(UUID.randomUUID());
    p.setFirstName("First M. Last");
    p.setPosition("Director");
    p.setPhone("[1] 479/575-4372");
    p.setEmail("b@b.com");
    Address mailingAddress = new Address();
    mailingAddress.setCity("FAYETTEVILLE");
    mailingAddress.setProvince("Arkansas");
    mailingAddress.setCountry(Country.UNITED_STATES);
    p.setMailingAddress(mailingAddress);

    IHStaff s = new IHStaff();
    s.setIrn(IRN_TEST);
    s.setCode("CODE");
    s.setLastName("Last");
    s.setMiddleName("M.");
    s.setFirstName("First");
    s.setPosition("Professor Emeritus");

    IHStaff.Address address = new IHStaff.Address();
    address.setStreet("");
    address.setCity("Fayetteville");
    address.setState("Arkansas");
    address.setCountry("U.S.A.");
    s.setAddress(address);

    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setEmail("a@a.com");
    s.setContact(contact);

    Person expected = new Person();
    expected.setKey(p.getKey());
    expected.setFirstName(s.getFirstName() + " " + s.getMiddleName());
    expected.setLastName(s.getLastName());
    expected.setPosition(s.getPosition());
    expected.setEmail(s.getContact().getEmail());
    Address expectedAddress = new Address();
    expectedAddress.setCity(address.getCity());
    expectedAddress.setProvince(address.getState());
    expectedAddress.setCountry(Country.UNITED_STATES);
    expected.setMailingAddress(expectedAddress);
    Identifier newIdentifier = new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST));
    newIdentifier.setCreatedBy(TEST_USER);
    expected.getIdentifiers().add(newIdentifier);

    return TestEntity.<Person, IHStaff>builder().entity(p).ih(s).expected(expected).build();
  }

  protected TestEntity<Person, IHStaff> createTestStaffNoChange() {
    Person p = new Person();
    p.setFirstName("foo");
    p.setKey(UUID.randomUUID());
    p.setEmail("foo@foo.com");
    p.getIdentifiers().add(new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST)));

    IHStaff s = new IHStaff();
    s.setFirstName("foo");
    s.setIrn(IRN_TEST);
    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setEmail("foo@foo.com");
    s.setContact(contact);

    return TestEntity.<Person, IHStaff>builder().entity(p).ih(s).build();
  }

  protected TestEntity<Person, IHStaff> createTestStaffToRemove() {
    Person p = new Person();
    p.setKey(UUID.randomUUID());
    p.setFirstName("extra person");

    IHStaff ihStaff = new IHStaff();
    ihStaff.setIrn(IRN_TEST);

    return TestEntity.<Person, IHStaff>builder().entity(p).ih(ihStaff).build();
  }

  protected TestEntity<Person, IHStaff> createTestStaffToCreate() {
    IHStaff s = new IHStaff();
    s.setCode("CODE");
    s.setLastName("Last");
    s.setMiddleName("M.");
    s.setFirstName("First");
    s.setPosition("Collections Manager");

    IHStaff.Address address = new IHStaff.Address();
    address.setStreet("");
    address.setCity("Fayetteville");
    address.setState("AR");
    address.setCountry("U.S.A.");
    address.setZipCode("72701");
    s.setAddress(address);

    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setPhone("[1] 479 575 4372");
    contact.setEmail("a@a.com");
    s.setContact(contact);

    Person expected = new Person();
    expected.setFirstName(s.getFirstName() + " " + s.getMiddleName());
    expected.setLastName(s.getLastName());
    expected.setPosition(s.getPosition());
    expected.setEmail(s.getContact().getEmail());
    expected.setPhone(s.getContact().getPhone());
    Address expectedAddress = new Address();
    expectedAddress.setAddress(address.getStreet());
    expectedAddress.setCity(address.getCity());
    expectedAddress.setProvince(address.getState());
    expectedAddress.setCountry(Country.UNITED_STATES);
    expected.setMailingAddress(expectedAddress);

    return TestEntity.<Person, IHStaff>builder().ih(s).expected(expected).build();
  }

  protected Map<String, Integer> convertCollectionSummary(IHInstitution.CollectionSummary summary) {
    Map<String, Integer> map = new HashMap<>();
    map.put("numAlgae", summary.getNumAlgae());
    map.put("numAlgaeDatabased", summary.getNumAlgaeDatabased());
    map.put("numAlgaeImaged", summary.getNumAlgaeImaged());
    map.put("numBryos", summary.getNumBryos());
    map.put("numBryosDatabased", summary.getNumBryosDatabased());
    map.put("numBryosImaged", summary.getNumBryosImaged());
    map.put("numFungi", summary.getNumFungi());
    map.put("numFungiDatabased", summary.getNumFungiDatabased());
    map.put("numFungiImaged", summary.getNumFungiImaged());
    map.put("numPteridos", summary.getNumPteridos());
    map.put("numPteridosDatabased", summary.getNumPteridosDatabased());
    map.put("numPteridosImaged", summary.getNumPteridosImaged());
    map.put("numSeedPl", summary.getNumSeedPl());
    map.put("numSeedPlDatabased", summary.getNumSeedPlDatabased());
    map.put("numSeedPlImaged", summary.getNumSeedPlImaged());

    return map;
  }

  private static IHConfig createConfig() {
    IHConfig ihConfig = new IHConfig();
    ihConfig.setSyncConfig(createTestSyncConfig());
    return ihConfig;
  }

  @Builder
  @Data
  protected static class TestEntity<T extends CollectionEntity, R extends IHEntity> {
    T entity;
    T expected;
    R ih;
  }
}
