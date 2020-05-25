package org.gbif.collections.sync.ih;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.gbif.collections.sync.ih.match.MatchResult;
import org.gbif.collections.sync.ih.model.IHEntity;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.parsers.CountryParser;

import org.junit.Test;

import lombok.Builder;
import lombok.Data;

import static org.gbif.collections.sync.Utils.encodeIRN;
import static org.gbif.collections.sync.parsers.DataParser.TO_BIGDECIMAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests the {@link IHSync}. */
public class IHSyncTest {

  private static final String IRN_TEST = "1";
  private static final String TEST_USER = "test-user";
  private static final CountryParser COUNTRY_PARSER =
      CountryParser.from(Arrays.asList("UK", "U.K.", "U.S.A.", "United Kingdom", "United States"));
  private static final EntityConverter ENTITY_CONVERTER =
      EntityConverter.builder().countryParser(COUNTRY_PARSER).creationUser(TEST_USER).build();
  private static final IHSync IH_SYNC =
      IHSync.builder().countryParser(COUNTRY_PARSER).entityConverter(ENTITY_CONVERTER).build();

  @Test
  public void collectionToUpdateTest() {
    TestEntity<Collection, IHInstitution> collectionToUpdate = createCollectionToUpdate();
    MatchResult match =
        MatchResult.builder()
            .collections(Collections.singleton(collectionToUpdate.entity))
            .ihInstitution(collectionToUpdate.ih)
            .build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IH_SYNC.handleCollectionMatch(match);
    assertEntityMatch(collectionOnlyMatch.getMatchedCollection(), collectionToUpdate, true);
    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
  }

  @Test
  public void collectionNoChangeTest() {
    TestEntity<Collection, IHInstitution> collectionNoChange = createCollectionNoChange();
    MatchResult match =
        MatchResult.builder()
            .collections(Collections.singleton(collectionNoChange.entity))
            .ihInstitution(collectionNoChange.ih)
            .build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IH_SYNC.handleCollectionMatch(match);
    assertEntityMatch(collectionOnlyMatch.getMatchedCollection(), collectionNoChange, false);
    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
  }

  @Test
  public void institutionToUpdateTest() {
    TestEntity<Institution, IHInstitution> institutionToUpdate = createInstitutionToUpdate();
    MatchResult match =
        MatchResult.builder()
            .institutions(Collections.singleton(institutionToUpdate.entity))
            .ihInstitution(institutionToUpdate.ih)
            .build();

    SyncResult.InstitutionOnlyMatch institutionOnlyMatch = IH_SYNC.handleInstitutionMatch(match);
    assertEntityMatch(institutionOnlyMatch.getMatchedInstitution(), institutionToUpdate, true);
    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
  }

  @Test
  public void institutionNoChangeTest() {
    TestEntity<Institution, IHInstitution> institutionNoChange = createInstitutionNoChange();
    MatchResult match =
        MatchResult.builder()
            .institutions(Collections.singleton(institutionNoChange.entity))
            .ihInstitution(institutionNoChange.ih)
            .build();

    SyncResult.InstitutionOnlyMatch institutionOnlyMatch = IH_SYNC.handleInstitutionMatch(match);
    assertEntityMatch(institutionOnlyMatch.getMatchedInstitution(), institutionNoChange, false);
    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
  }

  @Test
  public void noMatchTest() {
    // IH institution
    IHInstitution ih = new IHInstitution();
    ih.setCode("foo");
    ih.setOrganization("foo");
    ih.setSpecimenTotal(1000);

    // Expected institution
    Institution expectedInstitution = new Institution();
    expectedInstitution.setCode(ih.getCode());
    expectedInstitution.setName(ih.getOrganization());
    expectedInstitution.setIndexHerbariorumRecord(true);

    // Expected collection
    Collection expectedCollection = new Collection();
    expectedCollection.setCode(ih.getCode());
    expectedCollection.setName(ih.getOrganization());
    expectedCollection.setNumberSpecimens(1000);
    expectedCollection.setIndexHerbariorumRecord(true);

    // add identifier to expected entities
    Identifier newIdentifier = new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST));
    newIdentifier.setCreatedBy(TEST_USER);
    expectedInstitution.getIdentifiers().add(newIdentifier);
    expectedCollection.getIdentifiers().add(newIdentifier);

    MatchResult match = MatchResult.builder().ihInstitution(ih).build();
    SyncResult.NoEntityMatch noEntityMatch = IH_SYNC.handleNoMatches(match);
    assertTrue(noEntityMatch.getNewCollection().lenientEquals(expectedCollection));
    assertTrue(noEntityMatch.getNewInstitution().lenientEquals(expectedInstitution));
    assertEmptyStaffMatch(noEntityMatch.getStaffMatch());
  }

  @Test
  public void institutionAndCollectionMatchTest() {
    TestEntity<Collection, IHInstitution> collectionToUpdate = createCollectionToUpdate();
    TestEntity<Institution, IHInstitution> institutionToUpdate = createInstitutionToUpdate();
    MatchResult match =
        MatchResult.builder()
            .collections(Collections.singleton(collectionToUpdate.entity))
            .institutions(Collections.singleton(institutionToUpdate.entity))
            .ihInstitution(collectionToUpdate.ih)
            .build();

    SyncResult.InstitutionAndCollectionMatch instAndColMatch =
        IH_SYNC.handleInstitutionAndCollectionMatch(match);
    assertEntityMatch(instAndColMatch.getMatchedCollection(), collectionToUpdate, true);
    assertEntityMatch(instAndColMatch.getMatchedInstitution(), institutionToUpdate, true);
    assertEmptyStaffMatch(instAndColMatch.getStaffMatch());
  }

  @Test
  public void handleConflictTest() {
    IHInstitution ihInstitution = new IHInstitution();
    ihInstitution.setCode("code");
    ihInstitution.setOrganization("org");

    Collection c1 = new Collection();
    c1.setKey(UUID.randomUUID());

    Collection c2 = new Collection();
    c2.setKey(UUID.randomUUID());

    MatchResult match =
        MatchResult.builder().ihInstitution(ihInstitution).collection(c1).collection(c2).build();

    SyncResult.Conflict conflictMatch = IH_SYNC.handleConflict(match);
    assertNotNull(conflictMatch.getEntity());
    assertEquals(2, conflictMatch.getGrSciCollEntities().size());
  }

  @Test
  public void staffUpdateTest() {
    TestEntity<Person, IHStaff> personToUpdate = createTestStaffToUpdate();
    MatchResult match =
        MatchResult.builder()
            .ihStaff(personToUpdate.ih)
            .staffMatcher((s, ppl) -> Collections.singleton(personToUpdate.entity))
            .build();

    Institution institution = new Institution();
    institution.setContacts(Collections.singletonList(personToUpdate.entity));

    SyncResult.StaffMatch staffMatch =
        IH_SYNC.handleStaff(match, Collections.singletonList(institution));

    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
    assertEquals(1, staffMatch.getMatchedPersons().size());
    assertEntityMatch(staffMatch.getMatchedPersons().iterator().next(), personToUpdate, true);
  }

  @Test
  public void staffNoChangeTest() {
    TestEntity<Person, IHStaff> personNoChange = createTestStaffNoChange();
    MatchResult match =
        MatchResult.builder()
            .ihStaff(personNoChange.ih)
            .staffMatcher((s, ppl) -> Collections.singleton(personNoChange.entity))
            .build();

    Institution institution = new Institution();
    institution.setContacts(Collections.singletonList(personNoChange.entity));

    SyncResult.StaffMatch staffMatch =
        IH_SYNC.handleStaff(match, Collections.singletonList(institution));

    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
    assertEquals(1, staffMatch.getMatchedPersons().size());
    assertEntityMatch(staffMatch.getMatchedPersons().iterator().next(), personNoChange, false);
  }

  @Test
  public void staffToRemoveTest() {
    TestEntity<Person, IHStaff> personToRemove = createTestStaffToRemove();
    MatchResult match = MatchResult.builder().build();

    Institution institution = new Institution();
    institution.setContacts(Collections.singletonList(personToRemove.entity));

    SyncResult.StaffMatch staffMatch =
        IH_SYNC.handleStaff(match, Collections.singletonList(institution));

    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(1, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
    assertEquals(0, staffMatch.getMatchedPersons().size());
  }

  @Test
  public void staffToCreateTest() {
    TestEntity<Person, IHStaff> personsToCreate = createTestStaffToCreate();
    MatchResult match =
        MatchResult.builder()
            .ihStaff(personsToCreate.ih)
            .staffMatcher((s, ppl) -> Collections.emptySet())
            .build();

    SyncResult.StaffMatch staffMatch = IH_SYNC.handleStaff(match, Collections.emptyList());

    assertEquals(1, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
    assertEquals(0, staffMatch.getMatchedPersons().size());
  }

  @Test
  public void staffConflictTest() {
    IHStaff ihStaff = new IHStaff();
    ihStaff.setIrn(IRN_TEST);
    ihStaff.setFirstName("foo");

    Person p1 = new Person();
    p1.setKey(UUID.randomUUID());

    Person p2 = new Person();
    p2.setKey(UUID.randomUUID());

    MatchResult match =
        MatchResult.builder()
            .ihStaff(ihStaff)
            .staffMatcher((s, ppl) -> new HashSet<>(Arrays.asList(p1, p2)))
            .build();

    SyncResult.StaffMatch staffMatch = IH_SYNC.handleStaff(match, Collections.emptyList());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(1, staffMatch.getConflicts().size());
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertNotNull(staffMatch.getConflicts().iterator().next().getEntity());
    assertEquals(2, staffMatch.getConflicts().iterator().next().getGrSciCollEntities().size());
  }

  @Test
  public void identifierAndTagUpdateInCollectionTest() {
    TestEntity<Collection, IHInstitution> collectionNoChange = createCollectionNoChange();
    collectionNoChange.getEntity().setIdentifiers(Collections.emptyList());
    MatchResult match =
        MatchResult.builder()
            .collections(Collections.singleton(collectionNoChange.entity))
            .ihInstitution(collectionNoChange.ih)
            .build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IH_SYNC.handleCollectionMatch(match);
    assertTrue(collectionOnlyMatch.getMatchedCollection().isUpdate());
  }

  @Test
  public void identifierAndTagUpdateInInstitutionTest() {
    TestEntity<Institution, IHInstitution> institutionNoChange = createInstitutionNoChange();
    institutionNoChange.getEntity().setIdentifiers(Collections.emptyList());
    MatchResult match =
        MatchResult.builder()
            .institutions(Collections.singleton(institutionNoChange.entity))
            .ihInstitution(institutionNoChange.ih)
            .build();

    SyncResult.InstitutionOnlyMatch institutionOnlyMatch = IH_SYNC.handleInstitutionMatch(match);
    assertTrue(institutionOnlyMatch.getMatchedInstitution().isUpdate());
  }

  private <T extends CollectionEntity & LenientEquals<T>, R extends IHEntity>
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

  private void assertEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
  }

  private TestEntity<Institution, IHInstitution> createInstitutionNoChange() {
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

  private IHInstitution createIHInstitution() {
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

  private TestEntity<Institution, IHInstitution> createInstitutionToUpdate() {
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

  private TestEntity<Collection, IHInstitution> createCollectionNoChange() {
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

  private TestEntity<Collection, IHInstitution> createCollectionToUpdate() {
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

  private TestEntity<Person, IHStaff> createTestStaffToUpdate() {
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

  private TestEntity<Person, IHStaff> createTestStaffNoChange() {
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

  private TestEntity<Person, IHStaff> createTestStaffToRemove() {
    Person p = new Person();
    p.setKey(UUID.randomUUID());
    p.setFirstName("extra person");

    IHStaff ihStaff = new IHStaff();
    ihStaff.setIrn(IRN_TEST);

    return TestEntity.<Person, IHStaff>builder().entity(p).ih(ihStaff).build();
  }

  private TestEntity<Person, IHStaff> createTestStaffToCreate() {
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

  private Map<String, Integer> convertCollectionSummary(IHInstitution.CollectionSummary summary) {
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

  @Builder
  @Data
  private static class TestEntity<T extends CollectionEntity, R extends IHEntity> {
    T entity;
    T expected;
    R ih;
  }
}
