//package org.gbif.collections.sync.idigbio;
//
//import java.util.UUID;
//
//import org.gbif.api.model.collections.Collection;
//import org.gbif.api.model.collections.Institution;
//import org.gbif.collections.sync.SyncResult;
//import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
//import org.gbif.collections.sync.idigbio.match.MatchResult;
//
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
//
///** Tests the {@link IDigBioSync}. */
//public class IDigBioSyncTest {
//
//  private static final IDigBioSync IDIGBIO_SYNC = IDigBioSync.builder().build();
//
//  @Test
//  public void collectionToUpdateTest() {
//    Collection collection = new Collection();
//    collection.setCode("code");
//    collection.setName("name");
//
//    IDigBioRecord record = new IDigBioRecord();
//    record.setCollectionCode("code2");
//
//    MatchResult match =
//        MatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();
//
//    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IDIGBIO_SYNC.handleCollectionMatch(match);
//    assertTrue(collectionOnlyMatch.getMatchedCollection().isUpdate());
//    assertFalse(
//        collectionOnlyMatch
//            .getMatchedCollection()
//            .getMerged()
//            .lenientEquals(collectionOnlyMatch.getMatchedCollection().getMatched()));
//    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
//  }
//
//  @Test
//  public void collectionNoChangeTest() {
//    Collection collection = new Collection();
//    collection.setCode("code");
//    collection.setName("name");
//    collection.setActive(true);
//
//    IDigBioRecord record = new IDigBioRecord();
//    record.setCollectionCode(collection.getCode());
//    record.setCollection(collection.getName());
//
//    MatchResult match =
//        MatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();
//
//    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IDIGBIO_SYNC.handleCollectionMatch(match);
//    assertFalse(collectionOnlyMatch.getMatchedCollection().isUpdate());
//    assertTrue(
//        collectionOnlyMatch
//            .getMatchedCollection()
//            .getMerged()
//            .lenientEquals(collectionOnlyMatch.getMatchedCollection().getMatched()));
//    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
//  }
//
//  @Test
//  public void institutionToUpdateTest() {
//    Institution institution = new Institution();
//    institution.setCode("code");
//    institution.setName("name");
//
//    IDigBioRecord record = new IDigBioRecord();
//    record.setInstitutionCode("code2");
//    record.setInstitution(institution.getName());
//
//    MatchResult match =
//        MatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();
//
//    InstitutionOnlyMatch institutionOnlyMatch = IDIGBIO_SYNC.handleInstitutionMatch(match);
//    assertTrue(institutionOnlyMatch.getMatchedInstitution().isUpdate());
//    assertNotNull(institutionOnlyMatch.getNewCollection());
//    assertFalse(
//        institutionOnlyMatch
//            .getMatchedInstitution()
//            .getMerged()
//            .lenientEquals(institutionOnlyMatch.getMatchedInstitution().getMatched()));
//    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
//  }
//
//  @Test
//  public void institutionNoChangeTest() {
//    Institution institution = new Institution();
//    institution.setCode("code");
//    institution.setName("name");
//    institution.setActive(true);
//
//    IDigBioRecord record = new IDigBioRecord();
//    record.setInstitutionCode(institution.getCode());
//    record.setInstitution(institution.getName());
//
//    MatchResult match =
//        MatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();
//
//    InstitutionOnlyMatch institutionOnlyMatch = IDIGBIO_SYNC.handleInstitutionMatch(match);
//    assertFalse(institutionOnlyMatch.getMatchedInstitution().isUpdate());
//    assertNotNull(institutionOnlyMatch.getNewCollection());
//    assertTrue(
//        institutionOnlyMatch
//            .getMatchedInstitution()
//            .getMerged()
//            .lenientEquals(institutionOnlyMatch.getMatchedInstitution().getMatched()));
//    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
//  }
//
//  @Test
//  public void noMatchTest() {
//    // iDigBio record
//    IDigBioRecord iDigBioRecord = new IDigBioRecord();
//    iDigBioRecord.setInstitution("inst");
//    iDigBioRecord.setInstitutionCode("instCode");
//    iDigBioRecord.setCollectionCode("collCode");
//
//    // expected institution
//    Institution expectedInstitution = new Institution();
//    expectedInstitution.setCode(iDigBioRecord.getInstitutionCode());
//    expectedInstitution.setName(iDigBioRecord.getInstitution());
//    expectedInstitution.setActive(true);
//
//    // expected collection
//    Collection expectedCollection = new Collection();
//    expectedCollection.setCode(iDigBioRecord.getCollectionCode());
//    expectedCollection.setName(iDigBioRecord.getInstitution());
//    expectedCollection.setActive(true);
//
//    MatchResult match = MatchResult.builder().iDigBioRecord(iDigBioRecord).build();
//    SyncResult.NoEntityMatch noEntityMatch = IDIGBIO_SYNC.handleNoMatches(match);
//    assertTrue(noEntityMatch.getNewCollection().lenientEquals(expectedCollection));
//    assertTrue(noEntityMatch.getNewInstitution().lenientEquals(expectedInstitution));
//    assertEmptyStaffMatch(noEntityMatch.getStaffMatch());
//  }
//
//  @Test
//  public void institutionAndCollectionMatchTest() {
//    // iDigBio record
//    IDigBioRecord iDigBioRecord = new IDigBioRecord();
//    iDigBioRecord.setInstitution("inst");
//    iDigBioRecord.setInstitutionCode("instCode");
//    iDigBioRecord.setCollectionCode("collCode");
//
//    // institution
//    Institution inst = new Institution();
//    inst.setCode(iDigBioRecord.getInstitutionCode());
//    inst.setName(iDigBioRecord.getInstitution());
//    inst.setActive(true);
//
//    // collection
//    Collection coll = new Collection();
//    coll.setCode(iDigBioRecord.getCollectionCode());
//    coll.setActive(true);
//
//    MatchResult match =
//        MatchResult.builder()
//            .collectionMatched(coll)
//            .institutionMatched(inst)
//            .iDigBioRecord(iDigBioRecord)
//            .build();
//
//    SyncResult.InstitutionAndCollectionMatch instAndColMatch =
//        IDIGBIO_SYNC.handleInstitutionAndCollectionMatch(match);
//    assertFalse(instAndColMatch.getMatchedInstitution().isUpdate());
//    assertTrue(instAndColMatch.getMatchedCollection().isUpdate());
//    assertEquals(
//        iDigBioRecord.getInstitution(),
//        instAndColMatch.getMatchedCollection().getMerged().getName());
//    assertEmptyStaffMatch(instAndColMatch.getStaffMatch());
//  }
//
//  @Test
//  public void handleConflictTest() {
//    // iDigBio record
//    IDigBioRecord iDigBioRecord = new IDigBioRecord();
//    iDigBioRecord.setInstitution("inst");
//    iDigBioRecord.setInstitutionCode("instCode");
//    iDigBioRecord.setCollectionCode("collCode");
//    iDigBioRecord.setCollectionUuid(UUID.randomUUID().toString());
//
//    Institution inst = new Institution();
//    inst.setKey(UUID.randomUUID());
//
//    Collection c1 = new Collection();
//    c1.setKey(UUID.randomUUID());
//
//    MatchResult match =
//        MatchResult.builder()
//            .iDigBioRecord(iDigBioRecord)
//            .institutionMatched(inst)
//            .collectionMatched(c1)
//            .build();
//
//    SyncResult.Conflict conflictMatch = IDIGBIO_SYNC.handleConflict(match);
//    assertNotNull(conflictMatch.getEntity());
//    assertEquals(2, conflictMatch.getGrSciCollEntities().size());
//  }
//
//  @Test
//  public void identifierAndTagUpdateInCollectionTest() {
//    Collection collection = new Collection();
//    collection.setCode("code");
//    collection.setName("name");
//    collection.setActive(true);
//
//    IDigBioRecord record = new IDigBioRecord();
//    record.setCollectionCode(collection.getCode());
//    record.setCollection(collection.getName());
//
//    MatchResult match =
//        MatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();
//
//    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IDIGBIO_SYNC.handleCollectionMatch(match);
//    assertFalse(collectionOnlyMatch.getMatchedCollection().isUpdate());
//
//    // add uuid to create identifier
//    record.setCollectionUuid("uuid");
//    collectionOnlyMatch = IDIGBIO_SYNC.handleCollectionMatch(match);
//    assertTrue(collectionOnlyMatch.getMatchedCollection().isUpdate());
//
//    assertTrue(
//        collectionOnlyMatch
//            .getMatchedCollection()
//            .getMerged()
//            .lenientEquals(collectionOnlyMatch.getMatchedCollection().getMatched()));
//    assertFalse(collectionOnlyMatch.getMatchedCollection().getMerged().getIdentifiers().isEmpty());
//    assertFalse(collectionOnlyMatch.getMatchedCollection().getMerged().getMachineTags().isEmpty());
//    assertTrue(collectionOnlyMatch.getMatchedCollection().getMatched().getIdentifiers().isEmpty());
//    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
//  }
//
//  @Test
//  public void identifierAndTagUpdateInInstitutionTest() {
//    Institution institution = new Institution();
//    institution.setCode("code");
//    institution.setName("name");
//    institution.setActive(true);
//
//    IDigBioRecord record = new IDigBioRecord();
//    record.setInstitutionCode(institution.getCode());
//    record.setInstitution(institution.getName());
//
//    MatchResult match =
//        MatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();
//
//    InstitutionOnlyMatch institutionOnlyMatch = IDIGBIO_SYNC.handleInstitutionMatch(match);
//    assertFalse(institutionOnlyMatch.getMatchedInstitution().isUpdate());
//
//    // add uuid to create identifier
//    record.setUniqueNameUuid("uuid");
//    institutionOnlyMatch = IDIGBIO_SYNC.handleInstitutionMatch(match);
//    assertTrue(institutionOnlyMatch.getMatchedInstitution().isUpdate());
//    assertTrue(
//        institutionOnlyMatch
//            .getMatchedInstitution()
//            .getMerged()
//            .lenientEquals(institutionOnlyMatch.getMatchedInstitution().getMatched()));
//    assertFalse(
//        institutionOnlyMatch.getMatchedInstitution().getMerged().getIdentifiers().isEmpty());
//    assertFalse(
//        institutionOnlyMatch.getMatchedInstitution().getMerged().getMachineTags().isEmpty());
//    assertTrue(
//        institutionOnlyMatch.getMatchedInstitution().getMatched().getIdentifiers().isEmpty());
//    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
//  }
//
//  private void assertEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
//    assertEquals(0, staffMatch.getMatchedPersons().size());
//    assertEquals(0, staffMatch.getNewPersons().size());
//    assertEquals(0, staffMatch.getRemovedPersons().size());
//    assertEquals(0, staffMatch.getConflicts().size());
//  }
//}
