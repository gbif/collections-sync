package org.gbif.collections.sync.idigbio;

import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.idigbio.match.IDigBioMatchResult;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import org.junit.Test;

import static org.gbif.collections.sync.TestUtils.assertEmptyStaffMatch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IDigBioSynchronizerHandlersTest extends BaseIDigBioTest {

  private final IDigBioSynchronizer synchronizer =
      IDigBioSynchronizer.builder()
          .dataLoader(TestDataLoader.builder().build())
          .iDigBioConfig(iDigBioConfig)
          .build();

  @Test
  public void collectionToUpdateTest() {
    // State
    Collection collection = new Collection();
    collection.setCode("code");
    collection.setName("name");

    IDigBioRecord record = new IDigBioRecord();
    record.setCollectionCode("code2");

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();

    // When
    SyncResult.CollectionOnlyMatch collectionOnlyMatch = synchronizer.handleCollectionMatch(match);

    // Should
    assertTrue(collectionOnlyMatch.getMatchedCollection().isUpdate());
    assertFalse(
        collectionOnlyMatch
            .getMatchedCollection()
            .getMerged()
            .lenientEquals(collectionOnlyMatch.getMatchedCollection().getMatched()));
    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
  }

  @Test
  public void collectionNoChangeTest() {
    // State
    Collection collection = new Collection();
    collection.setCode("code");
    collection.setName("name");
    collection.setActive(true);

    IDigBioRecord record = new IDigBioRecord();
    record.setCollectionCode(collection.getCode());
    record.setCollection(collection.getName());

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();

    // When
    SyncResult.CollectionOnlyMatch collectionOnlyMatch = synchronizer.handleCollectionMatch(match);

    // Should
    assertFalse(collectionOnlyMatch.getMatchedCollection().isUpdate());
    assertTrue(
        collectionOnlyMatch
            .getMatchedCollection()
            .getMerged()
            .lenientEquals(collectionOnlyMatch.getMatchedCollection().getMatched()));
    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
  }

  @Test
  public void institutionToUpdateTest() {
    // State
    Institution institution = new Institution();
    institution.setCode("code");
    institution.setName("name");

    IDigBioRecord record = new IDigBioRecord();
    record.setInstitutionCode("code2");
    record.setInstitution(institution.getName());

    IDigBioMatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();

    // When
    InstitutionOnlyMatch institutionOnlyMatch = synchronizer.handleInstitutionMatch(match);

    // Should
    assertTrue(institutionOnlyMatch.getMatchedInstitution().isUpdate());
    assertNotNull(institutionOnlyMatch.getNewCollection());
    assertFalse(
        institutionOnlyMatch
            .getMatchedInstitution()
            .getMerged()
            .lenientEquals(institutionOnlyMatch.getMatchedInstitution().getMatched()));
    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
  }

  @Test
  public void institutionNoChangeTest() {
    // State
    Institution institution = new Institution();
    institution.setCode("code");
    institution.setName("name");
    institution.setActive(true);

    IDigBioRecord record = new IDigBioRecord();
    record.setInstitutionCode(institution.getCode());
    record.setInstitution(institution.getName());

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();

    // When
    InstitutionOnlyMatch institutionOnlyMatch = synchronizer.handleInstitutionMatch(match);

    // Should
    assertFalse(institutionOnlyMatch.getMatchedInstitution().isUpdate());
    assertNotNull(institutionOnlyMatch.getNewCollection());
    assertTrue(
        institutionOnlyMatch
            .getMatchedInstitution()
            .getMerged()
            .lenientEquals(institutionOnlyMatch.getMatchedInstitution().getMatched()));
    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
  }

  @Test
  public void noMatchTest() {
    // State
    // iDigBio record
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setInstitution("inst");
    iDigBioRecord.setInstitutionCode("instCode");
    iDigBioRecord.setCollectionCode("collCode");

    // expected institution
    Institution expectedInstitution = new Institution();
    expectedInstitution.setCode(iDigBioRecord.getInstitutionCode());
    expectedInstitution.setName(iDigBioRecord.getInstitution());
    expectedInstitution.setActive(true);

    // expected collection
    Collection expectedCollection = new Collection();
    expectedCollection.setCode(iDigBioRecord.getCollectionCode());
    expectedCollection.setName(iDigBioRecord.getInstitution());
    expectedCollection.setActive(true);

    IDigBioMatchResult match = IDigBioMatchResult.builder().iDigBioRecord(iDigBioRecord).build();

    // When
    SyncResult.NoEntityMatch noEntityMatch = synchronizer.handleNoMatch(match);

    // Should
    assertTrue(noEntityMatch.getNewCollection().lenientEquals(expectedCollection));
    assertTrue(noEntityMatch.getNewInstitution().lenientEquals(expectedInstitution));
    assertEmptyStaffMatch(noEntityMatch.getStaffMatch());
  }

  @Test
  public void institutionAndCollectionMatchTest() {
    // State
    // iDigBio record
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setInstitution("inst");
    iDigBioRecord.setInstitutionCode("instCode");
    iDigBioRecord.setCollectionCode("collCode");

    // institution
    Institution inst = new Institution();
    inst.setCode(iDigBioRecord.getInstitutionCode());
    inst.setName(iDigBioRecord.getInstitution());
    inst.setActive(true);

    // collection
    Collection coll = new Collection();
    coll.setCode(iDigBioRecord.getCollectionCode());
    coll.setActive(true);

    IDigBioMatchResult match =
        IDigBioMatchResult.builder()
            .collectionMatched(coll)
            .institutionMatched(inst)
            .iDigBioRecord(iDigBioRecord)
            .build();

    // When
    SyncResult.InstitutionAndCollectionMatch instAndColMatch =
        synchronizer.handleInstAndCollMatch(match);

    // Should
    assertFalse(instAndColMatch.getMatchedInstitution().isUpdate());
    assertTrue(instAndColMatch.getMatchedCollection().isUpdate());
    assertEquals(
        iDigBioRecord.getInstitution(),
        instAndColMatch.getMatchedCollection().getMerged().getName());
    assertEmptyStaffMatch(instAndColMatch.getStaffMatch());
  }

  @Test
  public void handleConflictTest() {
    // State
    // iDigBio record
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setInstitution("inst");
    iDigBioRecord.setInstitutionCode("instCode");
    iDigBioRecord.setCollectionCode("collCode");
    iDigBioRecord.setCollectionUuid(UUID.randomUUID().toString());

    Institution inst = new Institution();
    inst.setKey(UUID.randomUUID());

    Collection c1 = new Collection();
    c1.setKey(UUID.randomUUID());

    IDigBioMatchResult match =
        IDigBioMatchResult.builder()
            .iDigBioRecord(iDigBioRecord)
            .institutionMatched(inst)
            .collectionMatched(c1)
            .build();

    // When
    SyncResult.Conflict conflictMatch = synchronizer.handleConflict(match);

    // Should
    assertNotNull(conflictMatch.getEntity());
    assertEquals(2, conflictMatch.getGrSciCollEntities().size());
  }

  @Test
  public void identifierAndTagUpdateInCollectionTest() {
    Collection collection = new Collection();
    collection.setCode("code");
    collection.setName("name");
    collection.setActive(true);

    IDigBioRecord record = new IDigBioRecord();
    record.setCollectionCode(collection.getCode());
    record.setCollection(collection.getName());

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();
    SyncResult.CollectionOnlyMatch collectionOnlyMatch = synchronizer.handleCollectionMatch(match);
    assertFalse(collectionOnlyMatch.getMatchedCollection().isUpdate());

    // add uuid to create identifier
    record.setCollectionUuid("uuid");
    collectionOnlyMatch = synchronizer.handleCollectionMatch(match);
    assertTrue(collectionOnlyMatch.getMatchedCollection().isUpdate());

    assertTrue(
        collectionOnlyMatch
            .getMatchedCollection()
            .getMerged()
            .lenientEquals(collectionOnlyMatch.getMatchedCollection().getMatched()));

    assertFalse(collectionOnlyMatch.getMatchedCollection().getMerged().getIdentifiers().isEmpty());

    assertFalse(collectionOnlyMatch.getMatchedCollection().getMerged().getMachineTags().isEmpty());

    assertTrue(collectionOnlyMatch.getMatchedCollection().getMatched().getIdentifiers().isEmpty());
    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
  }

  @Test
  public void identifierAndTagUpdateInInstitutionTest() {
    Institution institution = new Institution();
    institution.setCode("code");
    institution.setName("name");
    institution.setActive(true);

    IDigBioRecord record = new IDigBioRecord();
    record.setInstitutionCode(institution.getCode());
    record.setInstitution(institution.getName());

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();

    InstitutionOnlyMatch institutionOnlyMatch = synchronizer.handleInstitutionMatch(match);
    assertFalse(institutionOnlyMatch.getMatchedInstitution().isUpdate());

    // add uuid to create identifier
    record.setUniqueNameUuid("uuid");
    institutionOnlyMatch = synchronizer.handleInstitutionMatch(match);
    assertTrue(institutionOnlyMatch.getMatchedInstitution().isUpdate());
    assertTrue(
        institutionOnlyMatch
            .getMatchedInstitution()
            .getMerged()
            .lenientEquals(institutionOnlyMatch.getMatchedInstitution().getMatched()));
    assertFalse(
        institutionOnlyMatch.getMatchedInstitution().getMerged().getIdentifiers().isEmpty());
    assertFalse(
        institutionOnlyMatch.getMatchedInstitution().getMerged().getMachineTags().isEmpty());
    assertTrue(
        institutionOnlyMatch.getMatchedInstitution().getMatched().getIdentifiers().isEmpty());
    assertEmptyStaffMatch(institutionOnlyMatch.getStaffMatch());
  }
}
