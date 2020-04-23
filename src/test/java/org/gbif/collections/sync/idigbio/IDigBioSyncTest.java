package org.gbif.collections.sync.idigbio;

import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.idigbio.match.MatchResult;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests the {@link IDigBioSync}. */
public class IDigBioSyncTest {

  private static final IDigBioSync IDIGBIO_SYNC = IDigBioSync.builder().build();

  @Test
  public void collectionToUpdateTest() {
    Collection collection = new Collection();
    collection.setCode("code");
    collection.setName("name");

    IDigBioRecord record = new IDigBioRecord();
    record.setCollectionCode("code2");

    MatchResult match =
        MatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IDIGBIO_SYNC.handleCollectionMatch(match);
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
    Collection collection = new Collection();
    collection.setCode("code");
    collection.setName("name");

    IDigBioRecord record = new IDigBioRecord();
    record.setCollectionCode(collection.getCode());
    record.setCollection(collection.getName());

    MatchResult match =
        MatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = IDIGBIO_SYNC.handleCollectionMatch(match);
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
    Institution institution = new Institution();
    institution.setCode("code");
    institution.setName("name");

    IDigBioRecord record = new IDigBioRecord();
    record.setInstitutionCode("code2");
    record.setInstitution(institution.getName());

    MatchResult match =
        MatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();

    InstitutionOnlyMatch institutionOnlyMatch = IDIGBIO_SYNC.handleInstitutionMatch(match);
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
    Institution institution = new Institution();
    institution.setCode("code");
    institution.setName("name");

    IDigBioRecord record = new IDigBioRecord();
    record.setInstitutionCode(institution.getCode());
    record.setInstitution(institution.getName());

    MatchResult match =
        MatchResult.builder().institutionMatched(institution).iDigBioRecord(record).build();

    InstitutionOnlyMatch institutionOnlyMatch = IDIGBIO_SYNC.handleInstitutionMatch(match);
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
    // iDigBio record
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setInstitution("inst");
    iDigBioRecord.setInstitutionCode("instCode");
    iDigBioRecord.setCollectionCode("collCode");

    // expected institution
    Institution expectedInstitution = new Institution();
    expectedInstitution.setCode(iDigBioRecord.getInstitutionCode());
    expectedInstitution.setName(iDigBioRecord.getInstitution());

    // expected collection
    Collection expectedCollection = new Collection();
    expectedCollection.setCode(iDigBioRecord.getCollectionCode());
    expectedCollection.setName(iDigBioRecord.getInstitution());

    MatchResult match = MatchResult.builder().iDigBioRecord(iDigBioRecord).build();
    SyncResult.NoEntityMatch noEntityMatch = IDIGBIO_SYNC.handleNoMatches(match);
    assertTrue(noEntityMatch.getNewCollection().lenientEquals(expectedCollection));
    assertTrue(noEntityMatch.getNewInstitution().lenientEquals(expectedInstitution));
    assertEmptyStaffMatch(noEntityMatch.getStaffMatch());
  }

  @Test
  public void institutionAndCollectionMatchTest() {
    // iDigBio record
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setInstitution("inst");
    iDigBioRecord.setInstitutionCode("instCode");
    iDigBioRecord.setCollectionCode("collCode");

    // institution
    Institution inst = new Institution();
    inst.setCode(iDigBioRecord.getInstitutionCode());
    inst.setName(iDigBioRecord.getInstitution());

    // collection
    Collection coll = new Collection();
    coll.setCode(iDigBioRecord.getCollectionCode());
    coll.setName("foo");

    MatchResult match =
        MatchResult.builder()
            .collectionMatched(coll)
            .institutionMatched(inst)
            .iDigBioRecord(iDigBioRecord)
            .build();

    SyncResult.InstitutionAndCollectionMatch instAndColMatch =
        IDIGBIO_SYNC.handleInstitutionAndCollectionMatch(match);
    assertFalse(instAndColMatch.getMatchedInstitution().isUpdate());
    assertTrue(instAndColMatch.getMatchedCollection().isUpdate());
    assertEmptyStaffMatch(instAndColMatch.getStaffMatch());
  }

  @Test
  public void handleConflictTest() {
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

    MatchResult match =
        MatchResult.builder()
            .iDigBioRecord(iDigBioRecord)
            .institutionMatched(inst)
            .collectionMatched(c1)
            .build();

    SyncResult.Conflict conflictMatch = IDIGBIO_SYNC.handleConflict(match);
    assertNotNull(conflictMatch.getEntity());
    assertEquals(2, conflictMatch.getGrSciCollEntities().size());
  }

  private void assertEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
  }
}
