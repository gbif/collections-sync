package org.gbif.collections.sync.idigbio;

import org.gbif.api.model.collections.Collection;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.idigbio.match.MatchResult;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IDigBioSyncTest {

  private static final IDigBioSync IDIGBIO_SYNC = IDigBioSync.builder().build();

  // TODO: add more tests

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
    record.setCollectionExtent(collection.getCode());
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

  private void assertEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
  }
}
