package org.gbif.collections.sync.idigbio.match.strategy;

import org.gbif.api.model.collections.Collection;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;
import org.gbif.collections.sync.idigbio.match.MatchData;
import org.gbif.collections.sync.idigbio.match.IDigBioMatchResult;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectionMatchStrategyTest extends BaseMatchStrategyTest {

  @Test
  public void collectionToUpdateTest() {
    CollectionMatchStrategy strategy =
        CollectionMatchStrategy.builder()
            .matchData(new MatchData())
            .syncResultBuilder(SyncResult.builder())
            .build();

    Collection collection = new Collection();
    collection.setCode("code");
    collection.setName("name");

    IDigBioRecord record = new IDigBioRecord();
    record.setCollectionCode("code2");

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = strategy.handleAndReturn(match);
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
    CollectionMatchStrategy strategy =
        CollectionMatchStrategy.builder()
            .matchData(new MatchData())
            .syncResultBuilder(SyncResult.builder())
            .build();

    Collection collection = new Collection();
    collection.setCode("code");
    collection.setName("name");
    collection.setActive(true);

    IDigBioRecord record = new IDigBioRecord();
    record.setCollectionCode(collection.getCode());
    record.setCollection(collection.getName());

    IDigBioMatchResult match =
        IDigBioMatchResult.builder().collectionMatched(collection).iDigBioRecord(record).build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = strategy.handleAndReturn(match);
    assertFalse(collectionOnlyMatch.getMatchedCollection().isUpdate());
    assertTrue(
        collectionOnlyMatch
            .getMatchedCollection()
            .getMerged()
            .lenientEquals(collectionOnlyMatch.getMatchedCollection().getMatched()));
    assertEmptyStaffMatch(collectionOnlyMatch.getStaffMatch());
  }
}
