package org.gbif.collections.sync.idigbio.match.strategy;

import org.gbif.collections.sync.SyncResult;

import static org.junit.Assert.assertEquals;

abstract class BaseMatchStrategyTest {

  protected void assertEmptyStaffMatch(SyncResult.StaffMatch staffMatch) {
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
  }
}
