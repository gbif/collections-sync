package org.gbif.collections.sync.idigbio.match;

import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.idigbio.IDigBioRecord;

import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests the {@link Matcher}. */
public class MatcherTest {

  @Test
  public void matchContactTest() {
    IDigBioRecord iDigBioRecord = new IDigBioRecord();
    iDigBioRecord.setContact("name");
    iDigBioRecord.setContactEmail("aa@aa.com");
    iDigBioRecord.setContactRole("role");

    Person existing = new Person();
    existing.setFirstName("first");
    existing.setPosition("pos");
    existing.setPhone("123456");

    assertEquals(0, Matcher.matchContact(iDigBioRecord, Collections.singleton(existing)).size());

    existing.setFirstName(iDigBioRecord.getContact());
    existing.setEmail(iDigBioRecord.getContactEmail());
    existing.setPosition(iDigBioRecord.getContactRole());
    assertEquals(1, Matcher.matchContact(iDigBioRecord, Collections.singleton(existing)).size());
  }
}
