package org.gbif.collections.sync.ih.match;

import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.UserId;
import org.gbif.api.vocabulary.collections.IdType;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.ih.BaseIHTest;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.IHEntityConverter;
import org.gbif.collections.sync.ih.TestDataLoader;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IHStaffMatchResultHandlerTest extends BaseIHTest {

  private final DataLoader<IHData> dataLoader =
      TestDataLoader.builder().countries(COUNTRIES).build();
  IHProxyClient proxyClient =
      IHProxyClient.builder().dataLoader(dataLoader).ihConfig(ihConfig).build();
  private final IHStaffMatchResultHandler staffMatchResultHandler =
      IHStaffMatchResultHandler.builder()
          .proxyClient(proxyClient)
          .entityConverter(IHEntityConverter.create(countryParser))
          .ihConfig(ihConfig)
          .build();

  @Test
  public void contactUpdateTest() {
    TestEntity<Contact, IHStaff> contactToUpdate = createTestContactToUpdate();
    IHMatchResult match = IHMatchResult.builder().ihStaff(contactToUpdate.getIh()).build();

    Institution institution = new Institution();
    institution.setContactPersons(Collections.singletonList(contactToUpdate.getEntity()));

    SyncResult.ContactMatch contactMatch = staffMatchResultHandler.handleStaff(match, institution);

    assertEquals(0, contactMatch.getNewContacts().size());
    assertEquals(0, contactMatch.getRemovedContacts().size());
    assertEquals(0, contactMatch.getConflicts().size());
    assertEquals(1, contactMatch.getMatchedContacts().size());
    assertEntityMatch(contactMatch.getMatchedContacts().iterator().next(), contactToUpdate, true);
  }

  @Test
  public void contactNoChangeTest() {
    TestEntity<Contact, IHStaff> contactNoChange = createTestContactNoChange();
    IHMatchResult match = IHMatchResult.builder().ihStaff(contactNoChange.getIh()).build();

    Institution institution = new Institution();
    institution.setContactPersons(Collections.singletonList(contactNoChange.getEntity()));

    SyncResult.ContactMatch contactMatch = staffMatchResultHandler.handleStaff(match, institution);

    assertEquals(0, contactMatch.getNewContacts().size());
    assertEquals(0, contactMatch.getRemovedContacts().size());
    assertEquals(0, contactMatch.getConflicts().size());
    assertEquals(1, contactMatch.getMatchedContacts().size());
    assertEntityMatch(contactMatch.getMatchedContacts().iterator().next(), contactNoChange, false);
  }

  @Test
  public void contactToRemoveTest() {
    TestEntity<Contact, IHStaff> contactToRemove = createTestContactToRemove();
    IHMatchResult match = IHMatchResult.builder().build();

    Institution institution = new Institution();
    institution.setContactPersons(Collections.singletonList(contactToRemove.getEntity()));

    SyncResult.ContactMatch contactMatch = staffMatchResultHandler.handleStaff(match, institution);

    assertEquals(0, contactMatch.getNewContacts().size());
    assertEquals(1, contactMatch.getRemovedContacts().size());
    assertEquals(0, contactMatch.getConflicts().size());
    assertEquals(0, contactMatch.getMatchedContacts().size());
  }

  @Test
  public void contactToCreateTest() {
    TestEntity<Contact, IHStaff> personsToCreate = createTestContactToCreate();
    IHMatchResult match = IHMatchResult.builder().ihStaff(personsToCreate.getIh()).build();

    SyncResult.ContactMatch contactMatch =
        staffMatchResultHandler.handleStaff(match, new Institution());

    assertEquals(1, contactMatch.getNewContacts().size());
    assertEquals(0, contactMatch.getRemovedContacts().size());
    assertEquals(0, contactMatch.getConflicts().size());
    assertEquals(0, contactMatch.getMatchedContacts().size());
  }

  @Test
  public void contactConflictTest() {
    IHStaff ihStaff = new IHStaff();
    ihStaff.setIrn(IRN_TEST);
    ihStaff.setFirstName("foo");
    ihStaff.setCurrentStatus("Active");

    Contact c1 = new Contact();
    c1.setKey(1);
    c1.setFirstName("c1");
    c1.getUserIds().add(new UserId(IdType.IH_IRN, IRN_TEST));

    Contact c2 = new Contact();
    c2.setKey(2);
    c2.setFirstName("c2");
    c2.getUserIds().add(new UserId(IdType.IH_IRN, IRN_TEST));

    Institution institution = new Institution();
    institution.setContactPersons(Arrays.asList(c1, c2));

    IHMatchResult match = IHMatchResult.builder().ihStaff(ihStaff).build();

    SyncResult.ContactMatch contactMatch = staffMatchResultHandler.handleStaff(match, institution);
    assertEquals(0, contactMatch.getNewContacts().size());
    assertEquals(0, contactMatch.getRemovedContacts().size());
    assertEquals(1, contactMatch.getConflicts().size());
    assertEquals(0, contactMatch.getMatchedContacts().size());
    assertNotNull(contactMatch.getConflicts().iterator().next().getEntity());
    assertEquals(2, contactMatch.getConflicts().iterator().next().getGrSciCollEntities().size());
  }

  @Test
  public void invalidStaffTest() {
    IHStaff ihStaff = new IHStaff();
    ihStaff.setFirstName("first");
    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setEmail("asfsf");
    ihStaff.setContact(contact);

    assertTrue(IHStaffMatchResultHandler.isInvalidIhStaff(ihStaff));
    contact.setEmail(null);
    assertFalse(IHStaffMatchResultHandler.isInvalidIhStaff(ihStaff));
    ihStaff.setFirstName(null);
    assertTrue(IHStaffMatchResultHandler.isInvalidIhStaff(ihStaff));

    ihStaff.setFirstName("aasdsa");
    contact.setEmail("aa@aa.com;bb@bb.es");
    assertFalse(IHStaffMatchResultHandler.isInvalidIhStaff(ihStaff));

    contact.setEmail("aa@aa.com;bb@bb.es;aaa");
    assertTrue(IHStaffMatchResultHandler.isInvalidIhStaff(ihStaff));
  }
}
