package org.gbif.collections.sync.ih.match;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.ih.BaseIHTest;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.IHEntityConverter;
import org.gbif.collections.sync.ih.TestDataLoader;
import org.gbif.collections.sync.ih.model.IHStaff;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
  public void staffUpdateTest() {
    TestEntity<Person, IHStaff> personToUpdate = createTestStaffToUpdate();
    IHMatchResult match =
        IHMatchResult.builder()
            .ihStaff(personToUpdate.getIh())
            .staffMatcher((s, ppl) -> Collections.singleton(personToUpdate.getEntity()))
            .build();

    Institution institution = new Institution();
    institution.setContacts(Collections.singletonList(personToUpdate.getEntity()));

    SyncResult.StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(match, Collections.singletonList(institution));

    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
    assertEquals(1, staffMatch.getMatchedPersons().size());
    assertEntityMatch(staffMatch.getMatchedPersons().iterator().next(), personToUpdate, true);
  }

  @Test
  public void staffNoChangeTest() {
    TestEntity<Person, IHStaff> personNoChange = createTestStaffNoChange();
    IHMatchResult match =
        IHMatchResult.builder()
            .ihStaff(personNoChange.getIh())
            .staffMatcher((s, ppl) -> Collections.singleton(personNoChange.getEntity()))
            .build();

    Institution institution = new Institution();
    institution.setContacts(Collections.singletonList(personNoChange.getEntity()));

    SyncResult.StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(match, Collections.singletonList(institution));

    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
    assertEquals(1, staffMatch.getMatchedPersons().size());
    assertEntityMatch(staffMatch.getMatchedPersons().iterator().next(), personNoChange, false);
  }

  @Test
  public void staffToRemoveTest() {
    TestEntity<Person, IHStaff> personToRemove = createTestStaffToRemove();
    IHMatchResult match = IHMatchResult.builder().build();

    Institution institution = new Institution();
    institution.setContacts(Collections.singletonList(personToRemove.getEntity()));

    SyncResult.StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(match, Collections.singletonList(institution));

    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(1, staffMatch.getRemovedPersons().size());
    assertEquals(0, staffMatch.getConflicts().size());
    assertEquals(0, staffMatch.getMatchedPersons().size());
  }

  @Test
  public void staffToCreateTest() {
    TestEntity<Person, IHStaff> personsToCreate = createTestStaffToCreate();
    IHMatchResult match =
        IHMatchResult.builder()
            .ihStaff(personsToCreate.getIh())
            .staffMatcher((s, ppl) -> Collections.emptySet())
            .build();

    SyncResult.StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(match, Collections.emptyList());

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

    IHMatchResult match =
        IHMatchResult.builder()
            .ihStaff(ihStaff)
            .staffMatcher((s, ppl) -> new HashSet<>(Arrays.asList(p1, p2)))
            .build();

    SyncResult.StaffMatch staffMatch =
        staffMatchResultHandler.handleStaff(match, Collections.emptyList());
    assertEquals(0, staffMatch.getNewPersons().size());
    assertEquals(0, staffMatch.getRemovedPersons().size());
    assertEquals(1, staffMatch.getConflicts().size());
    assertEquals(0, staffMatch.getMatchedPersons().size());
    assertNotNull(staffMatch.getConflicts().iterator().next().getEntity());
    assertEquals(2, staffMatch.getConflicts().iterator().next().getGrSciCollEntities().size());
  }
}
