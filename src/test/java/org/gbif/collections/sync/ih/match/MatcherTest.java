package org.gbif.collections.sync.ih.match;

import java.util.Collections;
import java.util.Set;

import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.Person;
import org.gbif.api.vocabulary.Country;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.staff.StaffNormalized;
import org.gbif.collections.sync.ih.BaseIHTest;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.TestDataLoader;
import org.gbif.collections.sync.ih.model.IHStaff;

import org.junit.Test;

import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link Matcher}. */
public class MatcherTest extends BaseIHTest {

  private final DataLoader<IHData> dataLoader =
      TestDataLoader.builder().countries(COUNTRIES).build();
  IHProxyClient proxyClient =
      IHProxyClient.builder().dataLoader(dataLoader).ihConfig(ihConfig).build();
  private final Matcher matcher = Matcher.create(proxyClient);

  @Test
  public void matchWithFieldsTest() {
    // IH Staff
    IHStaff s = new IHStaff();
    s.setFirstName("First");
    s.setMiddleName("M.");
    s.setLastName("Last");
    s.setPosition("Manager");

    IHStaff.Address ihAddress = new IHStaff.Address();
    ihAddress.setStreet("");
    ihAddress.setCity("Fayetteville");
    ihAddress.setState("AR");
    ihAddress.setCountry("U.S.A.");
    ihAddress.setZipCode("72701");
    s.setAddress(ihAddress);

    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setPhone("[1] 479 575 4372");
    contact.setEmail("a@a.com");
    s.setContact(contact);

    // GrSciColl persons
    Person p1 = new Person();
    p1.setFirstName("First M.");
    p1.setEmail("a@a.com");

    Person p2 = new Person();
    p2.setPosition("Manager");
    Address address = new Address();
    address.setCountry(Country.UNITED_STATES);
    p2.setMailingAddress(address);

    // When
    Set<Person> persons =
        matcher.matchWithFields(
            new Matcher.IHStaffToMatch(s), Sets.newHashSet(p1, p2), Collections.emptySet(), 0);

    // Expect
    assertEquals(1, persons.size());
    assertTrue(persons.contains(p1));

    // GrSciColl persons
    p1 = new Person();
    p1.setFirstName("First");

    p2 = new Person();
    p2.setPosition("Manager");
    address = new Address();
    address.setCountry(Country.UNITED_STATES);
    p2.setMailingAddress(address);

    // When
    persons =
        matcher.matchWithFields(
            new Matcher.IHStaffToMatch(s), Sets.newHashSet(p1, p2), Collections.emptySet(), 0);

    // Expect
    assertEquals(1, persons.size());
    assertTrue(persons.contains(p1));

    // GrSciColl persons
    p1 = new Person();
    p1.setFirstName("Fir");
    p1.setPosition("Manager");

    p2 = new Person();
    p2.setLastName("Last");

    // When
    persons =
        matcher.matchWithFields(
            new Matcher.IHStaffToMatch(s), Sets.newHashSet(p1, p2), Collections.emptySet(), 0);

    // Expect
    assertEquals(1, persons.size());
    assertTrue(persons.contains(p1));
  }

  /** Based on a real case. */
  @Test
  public void partialMatchInNamesTest() {
    // IH Staff
    IHStaff s = new IHStaff();
    s.setFirstName("First Second");
    s.setLastName("Third");
    s.setPosition("Curator");

    IHStaff.Address ihAddress = new IHStaff.Address();
    ihAddress.setCity("city");
    ihAddress.setState("some Province");
    ihAddress.setCountry("U.S.A.");
    s.setAddress(ihAddress);

    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setPhone("123");
    contact.setEmail("b@b.com");
    s.setContact(contact);

    // GrSciColl persons
    Person p1 = new Person();
    p1.setFirstName("F. S. Third");
    p1.setPosition("Curator/Manager");
    p1.setPhone("456");
    p1.setEmail("a@a.com");

    Address address = new Address();
    address.setCity("CITY");
    address.setProvince("Some Province");
    address.setCountry(Country.UNITED_STATES);
    p1.setMailingAddress(address);

    // When
    Set<Person> persons =
        matcher.matchWithFields(
            new Matcher.IHStaffToMatch(s), Collections.singleton(p1), Collections.emptySet(), 11);

    // Expect
    assertTrue(persons.isEmpty());

    // When
    persons =
        matcher.matchWithFields(
            new Matcher.IHStaffToMatch(s), Collections.singleton(p1), Collections.emptySet(), 0);

    // Expect
    assertEquals(1, persons.size());
  }

  @Test
  public void corporateEmailTest() {
    // IH Staff
    IHStaff s = new IHStaff();
    s.setFirstName("First");
    s.setPosition("Curator");

    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setPhone("123");
    contact.setEmail("generic@a.com");
    s.setContact(contact);

    // GrSciColl persons
    Person p1 = new Person();
    p1.setFirstName("Another");
    p1.setPosition("Curator/Manager");
    p1.setPhone("456");
    p1.setEmail("generic@a.com");

    // When
    int score =
        Matcher.getEqualityScore(
            StaffNormalized.fromIHStaff(s, null, null, countryParser),
            StaffNormalized.fromGrSciCollPerson(p1),
            false);

    // Expect
    assertEquals(0, score);
  }

  @Test
  public void sameEmailPartialNameTest() {
    // IH Staff
    IHStaff s = new IHStaff();
    s.setFirstName("First");
    s.setLastName("Lastt");

    IHStaff.Contact contact = new IHStaff.Contact();
    contact.setEmail("a@a.com");
    s.setContact(contact);

    // GrSciColl persons
    Person p1 = new Person();
    p1.setFirstName("F. Lastt");
    p1.setEmail("a@a.com");

    // When
    int score =
        Matcher.getEqualityScore(
            StaffNormalized.fromIHStaff(s, null, null, countryParser),
            StaffNormalized.fromGrSciCollPerson(p1),
            false);

    // Expect
    assertTrue(score > 10);
  }
}
