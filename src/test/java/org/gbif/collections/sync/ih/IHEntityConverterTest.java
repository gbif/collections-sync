package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.*;
import org.gbif.api.vocabulary.Country;
import org.gbif.collections.sync.common.converter.EntityConverter;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.gbif.collections.sync.common.parsers.DataParser.TO_BIGDECIMAL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link EntityConverter}.
 *
 * <p>This class is mostly tested in {@link IHSynchronizerTest} so this class doesn't cover all the
 * methods.
 */
public class IHEntityConverterTest {

  private static final String TEST_USER = "test-user";
  private static final CountryParser COUNTRY_PARSER =
      CountryParser.from(Arrays.asList("US", "U.K.", "U.S.A.", "United Kingdom", "United States"));
  private final IHEntityConverter entityConverter = IHEntityConverter.create(COUNTRY_PARSER);

  @Test
  public void getIHEmailsTest() {
    IHInstitution ih = new IHInstitution();
    IHInstitution.Contact contact = new IHInstitution.Contact();
    contact.setEmail("a@a.com\nb.com;c@c.com,d@d.com");
    ih.setContact(contact);

    List<String> emails = IHEntityConverter.getIhEmails(ih);
    assertEquals(3, emails.size());
    assertTrue(emails.contains("a@a.com"));
    assertTrue(emails.contains("c@c.com"));
    assertTrue(emails.contains("d@d.com"));
  }

  @Test
  public void getIHPhonesTest() {
    IHInstitution ih = new IHInstitution();
    IHInstitution.Contact contact = new IHInstitution.Contact();
    contact.setPhone("1;12345\n98765,34567");
    ih.setContact(contact);

    List<String> emails = IHEntityConverter.getIhPhones(ih);
    assertEquals(3, emails.size());
    assertTrue(emails.contains("12345"));
    assertTrue(emails.contains("98765"));
    assertTrue(emails.contains("34567"));
  }

  @Test
  public void getIHHomepageTest() {
    IHInstitution ih = new IHInstitution();
    IHInstitution.Contact contact = new IHInstitution.Contact();
    contact.setWebUrl("a  .com;b.com");
    ih.setContact(contact);

    URI uri = IHEntityConverter.getIhHomepage(ih);
    assertEquals(URI.create("http://a.com"), uri);
  }

  @Test
  public void setLocationTest() {
    // State
    IHInstitution ihInstitution = new IHInstitution();
    IHInstitution.Location location = new IHInstitution.Location();
    location.setLat(200.1);
    location.setLon(40.2);
    ihInstitution.setLocation(location);

    Institution institution = new Institution();
    institution.setLatitude(BigDecimal.valueOf(10.2));
    institution.setLongitude(BigDecimal.valueOf(100.2));

    // When
    IHEntityConverter.setLocation(ihInstitution, institution);

    // Expect
    assertEquals(institution.getLatitude(), institution.getLatitude());
    assertEquals(TO_BIGDECIMAL.apply(40.2), institution.getLongitude());

    // When
    location.setLat(20.2);
    IHEntityConverter.setLocation(ihInstitution, institution);

    // Expect
    assertEquals(TO_BIGDECIMAL.apply(20.2), institution.getLatitude());
    assertEquals(TO_BIGDECIMAL.apply(40.2), institution.getLongitude());

    // When
    location.setLat(null);
    IHEntityConverter.setLocation(ihInstitution, institution);

    // Expect
    assertNull(institution.getLatitude());
    assertEquals(TO_BIGDECIMAL.apply(40.2), institution.getLongitude());

    // When
    ihInstitution.setLocation(null);
    IHEntityConverter.setLocation(ihInstitution, institution);

    // Expect
    assertNull(institution.getLatitude());
    assertNull(institution.getLongitude());
  }

  @Test
  public void setAddressTest() {
    // State
    IHInstitution ihInstitution = new IHInstitution();
    IHInstitution.Address address = new IHInstitution.Address();
    address.setPhysicalCity("city1");
    address.setPhysicalStreet("addr1");
    address.setPostalCity("city2");
    address.setPostalStreet("addr2");
    ihInstitution.setAddress(address);

    Institution institution = new Institution();
    Address physicalAddress = new Address();
    physicalAddress.setAddress("physical addr");
    physicalAddress.setCity("physical city");
    physicalAddress.setPostalCode("123");
    institution.setAddress(physicalAddress);

    // When
    entityConverter.setAddress(institution, ihInstitution);

    // Expect
    assertEquals("city1", institution.getAddress().getCity());
    assertEquals("addr1", institution.getAddress().getAddress());
    assertNull(institution.getAddress().getPostalCode());
    assertNull(institution.getAddress().getCountry());
    assertNull(institution.getAddress().getProvince());

    assertEquals("city2", institution.getMailingAddress().getCity());
    assertEquals("addr2", institution.getMailingAddress().getAddress());
    assertNull(institution.getMailingAddress().getPostalCode());
    assertNull(institution.getMailingAddress().getCountry());
    assertNull(institution.getMailingAddress().getProvince());
  }

  @Test
  public void contactsTest() {
    IHStaff ihStaff = new IHStaff();
    ihStaff.setFirstName("first");
    ihStaff.setLastName("last");

    IHStaff.Contact ihContact = new IHStaff.Contact();
    ihContact.setEmail("bb@test.com");
    ihContact.setPhone("132435");
    ihContact.setFax("214324");
    ihStaff.setContact(ihContact);

    ihStaff.setIrn("11");
    ihStaff.setPosition("position");

    IHStaff.Address address = new IHStaff.Address();
    address.setCity("city");
    address.setCountry("US");
    address.setStreet("Street 1");
    address.setZipCode("1244");
    ihStaff.setAddress(address);

    Contact contact = entityConverter.convertToContact(ihStaff);
    assertEquals(ihStaff.getFirstName(), contact.getFirstName());
    assertEquals(ihStaff.getLastName(), contact.getLastName());

    assertTrue(contact.getEmail().contains(ihContact.getEmail()));
    assertTrue(contact.getPhone().contains(ihContact.getPhone()));
    assertTrue(contact.getFax().contains(ihContact.getFax()));
    assertTrue(contact.getUserIds().contains(new UserId(IdType.IH_IRN, ihStaff.getIrn())));
    assertTrue(contact.getPosition().contains(ihStaff.getPosition()));

    assertTrue(contact.getAddress().contains(address.getStreet()));
    assertEquals(address.getCity(), contact.getCity());
    assertEquals(address.getZipCode(), contact.getPostalCode());
    assertEquals(Country.UNITED_STATES, contact.getCountry());

    ihStaff.setFirstName("first name2");
    ihContact.setEmail("aaa@aaa.com");
    contact = entityConverter.convertToContact(ihStaff, contact);
    assertEquals(ihStaff.getFirstName(), contact.getFirstName());
    assertTrue(contact.getEmail().contains(ihContact.getEmail()));
    assertEquals(1, contact.getUserIds().size());
  }
}
