package org.gbif.collections.sync.ih;

import org.gbif.api.vocabulary.Country;
import org.gbif.collections.sync.http.clients.IHHttpClient;
import org.gbif.collections.sync.ih.model.IHInstitution;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link EntityConverter}.
 *
 * <p>Most of the methods are tested in {@link IHSyncTest} so no need to test them here again.
 */
public class EntityConverterTest {

  private static final EntityConverter ENTITY_CONVERTER =
      EntityConverter.builder()
          .countries(Arrays.asList("U.K.", "U.S.A.", "United Kingdom", "United States"))
          .creationUser("test-user")
          .build();

  @Test
  public void parseHomepageUrlTest() {
    IHInstitution ih = new IHInstitution();
    IHInstitution.Contact contact = new IHInstitution.Contact();
    contact.setWebUrl("http://www.  a.co m");
    ih.setContact(contact);

    assertEquals(URI.create("http://www.a.com"), EntityConverter.getIhHomepage(ih).get());

    contact.setWebUrl("www.b.com;http://www.  a.co m");
    assertEquals(URI.create("http://www.b.com"), EntityConverter.getIhHomepage(ih).get());

    contact.setWebUrl("www.b.com\nhttp://www.  a.co m");
    assertEquals(URI.create("http://www.b.com"), EntityConverter.getIhHomepage(ih).get());
  }

  @Test
  public void emailValidityTest() {
    assertTrue(EntityConverter.isValidEmail("a@a.c"));
    assertFalse(EntityConverter.isValidEmail("aa.c"));
    assertFalse(EntityConverter.isValidEmail("N/A"));
    assertFalse(EntityConverter.isValidEmail("@a.c"));
  }

  @Test
  public void phoneValidityTest() {
    assertTrue(EntityConverter.isValidPhone("[]132435"));
    assertFalse(EntityConverter.isValidPhone("12"));
    assertFalse(EntityConverter.isValidPhone("[][][][]"));
  }

  @Test
  public void faxValidityTest() {
    assertTrue(EntityConverter.isValidFax("[]132435"));
    assertFalse(EntityConverter.isValidFax("12"));
    assertFalse(EntityConverter.isValidFax("[][][][]"));
  }

  @Test
  public void parseDateTest() {
    assertTrue(EntityConverter.parseDate("2019").isPresent());
    assertTrue(EntityConverter.parseDate("2019.").isPresent());
    assertTrue(EntityConverter.parseDate("2019-08-08").isPresent());
    assertTrue(EntityConverter.parseDate("2019-08").isPresent());
    assertTrue(EntityConverter.parseDate("12/01/2019").isPresent());
    assertTrue(EntityConverter.parseDate("June 2019").isPresent());
    assertTrue(EntityConverter.parseDate("Junio 2019").isPresent());
  }


  @Ignore("Manual test")
  @Test
  public void countryMappingTest() {
    IHHttpClient ihHttpClient =
        IHHttpClient.getInstance("http://sweetgum.nybg.org/science/api/v1/");
    List<String> countries = ihHttpClient.getCountries();

    Map<String, Country> mappings = EntityConverter.mapCountries(countries);

    assertTrue(mappings.size() >= countries.size());
  }
}
