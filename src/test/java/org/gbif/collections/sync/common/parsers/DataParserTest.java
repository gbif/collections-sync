package org.gbif.collections.sync.common.parsers;

import java.net.URI;
import org.junit.Test;

import static org.gbif.collections.sync.common.parsers.DataParser.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the {@link org.gbif.collections.sync.common.parsers.DataParser}. */
public class DataParserTest {

  @Test
  public void emailValidityTest() {
    assertTrue(isValidEmail("a@ac.co"));
    assertFalse(isValidEmail("aa.c"));
    assertFalse(isValidEmail("N/A"));
    assertFalse(isValidEmail("@a.c"));
  }

  @Test
  public void phoneValidityTest() {
    assertTrue(isValidPhone("[]132435"));
    assertFalse(isValidPhone("12"));
    assertFalse(isValidPhone("[][][][]"));
  }

  @Test
  public void faxValidityTest() {
    assertTrue(isValidFax("[]132435"));
    assertFalse(isValidFax("12"));
    assertFalse(isValidFax("[][][][]"));
  }

  @Test
  public void parseDateTest() {
    assertEquals(2019, parseDateYear("2019").intValue());
    assertEquals(2019, parseDateYear("2019.").intValue());
    assertEquals(2019, parseDateYear("2019-08").intValue());
    assertEquals(2019, parseDateYear("2019-08-08").intValue());
    assertEquals(2019, parseDateYear("08/08/2019").intValue());
    assertEquals(2019, parseDateYear("June 2019").intValue());
    assertEquals(2019, parseDateYear("Junio 2019").intValue());
    assertEquals(2019, parseDateYear("1 January 2019").intValue());
  }

  @Test
  public void parseHomepageUrlTest() {
    assertEquals(URI.create("http://www.a.com"), parseUri("http://www.  a.co m").get());
    assertEquals(URI.create("http://www.b.com"), parseUri("www.b.com").get());
    assertEquals(URI.create("http://b.com"), parseUri("b.com").get());
    assertEquals(URI.create("http://abc.gov.dk/de/fg/"), parseUri("abc.gov.dk/de/fg/").get());
    assertEquals(URI.create("https://abc.com/de/fg/"), parseUri("https://abc.com/de/fg/").get());
    assertFalse(parseUri("na", ex -> {}).isPresent());
    assertFalse(parseUri("", ex -> {}).isPresent());
    assertFalse(parseUri(".com", ex -> {}).isPresent());
    assertFalse(parseUri("na.c", ex -> {}).isPresent());
    assertFalse(parseUri("httpwww.a.com", ex -> {}).isPresent());
  }

  @Test
  public void getFirstStringTest() {
    assertEquals("a", getFirstString("a;b").get());
    assertEquals("a", getFirstString("a\nb").get());
    assertEquals("a", getFirstString("a,b").get());
  }
}
