package org.gbif.collections.sync.ih.parsers;

import java.net.URI;

import org.junit.Test;

import static org.gbif.collections.sync.ih.parsers.IHParser.isValidEmail;
import static org.gbif.collections.sync.ih.parsers.IHParser.isValidFax;
import static org.gbif.collections.sync.ih.parsers.IHParser.isValidPhone;
import static org.gbif.collections.sync.ih.parsers.IHParser.parseDate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the {@link IHParser}. */
public class IHParserTest {

  @Test
  public void emailValidityTest() {
    assertTrue(isValidEmail("a@a.c"));
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
    assertTrue(parseDate("2019").isPresent());
    assertTrue(parseDate("2019.").isPresent());
    assertTrue(parseDate("2019-08-08").isPresent());
    assertTrue(parseDate("2019-08").isPresent());
    assertTrue(parseDate("12/01/2019").isPresent());
    assertTrue(parseDate("June 2019").isPresent());
    assertTrue(parseDate("Junio 2019").isPresent());
  }

  @Test
  public void parseHomepageUrlTest() {
    assertEquals(URI.create("http://www.a.com"), IHParser.parseUri("http://www.  a.co m").get());
    assertEquals(URI.create("http://www.b.com"), IHParser.parseUri("www.b.com").get());
  }

  @Test
  public void getFirstStringTest() {
    assertEquals("a", IHParser.getFirstString("a;b").get());
    assertEquals("a", IHParser.getFirstString("a\nb").get());
    assertEquals("a", IHParser.getFirstString("a,b").get());
  }
}
