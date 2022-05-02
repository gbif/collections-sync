package org.gbif.collections.sync.common.parsers;

import java.net.URI;
import java.util.Calendar;
import java.util.Date;

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
    Date date = parseDate("2019");
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = parseDate("2019.");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = parseDate("2019-08");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(7, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = parseDate("2019-08-08");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(7, cal.get(Calendar.MONTH));
    assertEquals(8, cal.get(Calendar.DAY_OF_MONTH));

    date = parseDate("08/08/2019");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(7, cal.get(Calendar.MONTH));
    assertEquals(8, cal.get(Calendar.DAY_OF_MONTH));

    date = parseDate("June 2019");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(5, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = parseDate("Junio 2019");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(5, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = parseDate("1 January 2019");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  public void parseHomepageUrlTest() {
    assertEquals(URI.create("http://www.a.com"), parseUri("http://www.  a.co m").get());
    assertEquals(URI.create("http://www.b.com"), parseUri("www.b.com").get());
  }

  @Test
  public void getFirstStringTest() {
    assertEquals("a", getFirstString("a;b").get());
    assertEquals("a", getFirstString("a\nb").get());
    assertEquals("a", getFirstString("a,b").get());
  }
}
