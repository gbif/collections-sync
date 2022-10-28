package org.gbif.collections.sync.idigbio.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.idigbio.BaseIDigBioTest;

import java.util.ArrayList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link Matcher}. */
public class MatcherTest extends BaseIDigBioTest {

  @Test
  public void stringSimilarityTest() {
    assertTrue(Matcher.stringSimilarity("test phrase", "test") > 0);
    assertTrue(Matcher.stringSimilarity("test phrase", "test  PHrase  ") > 0);
    assertTrue(Matcher.stringSimilarity("test phrase", "test other") == 0);
    assertTrue(Matcher.stringSimilarity("test phrases", "my phrase") > 0);
    assertTrue(Matcher.stringSimilarity("the test phrase", "the other thing") == 0);
  }

  @Test
  public void countIdentifierMatchesTest() {
    Collection collection = new Collection();
    collection.getIdentifiers().add(new Identifier(IdentifierType.LSID, "other"));

    assertEquals(0, Matcher.countIdentifierMatches("lsid:001", collection));
    collection
        .getIdentifiers()
        .add(new Identifier(IdentifierType.GRSCICOLL_URI, "http://test.com/lsid:001"));
    assertEquals(1, Matcher.countIdentifierMatches("lsid:001", collection));

    collection.setIdentifiers(new ArrayList<>());
    collection.getIdentifiers().add(new Identifier(IdentifierType.LSID, "lsid:001"));
    assertEquals(1, Matcher.countIdentifierMatches("http://test.com/lsid:001", collection));
  }
}
