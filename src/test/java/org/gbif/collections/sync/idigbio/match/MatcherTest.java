package org.gbif.collections.sync.idigbio.match;

import java.util.ArrayList;
import java.util.Collections;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.clients.proxy.IDigBioProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.idigbio.BaseIDigBioTest;
import org.gbif.collections.sync.idigbio.IDigBioDataLoader.IDigBioData;
import org.gbif.collections.sync.idigbio.TestDataLoader;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the {@link Matcher}. */
public class MatcherTest extends BaseIDigBioTest {

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

    DataLoader<IDigBioData> dataLoader =
        TestDataLoader.builder()
            .persons(Collections.singletonList(existing))
            .institutions(Collections.emptyList())
            .collections(Collections.emptyList())
            .build();
    IDigBioProxyClient proxyClient =
        IDigBioProxyClient.builder().dataLoader(dataLoader).iDigBioConfig(iDigBioConfig).build();
    Matcher matcher = new Matcher(proxyClient);

    assertTrue(matcher.matchContact(iDigBioRecord, Collections.emptySet()).isEmpty());

    existing.setFirstName(iDigBioRecord.getContact());
    existing.setEmail(iDigBioRecord.getContactEmail());
    existing.setPosition(iDigBioRecord.getContactRole());
    assertFalse(matcher.matchContact(iDigBioRecord, Collections.emptySet()).isEmpty());
  }

  @Test
  public void stringSimilarityTest() {
    assertTrue(Matcher.stringSimilarity("test phrase", "test other") > 0);
    assertTrue(Matcher.stringSimilarity("test phrases", "my phrase") > 0);
    assertFalse(Matcher.stringSimilarity("the test phrase", "the other thing") > 0);
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
