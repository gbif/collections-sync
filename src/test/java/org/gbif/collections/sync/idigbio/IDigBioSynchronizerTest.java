package org.gbif.collections.sync.idigbio;

import java.util.Arrays;
import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.SyncResult.NoEntityMatch;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.idigbio.IDigBioDataLoader.IDigBioData;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IDigBioSynchronizerTest extends BaseIDigBioTest {

  @Test
  public void iDigBioSynchronizerTest() {
    DataLoader<IDigBioData> dataLoader = createData();

    IDigBioSynchronizer iDigBioSynchronizer = IDigBioSynchronizer.create(iDigBioConfig, dataLoader);
    SyncResult syncResult = iDigBioSynchronizer.sync();

    // assert collection only matches
    assertEquals(0, syncResult.getCollectionOnlyMatches().size());

    // assert institution only matches
    assertEquals(2, syncResult.getInstitutionOnlyMatches().size());
    InstitutionOnlyMatch institutionOnlyMatch = syncResult.getInstitutionOnlyMatches().get(0);
    assertEquals(1, institutionOnlyMatch.getStaffMatch().getNewPersons().size());

    assertEquals(
        1,
        syncResult.getInstitutionOnlyMatches().stream()
            .filter(m -> m.getStaffMatch().getNewPersons().size() == 1)
            .count());
    assertEquals(
        1,
        syncResult.getInstitutionOnlyMatches().stream()
            .filter(m -> m.getStaffMatch().getMatchedPersons().size() == 1)
            .count());

    // assert institution and collection matches
    assertEquals(1, syncResult.getInstAndCollMatches().size());
    InstitutionAndCollectionMatch instAndCollMatch = syncResult.getInstAndCollMatches().get(0);
    assertEquals(1, instAndCollMatch.getStaffMatch().getMatchedPersons().size());
    assertFalse(instAndCollMatch.getStaffMatch().getMatchedPersons().iterator().next().isUpdate());

    // assert no matches
    assertEquals(1, syncResult.getNoMatches().size());
    NoEntityMatch noEntityMatch = syncResult.getNoMatches().get(0);
    assertEquals(1, noEntityMatch.getStaffMatch().getNewPersons().size());

    // assert invalid
    assertEquals(1, syncResult.getInvalidEntities().size());
  }

  private DataLoader<IDigBioData> createData() {
    Institution i1 = new Institution();
    i1.setKey(UUID.randomUUID());
    i1.setCode("i1");
    i1.setName("inst 1");

    Collection c1 = new Collection();
    c1.setKey(UUID.randomUUID());
    c1.setCode("c1");
    c1.setName("Collection1");
    c1.setInstitutionKey(i1.getKey());

    IDigBioRecord r1 = new IDigBioRecord();
    r1.setGrbioInstMatch(i1.getKey());
    r1.setInstitution("inst 1");
    r1.setInstitutionCode("i1");
    r1.setCollectionCode("foo");
    r1.setCollection("foo2");
    r1.setContact("contact");
    r1.setContactRole("role");
    r1.setContactEmail("contact@test.com");

    Institution i2 = new Institution();
    i2.setKey(UUID.randomUUID());
    i2.setCode("i2");
    i2.setName("inst 2");

    Collection c2 = new Collection();
    c2.setKey(UUID.randomUUID());
    c2.setCode("c2");
    c2.setName("Collection2");
    c2.setInstitutionKey(i2.getKey());

    IDigBioRecord r2 = new IDigBioRecord();
    r2.setGrbioInstMatch(i2.getKey());
    r2.setInstitution("inst 2");
    r2.setInstitutionCode("i2");
    r2.setCollectionCode("c2");
    r2.setCollection("Collection2");
    r2.setContact("contact");
    r2.setContactRole("role");
    r2.setContactEmail("contact@test.com");

    IDigBioRecord r3 = new IDigBioRecord();
    r3.setInstitution("inst 3");
    r3.setInstitutionCode("i3");
    r3.setContact("contact");
    r3.setContactRole("role2");
    r3.setContactEmail("contact@test.com");

    IDigBioRecord invalid = new IDigBioRecord();
    invalid.setInstitution("inst 3");

    IDigBioRecord repeated = new IDigBioRecord();
    repeated.setInstitution("inst 3");
    repeated.setInstitutionCode("i3");
    repeated.setContact("contact");
    repeated.setContactRole("role2");
    repeated.setContactEmail("contact@test.com");

    return TestDataLoader.builder()
        .institutions(Arrays.asList(i1, i2))
        .collections(Arrays.asList(c1, c2))
        .iDigBioRecords(Arrays.asList(r1, r2, r3, invalid, repeated))
        .build();
  }
}
