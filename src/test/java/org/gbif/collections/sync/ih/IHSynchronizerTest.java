package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.*;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.*;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import static org.gbif.collections.sync.TestUtils.assertEmptyContactMatch;
import static org.gbif.collections.sync.common.Utils.IH_NAMESPACE;
import static org.gbif.collections.sync.common.Utils.IRN_TAG;

import static org.junit.Assert.assertEquals;

public class IHSynchronizerTest extends BaseIHTest {

  @Test
  public void ihSynchronizerTest() {
    DataLoader<IHData> dataLoader = createData();

    IHSynchronizer ihSynchronizer = IHSynchronizer.create(ihConfig, dataLoader);
    SyncResult syncResult = ihSynchronizer.sync();

    // assert collection only matches
    assertEquals(1, syncResult.getCollectionOnlyMatches().size());
    CollectionOnlyMatch collectionOnlyMatch = syncResult.getCollectionOnlyMatches().get(0);
    assertEmptyContactMatch(collectionOnlyMatch.getContactMatch());

    // assert institution only matches
    assertEquals(1, syncResult.getInstitutionOnlyMatches().size());
    InstitutionOnlyMatch institutionOnlyMatch = syncResult.getInstitutionOnlyMatches().get(0);
    assertEmptyContactMatch(collectionOnlyMatch.getContactMatch());

    // assert institution and collection matches
    assertEquals(1, syncResult.getInstAndCollMatches().size());
    InstitutionAndCollectionMatch instAndCollMatch = syncResult.getInstAndCollMatches().get(0);
    assertEquals(2, instAndCollMatch.getContactMatch().getMatchedContacts().size());
    assertEquals(
        1,
        instAndCollMatch.getContactMatch().getMatchedContacts().stream()
            .filter(EntityMatch::isUpdate)
            .count());
    assertEquals(
        1,
        instAndCollMatch.getContactMatch().getMatchedContacts().stream()
            .filter(m -> !m.isUpdate())
            .count());
    assertEquals(4, instAndCollMatch.getContactMatch().getNewContacts().size());
    assertEquals(1, instAndCollMatch.getContactMatch().getRemovedContacts().size());

    // assert no matches
    assertEquals(1, syncResult.getNoMatches().size());
    NoEntityMatch noEntityMatch = syncResult.getNoMatches().get(0);
    assertEquals(2, noEntityMatch.getContactMatch().getNewContacts().size());
  }

  private DataLoader<IHData> createData() {
    IHInstitution ih1 = new IHInstitution();
    ih1.setCode("c1");
    ih1.setOrganization("o1");
    ih1.setIrn("1");

    IHStaff is1 = new IHStaff();
    is1.setFirstName("first");
    is1.setLastName("last");
    IHStaff.Contact contact1 = new IHStaff.Contact();
    contact1.setEmail("bb@test.com");
    is1.setContact(contact1);
    is1.setIrn("11");
    is1.setCode(ih1.getCode());

    IHStaff is2 = new IHStaff();
    is2.setFirstName("first2");
    is2.setLastName("last2");
    is2.setIrn("12");
    is2.setCode(ih1.getCode());

    IHStaff is3 = new IHStaff();
    is3.setFirstName("first3");
    is3.setLastName("last3");
    is3.setIrn("13");
    IHStaff.Contact contact3 = new IHStaff.Contact();
    contact3.setEmail("aa@test.com");
    is3.setContact(contact3);
    is3.setCode(ih1.getCode());
    is3.setPosition("pos3");

    IHInstitution ih2 = new IHInstitution();
    ih2.setCode("c2");
    ih2.setOrganization("o2");
    ih2.setIrn("2");

    IHInstitution ih3 = new IHInstitution();
    ih3.setCode("c3");
    ih3.setOrganization("o3");
    ih3.setIrn("3");
    ih1.setOrganization("o3");

    IHInstitution ih4 = new IHInstitution();
    ih4.setCode("c4");
    ih4.setOrganization("o4");
    ih4.setIrn("4");
    ih1.setOrganization("o4");

    IHStaff is4 = new IHStaff();
    is4.setFirstName("first4");
    is4.setIrn("14");
    is4.setCode(ih4.getCode());

    Institution i1 = new Institution();
    i1.setKey(UUID.randomUUID());
    i1.setCode("c11");
    i1.setName("Inst 1");
    i1.getMachineTags().add(new MachineTag(IH_NAMESPACE, IRN_TAG, ih1.getIrn()));

    Collection c1 = new Collection();
    c1.setKey(UUID.randomUUID());
    c1.setInstitutionKey(i1.getKey());
    c1.setCode("c1");
    c1.setName("Coll 1");
    c1.getMachineTags().add(new MachineTag(IH_NAMESPACE, IRN_TAG, ih1.getIrn()));

    Contact cNoChange = new Contact();
    cNoChange.setKey(1);
    cNoChange.setFirstName(is1.getFirstName());
    cNoChange.setLastName(is1.getLastName());
    cNoChange.getEmail().add(is1.getContact().getEmail());
    cNoChange.getUserIds().add(new UserId(IdType.IH_IRN, is1.getIrn()));
    i1.getContactPersons().add(cNoChange);

    Contact cToRemove = new Contact();
    cToRemove.setKey(2);
    cToRemove.setFirstName("person to remove");
    i1.getContactPersons().add(cToRemove);

    Contact cToUpdate = new Contact();
    cToUpdate.setKey(3);
    cToUpdate.setFirstName(is3.getFirstName() + "  " + is3.getLastName());
    cToUpdate.getEmail().add(is3.getContact().getEmail());
    cToUpdate.getUserIds().add(new UserId(IdType.IH_IRN, is3.getIrn()));
    i1.getContactPersons().add(cToUpdate);

    Institution i2 = new Institution();
    i2.setKey(UUID.randomUUID());
    i2.setCode("c2");
    i2.setName("Inst 2");
    i2.getMachineTags().add(new MachineTag(IH_NAMESPACE, IRN_TAG, ih2.getIrn()));

    Collection c2 = new Collection();
    c2.setKey(UUID.randomUUID());
    c2.setCode("c2");
    c2.setName("Coll 2");
    c2.getMachineTags().add(new MachineTag(IH_NAMESPACE, IRN_TAG, ih3.getIrn()));

    List<Institution> institutions = Arrays.asList(i1, i2);
    List<Collection> collections = Arrays.asList(c1, c2);
    List<Contact> contacts = Arrays.asList(cNoChange, cToRemove, cToUpdate);
    List<IHInstitution> ihInstitutions = Arrays.asList(ih1, ih2, ih3, ih4);
    List<IHStaff> ihStaff = Arrays.asList(is1, is2, is3, is4);

    return TestDataLoader.builder()
        .institutions(institutions)
        .collections(collections)
        .contacts(contacts)
        .ihInstitutions(ihInstitutions)
        .ihStaff(ihStaff)
        .countries(COUNTRIES)
        .build();
  }
}
