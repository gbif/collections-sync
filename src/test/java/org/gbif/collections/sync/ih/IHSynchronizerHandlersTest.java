package org.gbif.collections.sync.ih;

import java.util.ArrayList;
import java.util.List;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.MasterSourceMetadata;
import org.gbif.api.model.collections.suggestions.InstitutionChangeSuggestion;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.collections.Source;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.clients.proxy.GrSciCollProxyClient;
import org.gbif.collections.sync.ih.match.IHMatchResult;
import org.gbif.collections.sync.ih.model.IHInstitution;

import java.util.Collections;
import java.util.UUID;

import org.junit.Test;

import static org.gbif.collections.sync.TestUtils.assertEmptyContactMatch;
import static org.gbif.collections.sync.common.Utils.encodeIRN;
import static org.gbif.collections.sync.ih.IHEntityConverter.DEFAULT_COLLECTION_NAME_FORMAT;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IHSynchronizerHandlersTest extends BaseIHTest {

  @Test
  public void collectionToUpdateTest() {
    TestEntity<Collection, IHInstitution> collectionToUpdate = createCollectionToUpdate();
    IHMatchResult match =
        IHMatchResult.builder()
            .collections(Collections.singleton(collectionToUpdate.entity))
            .ihInstitution(collectionToUpdate.ih)
            .build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = synchronizer.handleCollectionMatch(match);
    assertEntityMatch(collectionOnlyMatch.getMatchedCollection(), collectionToUpdate, true);
    assertEmptyContactMatch(collectionOnlyMatch.getContactMatch());
  }

  @Test
  public void collectionNoChangeTest() {
    TestEntity<Collection, IHInstitution> collectionNoChange = createCollectionNoChange();
    IHMatchResult match =
        IHMatchResult.builder()
            .collections(Collections.singleton(collectionNoChange.entity))
            .ihInstitution(collectionNoChange.ih)
            .build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = synchronizer.handleCollectionMatch(match);
    assertEntityMatch(collectionOnlyMatch.getMatchedCollection(), collectionNoChange, false);
    assertEmptyContactMatch(collectionOnlyMatch.getContactMatch());
  }

  @Test
  public void institutionToUpdateTest() {
    TestEntity<Institution, IHInstitution> institutionToUpdate = createInstitutionToUpdate();
    IHMatchResult match =
        IHMatchResult.builder()
            .institutions(Collections.singleton(institutionToUpdate.entity))
            .ihInstitution(institutionToUpdate.ih)
            .build();

    SyncResult.InstitutionOnlyMatch institutionOnlyMatch =
        synchronizer.handleInstitutionMatch(match);
    assertEntityMatch(institutionOnlyMatch.getMatchedInstitution(), institutionToUpdate, true);
    assertEmptyContactMatch(institutionOnlyMatch.getContactMatch());
  }

  @Test
  public void institutionNoChangeTest() {
    TestEntity<Institution, IHInstitution> institutionNoChange = createInstitutionNoChange();
    IHMatchResult match =
        IHMatchResult.builder()
            .institutions(Collections.singleton(institutionNoChange.entity))
            .ihInstitution(institutionNoChange.ih)
            .build();

    SyncResult.InstitutionOnlyMatch institutionOnlyMatch =
        synchronizer.handleInstitutionMatch(match);
    assertEntityMatch(institutionOnlyMatch.getMatchedInstitution(), institutionNoChange, false);
    assertEmptyContactMatch(institutionOnlyMatch.getContactMatch());
  }

  @Test
  public void noMatchTest() {
    // IH institution
    IHInstitution ih = new IHInstitution();
    ih.setIrn(IRN_TEST);
    ih.setCode("foo");
    ih.setOrganization("foo");
    ih.setSpecimenTotal(1000);

    // Expected institution
    Institution expectedInstitution = new Institution();
    expectedInstitution.setCode(ih.getCode());
    expectedInstitution.setName(ih.getOrganization());
    expectedInstitution.setMasterSourceMetadata(new MasterSourceMetadata(Source.IH_IRN, IRN_TEST));

    // Expected collection
    Collection expectedCollection = new Collection();
    expectedCollection.setCode(ih.getCode());
    expectedCollection.setName(
        String.format(DEFAULT_COLLECTION_NAME_FORMAT, expectedInstitution.getName()));
    expectedCollection.setNumberSpecimens(1000);
    expectedCollection.setMasterSourceMetadata(new MasterSourceMetadata(Source.IH_IRN, IRN_TEST));

    // add identifier to expected entities
    Identifier newIdentifier = new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST));
    newIdentifier.setCreatedBy(TEST_USER);
    expectedInstitution.getIdentifiers().add(newIdentifier);
    expectedCollection.getIdentifiers().add(newIdentifier);

    IHMatchResult match = IHMatchResult.builder().ihInstitution(ih).build();
    SyncResult.NoEntityMatch noEntityMatch = synchronizer.handleNoMatch(match);
    assertTrue(noEntityMatch.getNewCollection().lenientEquals(expectedCollection));
    assertTrue(noEntityMatch.getNewInstitution().lenientEquals(expectedInstitution));
    assertEmptyContactMatch(noEntityMatch.getContactMatch());
  }

  @Test
  public void noMatchWithExistingInstitutionNameTest() {

    GrSciCollProxyClient proxyClient = mock(GrSciCollProxyClient.class);
    // IH institution
    IHInstitution ih = new IHInstitution();
    ih.setIrn(IRN_TEST);
    ih.setCode("foo");
    ih.setOrganization("foo");
    ih.setSpecimenTotal(1000);

    // Expected institution
    Institution expectedInstitution = new Institution();
    expectedInstitution.setCode(ih.getCode());
    expectedInstitution.setName(ih.getOrganization());
    expectedInstitution.setMasterSourceMetadata(new MasterSourceMetadata(Source.IH_IRN, IRN_TEST));

    // Expected collection
    Collection expectedCollection = new Collection();
    expectedCollection.setCode(ih.getCode());
    expectedCollection.setName(String.format(DEFAULT_COLLECTION_NAME_FORMAT, expectedInstitution.getName()));
    expectedCollection.setNumberSpecimens(1000);
    expectedCollection.setMasterSourceMetadata(new MasterSourceMetadata(Source.IH_IRN, IRN_TEST));

    // add identifier to expected entities
    Identifier newIdentifier = new Identifier(IdentifierType.IH_IRN, encodeIRN(IRN_TEST));
    newIdentifier.setCreatedBy(TEST_USER);
    expectedInstitution.getIdentifiers().add(newIdentifier);
    expectedCollection.getIdentifiers().add(newIdentifier);

    IHMatchResult match = IHMatchResult.builder().ihInstitution(ih).build();

    // Mock behavior for findInstitutionByName
    List<Institution> institutionsWithSameName = new ArrayList<>();
    institutionsWithSameName.add(expectedInstitution);

    when(proxyClient.findInstitutionByName(anyString())).thenReturn(institutionsWithSameName);
    when(proxyClient.createChangeSuggestion(any(InstitutionChangeSuggestion.class))).thenReturn(1);
    // Call the method under test
    SyncResult.NoEntityMatch noEntityMatch = synchronizer.handleNoMatch(match);

    // Assertions
    assertEquals(1, noEntityMatch.getNewChangeSuggestion());
    assertTrue(noEntityMatch.getNewInstitution().lenientEquals(expectedInstitution));
    assertEmptyContactMatch(noEntityMatch.getContactMatch());
  }

  @Test
  public void institutionAndCollectionMatchTest() {
    TestEntity<Collection, IHInstitution> collectionToUpdate = createCollectionToUpdate();
    TestEntity<Institution, IHInstitution> institutionToUpdate = createInstitutionToUpdate();
    IHMatchResult match =
        IHMatchResult.builder()
            .collections(Collections.singleton(collectionToUpdate.entity))
            .institutions(Collections.singleton(institutionToUpdate.entity))
            .ihInstitution(collectionToUpdate.ih)
            .build();

    SyncResult.InstitutionAndCollectionMatch instAndColMatch =
        synchronizer.handleInstAndCollMatch(match);
    assertEntityMatch(instAndColMatch.getMatchedCollection(), collectionToUpdate, true);
    assertEntityMatch(instAndColMatch.getMatchedInstitution(), institutionToUpdate, true);
    assertEmptyContactMatch(instAndColMatch.getContactMatch());
  }

  @Test
  public void handleConflictTest() {
    IHInstitution ihInstitution = new IHInstitution();
    ihInstitution.setCode("code");
    ihInstitution.setOrganization("org");

    Collection c1 = new Collection();
    c1.setKey(UUID.randomUUID());

    Collection c2 = new Collection();
    c2.setKey(UUID.randomUUID());

    IHMatchResult match =
        IHMatchResult.builder().ihInstitution(ihInstitution).collection(c1).collection(c2).build();

    SyncResult.Conflict conflictMatch = synchronizer.handleConflict(match);
    assertNotNull(conflictMatch.getEntity());
    assertEquals(2, conflictMatch.getGrSciCollEntities().size());
  }

  @Test
  public void masterSourceMetadataUpdateInCollectionTest() {
    TestEntity<Collection, IHInstitution> collectionNoChange = createCollectionNoChange();
    collectionNoChange.getEntity().setMasterSourceMetadata(null);
    IHMatchResult match =
        IHMatchResult.builder()
            .collections(Collections.singleton(collectionNoChange.entity))
            .ihInstitution(collectionNoChange.ih)
            .build();

    SyncResult.CollectionOnlyMatch collectionOnlyMatch = synchronizer.handleCollectionMatch(match);
    assertTrue(collectionOnlyMatch.getMatchedCollection().isUpdate());
  }

  @Test
  public void masterSourceMetadataUpdateInInstitutionTest() {
    TestEntity<Institution, IHInstitution> institutionNoChange = createInstitutionNoChange();
    institutionNoChange.getEntity().setMasterSourceMetadata(null);
    IHMatchResult match =
        IHMatchResult.builder()
            .institutions(Collections.singleton(institutionNoChange.entity))
            .ihInstitution(institutionNoChange.ih)
            .build();

    SyncResult.InstitutionOnlyMatch institutionOnlyMatch =
        synchronizer.handleInstitutionMatch(match);
    assertTrue(institutionOnlyMatch.getMatchedInstitution().isUpdate());
  }
}
