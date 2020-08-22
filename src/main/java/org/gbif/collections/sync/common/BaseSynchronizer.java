package org.gbif.collections.sync.common;

import java.util.Arrays;
import java.util.Collections;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.CollectionOnlyMatch;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.SyncResult.NoEntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.clients.proxy.GrSciCollProxyClient;
import org.gbif.collections.sync.common.converter.EntityConverter;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.common.match.StaffResultHandler;

public abstract class BaseSynchronizer<S, R> {

  protected final GrSciCollProxyClient proxyClient;
  protected final StaffResultHandler<S, R> staffResultHandler;
  protected final EntityConverter<S, R> entityConverter;

  protected BaseSynchronizer(
      GrSciCollProxyClient proxyClient,
      StaffResultHandler<S, R> staffResultHandler,
      EntityConverter<S, R> entityConverter) {
    this.proxyClient = proxyClient;
    this.staffResultHandler = staffResultHandler;
    this.entityConverter = entityConverter;
  }

  protected EntityMatch<Institution> updateInstitution(S source, Institution instMatched) {
    Institution mergedInstitution = entityConverter.convertToInstitution(source, instMatched);

    boolean updated = proxyClient.updateInstitution(instMatched, mergedInstitution);

    return EntityMatch.<Institution>builder()
        .matched(instMatched)
        .merged(mergedInstitution)
        .update(updated)
        .build();
  }

  protected EntityMatch<Collection> updateCollection(S source, Collection collMatched) {
    Collection mergedCollection = entityConverter.convertToCollection(source, collMatched);

    boolean updated = proxyClient.updateCollection(collMatched, mergedCollection);

    return EntityMatch.<Collection>builder()
        .matched(collMatched)
        .merged(mergedCollection)
        .update(updated)
        .build();
  }

  protected Collection createCollection(S source, Institution instMatched) {
    Collection newCollection = entityConverter.convertToCollection(source, instMatched);

    return proxyClient.createCollection(newCollection);
  }

  public CollectionOnlyMatch handleCollectionMatch(MatchResult<S, R> matchResult) {
    EntityMatch<Collection> entityMatch =
        updateCollection(
            matchResult.getSource(), matchResult.getCollectionMatches().iterator().next());

    StaffMatch staffMatch =
        staffResultHandler.handleStaff(
            matchResult, Collections.singletonList(entityMatch.getMatched()));

    return CollectionOnlyMatch.builder()
        .matchedCollection(entityMatch)
        .staffMatch(staffMatch)
        .build();
  }

  public InstitutionOnlyMatch handleInstitutionMatch(MatchResult<S, R> matchResult) {
    EntityMatch<Institution> institutionEntityMatch =
        updateInstitution(
            matchResult.getSource(), matchResult.getInstitutionMatches().iterator().next());

    // create new collection linked to the institution
    Collection createdCollection =
        createCollection(matchResult.getSource(), institutionEntityMatch.getMerged());

    // same staff for both entities
    StaffMatch staffMatch =
        staffResultHandler.handleStaff(
            matchResult, Arrays.asList(institutionEntityMatch.getMatched(), createdCollection));

    return InstitutionOnlyMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .newCollection(createdCollection)
        .staffMatch(staffMatch)
        .build();
  }

  public InstitutionAndCollectionMatch handleInstAndCollMatch(MatchResult<S, R> matchResult) {
    // update institution
    EntityMatch<Institution> institutionEntityMatch =
        updateInstitution(
            matchResult.getSource(), matchResult.getInstitutionMatches().iterator().next());

    // update collection
    EntityMatch<Collection> collectionEntityMatch =
        updateCollection(
            matchResult.getSource(), matchResult.getCollectionMatches().iterator().next());

    // update staff
    Institution institution = institutionEntityMatch.getMerged();
    Collection collection = collectionEntityMatch.getMerged();

    // then we handle the staff of both entities at the same time to avoid creating duplicates
    StaffMatch staffMatch =
        staffResultHandler.handleStaff(matchResult, Arrays.asList(institution, collection));

    return InstitutionAndCollectionMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .matchedCollection(collectionEntityMatch)
        .staffMatch(staffMatch)
        .build();
  }

  public NoEntityMatch handleNoMatch(MatchResult<S, R> matchResult) {
    // create institution
    Institution newInstitution = entityConverter.convertToInstitution(matchResult.getSource());

    Institution createdInstitution = proxyClient.createInstitution(newInstitution);

    // create collection
    Collection createdCollection = createCollection(matchResult.getSource(), createdInstitution);

    // same staff for both entities
    StaffMatch staffMatch =
        staffResultHandler.handleStaff(
            matchResult, Arrays.asList(createdInstitution, createdCollection));

    return NoEntityMatch.builder()
        .newCollection(createdCollection)
        .newInstitution(createdInstitution)
        .staffMatch(staffMatch)
        .build();
  }
}
