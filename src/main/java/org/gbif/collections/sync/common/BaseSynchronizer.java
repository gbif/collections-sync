package org.gbif.collections.sync.common;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;
import org.gbif.api.model.collections.suggestions.Type;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.SyncResult.*;
import org.gbif.collections.sync.clients.proxy.GrSciCollProxyClient;
import org.gbif.collections.sync.common.converter.ConvertedCollection;
import org.gbif.collections.sync.common.converter.EntityConverter;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.common.match.StaffResultHandler;

public abstract class BaseSynchronizer<S, R> {

  protected final GrSciCollProxyClient proxyClient;
  protected final StaffResultHandler<S, R> staffResultHandler;
  protected final EntityConverter<S, R> entityConverter;
  private final static String COMMENT = "This suggestion was created as part of the weekly synchronisation"
      + " of GRSciColl with Index Herbariorum (https://sweetgum.nybg.org/science/ih/)";

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
    ConvertedCollection mergedCollection = entityConverter.convertToCollection(source, collMatched);

    boolean updated = proxyClient.updateCollection(collMatched, mergedCollection);

    return EntityMatch.<Collection>builder()
        .matched(collMatched)
        .merged(mergedCollection.getCollection())
        .update(updated)
        .build();
  }

  protected Collection createCollection(S source, Institution instMatched) {
    ConvertedCollection newCollection = entityConverter.convertToCollection(source, instMatched);

    return proxyClient.createCollection(newCollection);
  }

  @VisibleForTesting
  public CollectionOnlyMatch handleCollectionMatch(MatchResult<S, R> matchResult) {
    EntityMatch<Collection> entityMatch =
        updateCollection(
            matchResult.getSource(), matchResult.getCollectionMatches().iterator().next());

    ContactMatch contactMatch =
        staffResultHandler.handleStaff(matchResult, entityMatch.getMerged());

    return CollectionOnlyMatch.builder()
        .matchedCollection(entityMatch)
        .contactMatch(contactMatch)
        .build();
  }

  @VisibleForTesting
  public InstitutionOnlyMatch handleInstitutionMatch(MatchResult<S, R> matchResult) {
    EntityMatch<Institution> institutionEntityMatch =
        updateInstitution(
            matchResult.getSource(), matchResult.getInstitutionMatches().iterator().next());

    // create new collection linked to the institution
    Collection createdCollection =
        createCollection(matchResult.getSource(), institutionEntityMatch.getMerged());

    // staff for both entities
    ContactMatch contactMatchInstitution =
        staffResultHandler.handleStaff(matchResult, institutionEntityMatch.getMerged());

    ContactMatch contactMatchCollection =
        staffResultHandler.handleStaff(matchResult, createdCollection);

    return InstitutionOnlyMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .newCollection(createdCollection)
        .contactMatch(mergeContactMatches(contactMatchInstitution, contactMatchCollection))
        .build();
  }

  @VisibleForTesting
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

    ContactMatch contactMatchInstitution = staffResultHandler.handleStaff(matchResult, institution);
    ContactMatch contactMatchCollection = staffResultHandler.handleStaff(matchResult, collection);

    return InstitutionAndCollectionMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .matchedCollection(collectionEntityMatch)
        .contactMatch(mergeContactMatches(contactMatchInstitution, contactMatchCollection))
        .build();
  }

  @VisibleForTesting
  public NoEntityMatch handleNoMatch(MatchResult<S, R> matchResult) {
    Institution newInstitution = entityConverter.convertToInstitution(matchResult.getSource());
    List<String> ihIdentifiers = getIhIdentifiers(newInstitution);

    //If we already created a suggestion before we do nothing
    if (!proxyClient.getCollectionChangeSuggestion(ihIdentifiers.get(0)).isEmpty()){
      return NoEntityMatch.builder().build();
    }
    //Check if there is another institution with the same name
    List<Institution> institutionWithSameName = proxyClient.findInstitutionByName(newInstitution.getName());
    //If not, we should create a suggestion with the option of creating an institution suggestion
    if (institutionWithSameName.isEmpty()) {
      return createAndSuggestCollection(matchResult, newInstitution, ihIdentifiers, true);
    }
    else {
      return createAndSuggestCollection(matchResult, institutionWithSameName.get(0), ihIdentifiers, false);
    }
  }

  @VisibleForTesting
  public Conflict handleConflict(MatchResult<S, R> matchResult) {
    return new Conflict(matchResult.getSource(), matchResult.getAllMatches());
  }

  private List<String> getIhIdentifiers(Institution institution) {
    return institution.getIdentifiers().stream()
        .filter(identifier -> identifier.getType().equals(IdentifierType.IH_IRN ))
        .map(Identifier::getIdentifier)
        .collect(Collectors.toList());
  }

  private NoEntityMatch createAndSuggestCollection(MatchResult<S, R> matchResult,
      Institution institution, List<String> ihIdentifiers, boolean createInstitution) {
    Collection newCollection = entityConverter.convertToCollection(matchResult.getSource(),
        institution).getCollection();
    CollectionChangeSuggestion collectionChangeSuggestion = createCollectionChangeSuggestion(newCollection,
        ihIdentifiers, createInstitution);
    collectionChangeSuggestion = staffResultHandler.handleStaffForCollectionChangeSuggestion(matchResult,newCollection,collectionChangeSuggestion);
    proxyClient.createCollectionChangeSuggestion(collectionChangeSuggestion);
    return NoEntityMatch.builder().newChangeSuggestion(collectionChangeSuggestion).build();
  }

  private CollectionChangeSuggestion createCollectionChangeSuggestion(Collection newCollection,
      List<String> ihIdentifiers, Boolean createInstitution) {
    CollectionChangeSuggestion collectionChangeSuggestion = new CollectionChangeSuggestion();
    collectionChangeSuggestion.setType(Type.CREATE);
    collectionChangeSuggestion.setProposerEmail("scientific-collections@gbif.org");
    List<String> comments = new ArrayList<>();
    comments.add(COMMENT);
    collectionChangeSuggestion.setComments(comments);
    collectionChangeSuggestion.setSuggestedEntity(newCollection);
    collectionChangeSuggestion.setIhIdentifier(ihIdentifiers.get(0));
    collectionChangeSuggestion.setCreateInstitution(createInstitution);
    return collectionChangeSuggestion;
  }

  private ContactMatch mergeContactMatches(ContactMatch contactMatch1, ContactMatch contactMatch2) {
    if (contactMatch1 == null) {
      return contactMatch2;
    } else if (contactMatch2 == null) {
      return contactMatch1;
    }

    List<Contact> newContacts = new ArrayList<>();
    if (contactMatch1.getNewContacts() != null) {
      newContacts.addAll(contactMatch1.getNewContacts());
    }
    if (contactMatch2.getNewContacts() != null) {
      newContacts.addAll(contactMatch2.getNewContacts());
    }

    List<EntityMatch<Contact>> matchedContacts = new ArrayList<>();
    if (contactMatch1.getMatchedContacts() != null) {
      matchedContacts.addAll(contactMatch1.getMatchedContacts());
    }
    if (contactMatch2.getMatchedContacts() != null) {
      matchedContacts.addAll(contactMatch2.getMatchedContacts());
    }

    List<Contact> removedContacts = new ArrayList<>();
    if (contactMatch1.getRemovedContacts() != null) {
      removedContacts.addAll(contactMatch1.getRemovedContacts());
    }
    if (contactMatch2.getRemovedContacts() != null) {
      removedContacts.addAll(contactMatch2.getRemovedContacts());
    }

    List<Conflict> conflicts = new ArrayList<>();
    if (contactMatch1.getConflicts() != null) {
      conflicts.addAll(contactMatch1.getConflicts());
    }
    if (contactMatch2.getConflicts() != null) {
      conflicts.addAll(contactMatch2.getConflicts());
    }

    return ContactMatch.builder()
        .newContacts(newContacts)
        .matchedContacts(matchedContacts)
        .removedContacts(removedContacts)
        .conflicts(conflicts)
        .build();
  }
}
