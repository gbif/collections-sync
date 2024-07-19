package org.gbif.collections.sync.common.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;

import static org.gbif.collections.sync.SyncResult.ContactMatch;

public interface StaffResultHandler<S, R> {

  <T extends CollectionEntity & Contactable> ContactMatch handleStaff(
      MatchResult<S, R> matchResult, T entity);

  CollectionChangeSuggestion handleStaffForCollectionChangeSuggestion(
      MatchResult<S, R> matchResult, Collection entity, CollectionChangeSuggestion collectionChangeSuggestion);
}
