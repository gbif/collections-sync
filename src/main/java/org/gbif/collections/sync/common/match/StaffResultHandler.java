package org.gbif.collections.sync.common.match;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;

import static org.gbif.collections.sync.SyncResult.ContactMatch;

public interface StaffResultHandler<S, R> {

  <T extends CollectionEntity & Contactable> ContactMatch handleStaff(
      MatchResult<S, R> matchResult, T entity);
}
