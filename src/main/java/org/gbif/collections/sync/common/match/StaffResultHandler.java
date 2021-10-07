package org.gbif.collections.sync.common.match;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.collections.sync.SyncResult.StaffMatch;

import java.util.List;

import static org.gbif.collections.sync.SyncResult.ContactMatch;

public interface StaffResultHandler<S, R> {

  @Deprecated
  <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult<S, R> matchResult, List<T> entities);

  <T extends CollectionEntity & Contactable> ContactMatch handleStaff(
      MatchResult<S, R> matchResult, T entity);
}
