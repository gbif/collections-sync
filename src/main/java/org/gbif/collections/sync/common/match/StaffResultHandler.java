package org.gbif.collections.sync.common.match;

import java.util.List;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.collections.sync.SyncResult.StaffMatch;

public interface StaffResultHandler<S, R> {

  <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult<S, R> matchResult, List<T> entities);
}
