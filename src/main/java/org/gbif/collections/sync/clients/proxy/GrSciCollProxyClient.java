package org.gbif.collections.sync.clients.proxy;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;

public interface GrSciCollProxyClient {

  Institution createInstitution(Institution newInstitution);

  boolean updateInstitution(Institution oldInstitution, Institution newInstitution);

  Collection createCollection(Collection newCollection);

  boolean updateCollection(Collection oldCollection, Collection newCollection);
}
