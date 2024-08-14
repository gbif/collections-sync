package org.gbif.collections.sync.clients.proxy;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.common.converter.ConvertedCollection;

public interface GrSciCollProxyClient {

  Institution createInstitution(Institution newInstitution);

  boolean updateInstitution(Institution oldInstitution, Institution newInstitution);

  Collection createCollection(ConvertedCollection newCollection);

  boolean updateCollection(Collection oldCollection, ConvertedCollection newCollection);
}
