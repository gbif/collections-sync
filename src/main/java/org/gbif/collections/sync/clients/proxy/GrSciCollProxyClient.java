package org.gbif.collections.sync.clients.proxy;

import java.util.List;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.common.converter.ConvertedCollection;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;

public interface GrSciCollProxyClient {

  Institution createInstitution(Institution newInstitution);

  boolean updateInstitution(Institution oldInstitution, Institution newInstitution);

  Collection createCollection(ConvertedCollection newCollection);


  List<Institution> findInstitutionByName(String institutionName);

  int createCollectionChangeSuggestion(CollectionChangeSuggestion createSuggestion);

  List<CollectionChangeSuggestion> getCollectionChangeSuggestion(String ihIdentifier);

  boolean updateCollection(Collection oldCollection, ConvertedCollection newCollection);
}
