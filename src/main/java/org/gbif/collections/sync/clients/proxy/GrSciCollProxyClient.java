package org.gbif.collections.sync.clients.proxy;

import java.util.List;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.suggestions.ChangeSuggestion;

public interface GrSciCollProxyClient {

  Institution createInstitution(Institution newInstitution);

  boolean updateInstitution(Institution oldInstitution, Institution newInstitution);

  Collection createCollection(Collection newCollection);

  boolean updateCollection(Collection oldCollection, Collection newCollection);

  List<Institution> findInstitutionByName(String institutionName);

  int createChangeSuggestion(ChangeSuggestion<Institution> createSuggestion);
}
