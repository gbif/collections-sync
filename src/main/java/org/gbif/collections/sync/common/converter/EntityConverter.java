package org.gbif.collections.sync.common.converter;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;

public interface EntityConverter<S, R> {

  Institution convertToInstitution(S source);

  Institution convertToInstitution(S source, Institution existing);

  Collection convertToCollection(S source, Collection existing);

  Collection convertToCollection(S source, Institution institution);

  Collection convertToCollection(S source, Collection existing, Institution institution);

  Person convertToPerson(R staffSource);

  Person convertToPerson(R staffSource, Person existing);
}
