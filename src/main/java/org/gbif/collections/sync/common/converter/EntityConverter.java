package org.gbif.collections.sync.common.converter;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;

public interface EntityConverter<S, R> {

  Institution convertToInstitution(S source);

  Institution convertToInstitution(S source, Institution existing);

  Collection convertToCollection(S source, Collection existing);

  Collection convertToCollection(S source, Institution institution);

  Collection convertToCollection(S source, Collection existing, Institution institution);

  @Deprecated
  Person convertToPerson(R staffSource);

  @Deprecated
  Person convertToPerson(R staffSource, Person existing);

  Contact convertToContact(R staffSource);

  Contact convertToContact(R staffSource, Contact existing);


}
