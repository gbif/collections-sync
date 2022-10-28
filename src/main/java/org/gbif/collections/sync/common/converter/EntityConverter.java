package org.gbif.collections.sync.common.converter;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;

public interface EntityConverter<S, R> {

  Institution convertToInstitution(S source);

  Institution convertToInstitution(S source, Institution existing);

  Collection convertToCollection(S source, Collection existing);

  Collection convertToCollection(S source, Institution institution);

  Collection convertToCollection(S source, Collection existing, Institution institution);

  Contact convertToContact(R staffSource);

  Contact convertToContact(R staffSource, Contact existing);
}
