package org.gbif.collections.sync;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.vocabulary.IdentifierType;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

  /**
   * Encodes the IH IRN into the format stored on the GRSciColl identifier. E.g. 123 ->
   * gbif:ih:irn:123
   */
  public static String encodeIRN(String irn) {
    return "gbif:ih:irn:" + irn;
  }

  public static String decodeIRN(String irn) {
    return irn.replace("gbif:ih:irn:", "");
  }

  public static boolean containsIrnIdentifier(Identifiable entity) {
    return entity.getIdentifiers() != null
        && entity.getIdentifiers().stream().anyMatch(i -> i.getType() == IdentifierType.IH_IRN);
  }

  public static <T extends CollectionEntity & Identifiable> Map<String, Set<T>> mapByIrn(
      java.util.Collection<T> entities) {
    Map<String, Set<T>> mapByIrn = new HashMap<>();
    if (entities == null) {
      return mapByIrn;
    }

    entities.forEach(
        o ->
            o.getIdentifiers().stream()
                .filter(i -> i.getType() == IdentifierType.IH_IRN)
                .forEach(
                    i -> mapByIrn.computeIfAbsent(i.getIdentifier(), s -> new HashSet<>()).add(o)));
    return mapByIrn;
  }

  public static boolean isPersonInContacts(UUID personKey, Collection<Person> contacts) {
    return contacts != null && contacts.stream().anyMatch(c -> c.getKey().equals(personKey));
  }
}
