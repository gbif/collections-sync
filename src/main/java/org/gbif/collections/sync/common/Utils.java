package org.gbif.collections.sync.common;

import org.gbif.api.model.collections.Person;
import org.gbif.api.model.collections.PrimaryCollectionEntity;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.collections.Source;

import java.util.*;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
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

  public static <T extends PrimaryCollectionEntity & MachineTaggable> Map<String, Set<T>> mapByIrn(
      java.util.Collection<T> entities) {
    Map<String, Set<T>> mapByIrn = new HashMap<>();
    if (entities == null) {
      return mapByIrn;
    }

    entities.stream()
        .filter(
            e ->
                e.getDeleted() == null
                    && e.getMasterSourceMetadata() != null
                    && e.getMasterSourceMetadata().getSource() == Source.IH_IRN)
        .forEach(
            e ->
                mapByIrn
                    .computeIfAbsent(
                        e.getMasterSourceMetadata().getSourceId(), s -> new HashSet<>())
                    .add(e));
    return mapByIrn;
  }

  public static boolean isPersonInContacts(UUID personKey, java.util.Collection<Person> contacts) {
    return contacts != null && contacts.stream().anyMatch(c -> c.getKey().equals(personKey));
  }

  public static String removeUuidNamespace(String identifier) {
    if (Strings.isNullOrEmpty(identifier)) {
      return identifier;
    }
    return identifier.replace("urn:uuid:", "");
  }

  /** Counts how many values of the instance are not null or not empty. */
  public static <T> long countNonNullValues(Class<T> clazz, T instance) {
    return Arrays.stream(clazz.getDeclaredFields())
        .filter(
            f -> {
              try {
                Object value =
                    clazz
                        .getMethod(
                            "get"
                                + f.getName().substring(0, 1).toUpperCase()
                                + f.getName().substring(1))
                        .invoke(instance);
                if (value instanceof String) {
                  return !Strings.isNullOrEmpty(String.valueOf(value));
                } else {
                  return value != null;
                }
              } catch (Exception e) {
                return false;
              }
            })
        .count();
  }
}
