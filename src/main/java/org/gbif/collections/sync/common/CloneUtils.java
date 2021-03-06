package org.gbif.collections.sync.common;

import java.util.ArrayList;

import org.gbif.api.model.collections.Address;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Commentable;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.MachineTaggable;
import org.gbif.api.model.registry.Taggable;

import org.apache.commons.beanutils.BeanUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class CloneUtils {
  public static Institution cloneInstitution(Institution institution) {
    Institution clone = cloneCollectionEntity(institution, new Institution());

    if (clone.getAlternativeCodes() != null) {
      clone.setAlternativeCodes(new ArrayList<>(clone.getAlternativeCodes()));
    }
    return clone;
  }

  public static Collection cloneCollection(Collection collection) {
    Collection clone = cloneCollectionEntity(collection, new Collection());

    if (clone.getAlternativeCodes() != null) {
      clone.setAlternativeCodes(new ArrayList<>(clone.getAlternativeCodes()));
    }

    return clone;
  }

  private static <
          T extends
              CollectionEntity & Identifiable & Taggable & MachineTaggable & Contactable
                  & Commentable>
      T cloneCollectionEntity(T entity, T clone) {
    if (entity != null) {
      // copy fields
      try {
        BeanUtils.copyProperties(clone, entity);

        if (clone.getIdentifiers() != null) {
          clone.setIdentifiers(new ArrayList<>(clone.getIdentifiers()));
        }
        if (clone.getMachineTags() != null) {
          clone.setMachineTags(new ArrayList<>(clone.getMachineTags()));
        }
        if (clone.getTags() != null) {
          clone.setTags(new ArrayList<>(clone.getTags()));
        }
        if (clone.getAddress() != null) {
          clone.setAddress((Address) BeanUtils.cloneBean(clone.getAddress()));
        }
        if (clone.getMailingAddress() != null) {
          clone.setMailingAddress((Address) BeanUtils.cloneBean(clone.getMailingAddress()));
        }
        if (clone.getContacts() != null) {
          clone.setContacts(new ArrayList<>(clone.getContacts()));
        }
        if (clone.getComments() != null) {
          clone.setComments(new ArrayList<>(clone.getComments()));
        }
      } catch (Exception e) {
        log.warn("Couldn't copy collection entity properties from bean: {}", entity);
      }
    }

    return clone;
  }

  public static Person clonePerson(Person person) {
    Person clone = new Person();
    if (person != null) {
      // copy fields
      try {
        BeanUtils.copyProperties(clone, person);

        if (clone.getIdentifiers() != null) {
          clone.setIdentifiers(new ArrayList<>(clone.getIdentifiers()));
        }
        if (clone.getMachineTags() != null) {
          clone.setMachineTags(new ArrayList<>(clone.getMachineTags()));
        }
        if (clone.getTags() != null) {
          clone.setTags(new ArrayList<>(clone.getTags()));
        }
        if (clone.getMailingAddress() != null) {
          clone.setMailingAddress((Address) BeanUtils.cloneBean(clone.getMailingAddress()));
        }
      } catch (Exception e) {
        log.warn("Couldn't copy person properties from bean: {}", person);
      }
    }

    return clone;
  }
}
