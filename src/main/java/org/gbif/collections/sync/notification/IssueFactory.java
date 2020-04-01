package org.gbif.collections.sync.notification;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncConfig;
import org.gbif.collections.sync.ih.IHSyncResult;
import org.gbif.collections.sync.ih.model.IHEntity;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.UnaryOperator;

import com.google.common.collect.Sets;

/** Factory to create {@link Issue}. */
public class IssueFactory {

  public static final String IH_SYNC_LABEL = "GrSciColl-IH sync";
  private static final String INVALID_ENTITY_TITLE = "Invalid IH entity with IRN %s";
  private static final String ENTITY_CONFLICT_TITLE =
      "IH %s with IRN %s matches with multiple GrSciColl entities";
  private static final String FAILS_TITLE =
      "Some operations have failed updating the registry in the IH sync";
  private static final String NEW_LINE = "\n";
  private static final String CODE_SEPARATOR = "```";
  private static final String FAIL_LABEL = "IH sync fail";

  private static final UnaryOperator<String> PORTAL_URL_NORMALIZER =
      url -> {
        if (url != null && url.endsWith("/")) {
          return url.substring(0, url.length() - 1);
        }
        return url;
      };

  private static IssueFactory instance;

  private final SyncConfig.NotificationConfig config;
  private final String ihInstitutionLink;
  private final String ihStaffLink;
  private final String registryInstitutionLink;
  private final String registryCollectionLink;
  private final String registryPersonLink;
  private final String syncTimestampLabel;

  private IssueFactory(SyncConfig.NotificationConfig config) {
    this.config = config;
    this.ihInstitutionLink =
        PORTAL_URL_NORMALIZER.apply(config.getIhPortalUrl()) + "/ih/herbarium-details/?irn=%s";
    this.ihStaffLink =
        PORTAL_URL_NORMALIZER.apply(config.getIhPortalUrl()) + "/ih/person-details/?irn=%s";
    this.registryInstitutionLink =
        PORTAL_URL_NORMALIZER.apply(config.getRegistryPortalUrl()) + "/institution/%s";
    this.registryCollectionLink =
        PORTAL_URL_NORMALIZER.apply(config.getRegistryPortalUrl()) + "/collection/%s";
    this.registryPersonLink =
        PORTAL_URL_NORMALIZER.apply(config.getRegistryPortalUrl()) + "/person/%s";
    syncTimestampLabel =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  public static IssueFactory getInstance(SyncConfig config) {
    if (instance == null) {
      instance = new IssueFactory(config.getNotification());
    }

    return instance;
  }

  public static IssueFactory fromDefaults() {
    SyncConfig.NotificationConfig config = new SyncConfig.NotificationConfig();
    config.setGhIssuesAssignees(Collections.emptySet());
    return new IssueFactory(config);
  }

  public Issue createConflict(List<CollectionEntity> entities, IHInstitution ihInstitution) {
    return createConflict(entities, ihInstitution, EntityType.IH_INSTITUTION);
  }

  public Issue createStaffConflict(Set<Person> persons, IHStaff ihStaff) {
    return createConflict(new ArrayList<>(persons), ihStaff, EntityType.IH_STAFF);
  }

  private <T extends CollectionEntity> Issue createConflict(
      List<T> entities, IHEntity ihEntity, EntityType ihEntityType) {
    // create body
    StringBuilder body =
        new StringBuilder()
            .append("The IH ")
            .append(createLink(ihEntity.getIrn(), ihEntityType))
            .append(":")
            .append(formatEntity(ihEntity))
            .append("have multiple candidates in GrSciColl: " + NEW_LINE)
            .append(formatEntities(entities))
            .append("A IH ")
            .append(ihEntityType.title)
            .append(
                " should be associated to only one GrSciColl entity. Please resolve the conflict.");

    return Issue.builder()
        .title(String.format(ENTITY_CONFLICT_TITLE, ihEntityType.title, ihEntity.getIrn()))
        .body(body.toString())
        .assignees(new HashSet<>(config.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(IH_SYNC_LABEL, syncTimestampLabel))
        .build();
  }

  public <T extends IHEntity> Issue createInvalidEntity(T entity, String message) {
    String body =
        message
            + NEW_LINE
            + createLink(entity.getIrn(), getEntityType(entity), true)
            + formatEntity(entity);

    return Issue.builder()
        .title(String.format(INVALID_ENTITY_TITLE, entity.getIrn()))
        .body(body)
        .assignees(new HashSet<>(config.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(IH_SYNC_LABEL, syncTimestampLabel))
        .build();
  }

  public Issue createFailsNotification(List<IHSyncResult.FailedAction> fails) {
    // create body
    StringBuilder body = new StringBuilder();
    body.append(
        "The next operations have failed when updating the registry with the IH data. Please check them: ");

    fails.forEach(
        f ->
            body.append(NEW_LINE)
                .append(CODE_SEPARATOR)
                .append(NEW_LINE)
                .append("Error: ")
                .append(f.getMessage())
                .append(NEW_LINE)
                .append("Entity: ")
                .append(f.getEntity())
                .append(NEW_LINE)
                .append(CODE_SEPARATOR)
                .append(NEW_LINE));

    return Issue.builder()
        .title(FAILS_TITLE)
        .body(body.toString())
        .assignees(new HashSet<>(config.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(FAIL_LABEL, syncTimestampLabel))
        .build();
  }

  private String formatEntity(Object entity) {
    return NEW_LINE
        + CODE_SEPARATOR
        + NEW_LINE
        + entity.toString()
        + NEW_LINE
        + CODE_SEPARATOR
        + NEW_LINE;
  }

  private <T extends CollectionEntity> String formatEntities(List<T> entities) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < entities.size(); i++) {
      T entity = entities.get(i);
      sb.append(i + 1)
          .append(". ")
          .append(createLink(entity.getKey().toString(), getEntityType(entity), true))
          .append(NEW_LINE);
    }

    sb.append(NEW_LINE).append(CODE_SEPARATOR).append(NEW_LINE);

    for (int i = 0; i < entities.size(); i++) {
      T entity = entities.get(i);
      sb.append(i + 1).append(". ").append(entity.toString()).append(NEW_LINE);
    }

    sb.append(CODE_SEPARATOR).append(NEW_LINE);
    return sb.toString();
  }

  private String createLink(String id, EntityType entityType) {
    return createLink(id, entityType, false);
  }

  private String createLink(String id, EntityType entityType, boolean omitText) {
    String linkTemplate;
    if (entityType == EntityType.IH_INSTITUTION) {
      linkTemplate = ihInstitutionLink;
    } else if (entityType == EntityType.IH_STAFF) {
      linkTemplate = ihStaffLink;
    } else if (entityType == EntityType.INSTITUTION) {
      linkTemplate = registryInstitutionLink;
    } else if (entityType == EntityType.COLLECTION) {
      linkTemplate = registryCollectionLink;
    } else {
      linkTemplate = registryPersonLink;
    }

    URI uri = URI.create(String.format(linkTemplate, id));
    String text = omitText ? uri.toString() : entityType.title;

    return "[" + text + "](" + uri.toString() + ")";
  }

  private EntityType getEntityType(Object entity) {
    if (entity instanceof Institution) {
      return EntityType.INSTITUTION;
    }
    if (entity instanceof Collection) {
      return EntityType.COLLECTION;
    }
    if (entity instanceof Person) {
      return EntityType.PERSON;
    }
    if (entity instanceof IHInstitution) {
      return EntityType.IH_INSTITUTION;
    }
    if (entity instanceof IHStaff) {
      return EntityType.IH_STAFF;
    }

    throw new IllegalArgumentException("Entity not supported: " + entity);
  }

  private enum EntityType {
    IH_INSTITUTION("institution"),
    IH_STAFF("staff"),
    INSTITUTION("institution"),
    COLLECTION("collection"),
    PERSON("person");

    String title;

    EntityType(String title) {
      this.title = title;
    }
  }
}
