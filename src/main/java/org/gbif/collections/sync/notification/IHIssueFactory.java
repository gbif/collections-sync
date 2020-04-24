package org.gbif.collections.sync.notification;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncConfig;
import org.gbif.collections.sync.SyncConfig.IHConfig;
import org.gbif.collections.sync.ih.model.IHEntity;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.*;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

/** Factory to create {@link Issue}. */
@Slf4j
public class IHIssueFactory extends BaseIssueFactory {

  public static final String IH_SYNC_LABEL = "GrSciColl-IH sync";
  private static final String INVALID_ENTITY_TITLE = "Invalid IH entity with IRN %s";
  private static final String ENTITY_CONFLICT_TITLE =
      "IH %s with IRN %s matches with multiple GrSciColl entities";

  private static IHIssueFactory instance;

  private IHIssueFactory(SyncConfig config) {
    super(config);
  }

  public static IHIssueFactory getInstance(SyncConfig config) {
    if (instance == null) {
      instance = new IHIssueFactory(config);
    }

    return instance;
  }

  public static IHIssueFactory fromDefaults() {
    SyncConfig.NotificationConfig notificationConfig = new SyncConfig.NotificationConfig();
    notificationConfig.setGhIssuesAssignees(Collections.emptySet());
    SyncConfig config = new SyncConfig();
    config.setNotification(notificationConfig);
    config.setIhConfig(new IHConfig());
    return new IHIssueFactory(config);
  }

  public Issue createConflict(List<CollectionEntity> entities, IHInstitution ihInstitution) {
    return createConflict(entities, ihInstitution, "institution");
  }

  public Issue createStaffConflict(Set<Person> persons, IHStaff ihStaff) {
    return createConflict(new ArrayList<>(persons), ihStaff, "staff");
  }

  private <T extends CollectionEntity> Issue createConflict(
      List<T> entities, IHEntity ihEntity, String ihEntityType) {
    // create body
    StringBuilder body =
        new StringBuilder()
            .append("The IH ")
            .append(createIHLink(ihEntity))
            .append(":")
            .append(formatEntity(ihEntity))
            .append("have multiple candidates in GrSciColl: " + NEW_LINE)
            .append(formatRegistryEntities(entities))
            .append("A IH ")
            .append(ihEntityType)
            .append(
                " should be associated to only one GrSciColl entity. Please resolve the conflict.");

    return Issue.builder()
        .title(String.format(ENTITY_CONFLICT_TITLE, ihEntityType, ihEntity.getIrn()))
        .body(body.toString())
        .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(IH_SYNC_LABEL, syncTimestampLabel))
        .build();
  }

  public <T extends IHEntity> Issue createInvalidEntity(T entity, String message) {
    String body = message + NEW_LINE + createIHLink(entity) + formatEntity(entity);

    return Issue.builder()
        .title(String.format(INVALID_ENTITY_TITLE, entity.getIrn()))
        .body(body)
        .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(IH_SYNC_LABEL, syncTimestampLabel))
        .build();
  }

  @Override
  String getProcessName() {
    return "IH sync";
  }
}