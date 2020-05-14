package org.gbif.collections.sync.notification;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.ih.model.IHEntity;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

/** Factory to create {@link Issue}. */
@Slf4j
public class IHIssueFactory extends BaseIssueFactory {

  public static final String IH_SYNC_LABEL = "GrSciColl-IH sync";
  private static final String INVALID_ENTITY_TITLE = "Invalid IH entity with IRN %s";
  private static final String ENTITY_CONFLICT_TITLE =
      "IH %s with IRN %s matches with multiple GrSciColl entities";

  private final String ihInstitutionLink;
  private final String ihStaffLink;

  private IHIssueFactory(IHConfig config) {
    super(config.getSyncConfig());
    this.ihInstitutionLink =
        PORTAL_URL_NORMALIZER.apply(config.getIhPortalUrl()) + "/ih/herbarium-details/?irn=%s";
    this.ihStaffLink =
        PORTAL_URL_NORMALIZER.apply(config.getIhPortalUrl()) + "/ih/person-details/?irn=%s";
  }

  public static IHIssueFactory create(IHConfig config) {
    return new IHIssueFactory(config);
  }

  public static IHIssueFactory fromDefaults() {
    SyncConfig.NotificationConfig notificationConfig = new SyncConfig.NotificationConfig();
    notificationConfig.setGhIssuesAssignees(Collections.emptySet());
    SyncConfig syncConfig = new SyncConfig();
    syncConfig.setNotification(notificationConfig);
    IHConfig ihConfig = new IHConfig();
    ihConfig.setSyncConfig(syncConfig);
    return new IHIssueFactory(ihConfig);
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

  protected <T extends IHEntity> String createIHLink(T entity) {
    String linkTemplate;
    String text;
    if (entity instanceof IHInstitution) {
      linkTemplate = ihInstitutionLink;
      text = "institution";
    } else {
      linkTemplate = ihStaffLink;
      text = "staff";
    }

    URI uri = URI.create(String.format(linkTemplate, entity.getIrn()));
    return "[" + text + "](" + uri.toString() + ")";
  }

  @Override
  String getProcessName() {
    return "IH sync";
  }
}
