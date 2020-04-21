package org.gbif.collections.sync.notification;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncConfig;
import org.gbif.collections.sync.idigbio.IDigBioRecord;
import org.gbif.collections.sync.ih.model.IHInstitution;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.Utils.removeUuidNamespace;

/** Factory to create {@link Issue}. */
@Slf4j
public class IDigBioIssueFactory extends BaseIssueFactory {

  private static final String IDIGBIO_IMPORT_LABEL = "iDigBio import";
  private static final String INVALID_ENTITY_TITLE = "Invalid iDigBio entity";
  private static final String ENTITY_CONFLICT_TITLE =
      "%s with UUID %s matches with multiple GrSciColl entities";
  private static final String OUTDATED_IH_INSTITUTION_TITLE =
      "The IH Institution with IRN %s is outdated";

  private static final UnaryOperator<String> PORTAL_URL_NORMALIZER =
      url -> {
        if (url != null && url.endsWith("/")) {
          return url.substring(0, url.length() - 1);
        }
        return url;
      };

  private static IDigBioIssueFactory instance;
  private final String iDigBioCollectionLink;

  private IDigBioIssueFactory(SyncConfig config) {
    super(config);
    this.iDigBioCollectionLink =
        PORTAL_URL_NORMALIZER.apply(config.getIDigBioConfig().getIDigBioPortalUrl() + "/%s");
  }

  public static IDigBioIssueFactory getInstance(SyncConfig config) {
    if (instance == null) {
      instance = new IDigBioIssueFactory(config);
    }

    return instance;
  }

  public static IDigBioIssueFactory fromDefaults() {
    SyncConfig.NotificationConfig notificationConfig = new SyncConfig.NotificationConfig();
    notificationConfig.setGhIssuesAssignees(Collections.emptySet());
    SyncConfig config = new SyncConfig();
    config.setNotification(notificationConfig);
    config.setIhConfig(new SyncConfig.IHConfig());
    config.setIDigBioConfig(new SyncConfig.IDigBioConfig());
    return new IDigBioIssueFactory(config);
  }

  public Issue createConflict(List<CollectionEntity> entities, IDigBioRecord iDigBioRecord) {
    return createConflict(entities, iDigBioRecord, "iDigBio collection");
  }

  public Issue createStaffConflict(Set<Person> persons, IDigBioRecord iDigBioRecord) {
    return createConflict(new ArrayList<>(persons), iDigBioRecord, "iDigBio contact");
  }

  protected <T extends CollectionEntity> Issue createConflict(
      List<T> entities, IDigBioRecord iDigBioRecord, String entityType) {
    String collectionUuid = removeUuidNamespace(iDigBioRecord.getCollectionUuid());

    // create body
    StringBuilder body =
        new StringBuilder()
            .append("The ")
            .append(createIDigBioLink(collectionUuid, entityType))
            .append(":")
            .append(formatEntity(iDigBioRecord))
            .append("have multiple candidates in GrSciColl: " + NEW_LINE)
            .append(formatRegistryEntities(entities))
            .append("A")
            .append(entityType)
            .append(
                " should be associated to only one GrSciColl entity. Please resolve the conflict.");

    return Issue.builder()
        .title(String.format(ENTITY_CONFLICT_TITLE, entityType, collectionUuid))
        .body(body.toString())
        .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(IDIGBIO_IMPORT_LABEL, syncTimestampLabel))
        .build();
  }

  public Issue createOutdatedIHInstitutionIssue(
      IHInstitution ihInstitution, IDigBioRecord iDigBioRecord) {

    StringBuilder body =
        new StringBuilder()
            .append("The ")
            .append(createIHLink(ihInstitution))
            .append(":")
            .append(formatEntity(ihInstitution))
            .append(" is less up-to-date than the one in ")
            .append(
                createIDigBioLink(
                    removeUuidNamespace(iDigBioRecord.getCollectionUuid()), "iDigbio"))
            .append(":")
            .append(formatEntity(iDigBioRecord))
            .append("\n")
            .append("Please check if this insitution should be updated in IH.");

    return Issue.builder()
        .title(String.format(OUTDATED_IH_INSTITUTION_TITLE, ihInstitution.getIrn()))
        .body(body.toString())
        .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(IDIGBIO_IMPORT_LABEL, syncTimestampLabel))
        .build();
  }

  public Issue createInvalidEntity(IDigBioRecord iDigBioRecord, String message) {
    String body = message + NEW_LINE + formatEntity(iDigBioRecord);

    return Issue.builder()
        .title(INVALID_ENTITY_TITLE)
        .body(body)
        .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
        .labels(Sets.newHashSet(IDIGBIO_IMPORT_LABEL, syncTimestampLabel))
        .build();
  }

  private String createIDigBioLink(String id, String text) {
    URI uri = URI.create(String.format(iDigBioCollectionLink, id));
    return "[" + text + "](" + uri.toString() + ")";
  }

  @Override
  String getProcessName() {
    return "iDigBio import";
  }
}
