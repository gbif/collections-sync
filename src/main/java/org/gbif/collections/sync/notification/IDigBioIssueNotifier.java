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
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.idigbio.IDigBioRecord;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.common.Utils.removeUuidNamespace;

/** Factory to create {@link Issue}. */
@Slf4j
public class IDigBioIssueNotifier extends IssueNotifier {

  private static final String IDIGBIO_IMPORT_LABEL = "iDigBio import";
  private static final String INVALID_ENTITIES_TITLE = "Invalid iDigBio entities";
  private static final String ENTITY_CONFLICT_TITLE =
      "%s with UUID %s matches with multiple GrSciColl entities";

  private static final UnaryOperator<String> PORTAL_URL_NORMALIZER =
      url -> {
        if (url != null && url.endsWith("/")) {
          return url.substring(0, url.length() - 1);
        }
        return url;
      };

  private final String iDigBioCollectionLink;

  private IDigBioIssueNotifier(IDigBioConfig iDigBioConfig) {
    super(iDigBioConfig.getSyncConfig());
    this.iDigBioCollectionLink =
        PORTAL_URL_NORMALIZER.apply(iDigBioConfig.getIDigBioPortalUrl()) + "/%s";
  }

  public static IDigBioIssueNotifier create(IDigBioConfig iDigBioConfig) {
    return new IDigBioIssueNotifier(iDigBioConfig);
  }

  public static IDigBioIssueNotifier fromDefaults() {
    SyncConfig.NotificationConfig notificationConfig = new SyncConfig.NotificationConfig();
    notificationConfig.setGhIssuesAssignees(Collections.emptySet());
    SyncConfig syncConfig = new SyncConfig();
    syncConfig.setNotification(notificationConfig);
    IDigBioConfig iDigBioConfig = new IDigBioConfig();
    iDigBioConfig.setSyncConfig(syncConfig);
    return new IDigBioIssueNotifier(iDigBioConfig);
  }

  public void createConflict(List<CollectionEntity> entities, IDigBioRecord iDigBioRecord) {
    createConflict(entities, iDigBioRecord, "iDigBio collection");
  }

  public void createStaffConflict(Set<Person> persons, IDigBioRecord iDigBioRecord) {
    createConflict(new ArrayList<>(persons), iDigBioRecord, "iDigBio contact");
  }

  protected <T extends CollectionEntity> void createConflict(
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

    Issue issue =
        Issue.builder()
            .title(String.format(ENTITY_CONFLICT_TITLE, entityType, collectionUuid))
            .body(body.toString())
            .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
            .labels(Sets.newHashSet(IDIGBIO_IMPORT_LABEL, syncTimestampLabel))
            .build();

    callExecutor.sendNotification(
        () -> githubClient.createIssue(issue),
        exceptionHandler(issue, "Failed to create fails notification"));
  }

  public void createInvalidEntitiesIssue(List<Object> invalidRecords) {
    StringBuilder body =
        new StringBuilder(
            "The following "
                + invalidRecords.size()
                + " iDigBio entities couldn't be migrated because they are missing some fields "
                + "that are required in the registry, such as the code or the name:"
                + NEW_LINE);

    for (int i = 0; i < invalidRecords.size(); i++) {
      body.append(NEW_LINE).append(i + 1).append(". ");

      if (invalidRecords.get(i) instanceof IDigBioRecord) {
        IDigBioRecord record = (IDigBioRecord) invalidRecords.get(i);
        body.append(createIDigBioLink(removeUuidNamespace(record.getCollectionUuid())));
      }

      body.append(NEW_LINE).append(formatEntity(invalidRecords.get(i)));
    }

    Issue issue =
        Issue.builder()
            .title(INVALID_ENTITIES_TITLE)
            .body(body.toString())
            .assignees(new HashSet<>(notificationConfig.getGhIssuesAssignees()))
            .labels(Sets.newHashSet(IDIGBIO_IMPORT_LABEL, syncTimestampLabel))
            .build();

    callExecutor.sendNotification(
        () -> githubClient.createIssue(issue),
        exceptionHandler(issue, "Failed to create invalid entities notification"));
  }

  private String createIDigBioLink(String id, String text) {
    URI uri = URI.create(String.format(iDigBioCollectionLink, id));
    return "[" + text + "](" + uri.toString() + ")";
  }

  private String createIDigBioLink(String id) {
    URI uri = URI.create(String.format(iDigBioCollectionLink, id));
    return "[" + uri.toString() + "](" + uri.toString() + ")";
  }

  @Override
  String getProcessName() {
    return "iDigBio import";
  }
}
