package org.gbif.collections.sync;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Person;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SyncResultExporter {

  private static final String SECTION_SEPARATOR =
      "##########################################################################";
  private static final String SUBSECTION_SEPARATOR =
      "--------------------------------------------------------------------------";
  private static final String MATCH_SEPARATOR =
      "  -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_";
  private static final String LINE_STARTER = ">";
  private static final String SMALL_INDENT = "\t\t";
  private static final String BIG_INDENT = "\t\t\t\t";

  public static void exportResultsToFile(SyncResult result, Path filePath) {

    try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {

      // Summary
      printWithNewLineAfter(writer, "IH Sync " + LocalDateTime.now());
      printWithNewLineAfter(writer, "Summary:");
      printWithNewLineAfter(writer, SUBSECTION_SEPARATOR);
      printWithNewLineAfter(
          writer, "Collection Only Matches: " + result.getCollectionOnlyMatches().size());
      printWithNewLineAfter(
          writer, "Institution Only Matches: " + result.getInstitutionOnlyMatches().size());
      printWithNewLineAfter(
          writer, "Institution & Collection Matches: " + result.getInstAndCollMatches().size());
      printWithNewLineAfter(writer, "No Matches: " + result.getNoMatches().size());
      printWithNewLineAfter(writer, "Conflicts: " + result.getConflicts().size());
      printWithNewLineAfter(writer, "Failed Actions: " + result.getFailedActions().size());
      printWithNewLineAfter(writer, "Invalid entities: " + result.getInvalidEntities().size());
      printWithNewLineAfter(writer, "Outdated entities: " + result.getOutdatedEntities().size());

      Counts counts = getSummaryCounts(result);
      writer.newLine();
      printWithNewLineAfter(writer, "Institutions created: " + counts.institutionsCreated);
      printWithNewLineAfter(writer, "Institutions updated: " + counts.institutionsUpdated);
      printWithNewLineAfter(writer, "Institutions no change: " + counts.institutionsNoChange);
      printWithNewLineAfter(writer, "Collections created: " + counts.collectionsCreated);
      printWithNewLineAfter(writer, "Collections updated: " + counts.collectionsUpdated);
      printWithNewLineAfter(writer, "Collections no change: " + counts.collectionsNoChange);
      printWithNewLineAfter(writer, "Persons created: " + counts.countStaffMatch.personsCreated);
      printWithNewLineAfter(writer, "Persons updated: " + counts.countStaffMatch.personsUpdated);
      printWithNewLineAfter(writer, "Persons no change: " + counts.countStaffMatch.personsNoChange);
      printWithNewLineAfter(writer, "Persons removed: " + counts.countStaffMatch.personsRemoved);
      printWithNewLineAfter(writer, "Staff conflicts: " + counts.countStaffMatch.staffConflicts);

      writer.newLine();
      writer.newLine();

      // Collection Only Matches
      printSectionTitle(
          writer, "Collection Only Matches: " + result.getCollectionOnlyMatches().size());
      result
          .getCollectionOnlyMatches()
          .forEach(
              m -> {
                printMatchTitle(writer, "Collection Match");
                printEntityMatch(writer, m.getMatchedCollection());
                printStaffMatch(writer, m.getStaffMatch());
              });

      writer.newLine();
      printSectionTitle(
          writer, "Institution Only Matches: " + result.getInstitutionOnlyMatches().size());
      result
          .getInstitutionOnlyMatches()
          .forEach(
              m -> {
                printMatchTitle(writer, "Institution Match");
                printEntityMatch(writer, m.getMatchedInstitution());
                printEntity(writer, "New Collection", m.getNewCollection());
                printStaffMatch(writer, m.getStaffMatch());
              });

      writer.newLine();
      printSectionTitle(
          writer, "Institution & Collection Matches: " + result.getInstAndCollMatches().size());
      result
          .getInstAndCollMatches()
          .forEach(
              m -> {
                printMatchTitle(writer, "Institution & Collection Match");
                printEntityMatch(writer, m.getMatchedInstitution());
                printEntityMatch(writer, m.getMatchedCollection());
                printStaffMatch(writer, m.getStaffMatch());
              });

      writer.newLine();
      printSectionTitle(writer, "No Matches: " + result.getNoMatches().size());
      result
          .getNoMatches()
          .forEach(
              m -> {
                printMatchTitle(writer, "No Match");
                printEntity(writer, "New Institution", m.getNewInstitution());
                printEntity(writer, "New Collection", m.getNewCollection());
                printStaffMatch(writer, m.getStaffMatch());
              });

      // Conflicts
      printSection(writer, "General Conflicts", result.getConflicts());

      // fails
      printSection(writer, "Failed Actions", result.getFailedActions());

      // fails
      printSection(writer, "Invalid entities", result.getInvalidEntities());

      // outdated entities
      printSection(writer, "Outdated entities", result.getOutdatedEntities());

    } catch (Exception e) {
      log.warn("Couldn't save diff results", e);
    }
  }

  private static void printMatchTitle(BufferedWriter writer, String title) {
    try {
      writer.write(title);
      writer.write(MATCH_SEPARATOR);
      writer.newLine();
    } catch (IOException e) {
      log.warn("Couldn't print title {}", title, e);
    }
  }

  private static void printSectionTitle(BufferedWriter writer, String title) {
    try {
      writer.write(title);
      writer.newLine();
      writer.write(SECTION_SEPARATOR);
      writer.newLine();
    } catch (IOException e) {
      log.warn("Couldn't print title {}", title, e);
    }
  }

  private static void printSubsectionTitle(BufferedWriter writer, String subtitle) {
    try {
      writer.write(BIG_INDENT + subtitle);
      writer.newLine();
      writer.write(BIG_INDENT + SUBSECTION_SEPARATOR);
      writer.newLine();
    } catch (IOException e) {
      log.warn("Couldn't print subtitle {}", subtitle, e);
    }
  }

  private static <T> void printSection(BufferedWriter writer, String title, List<T> collection)
      throws IOException {
    writer.newLine();
    printSectionTitle(writer, title + ": " + collection.size());
    printCollection(writer, collection);
    writer.newLine();
  }

  private static <T> void printSubsection(
      BufferedWriter writer, String title, Collection<T> collection) throws IOException {
    writer.newLine();
    printSubsectionTitle(writer, title + ": " + collection.size());
    printCollectionSubsection(writer, collection);
    writer.newLine();
  }

  private static <T> void printCollection(BufferedWriter writer, List<T> collection)
      throws IOException {
    for (T e : collection) {
      writer.write(LINE_STARTER);
      printWithNewLineAfter(writer, e.toString());
    }
  }

  private static <T> void printCollectionSubsection(BufferedWriter writer, Collection<T> collection)
      throws IOException {
    for (T e : collection) {
      writer.write(BIG_INDENT + LINE_STARTER);
      printWithNewLineAfter(writer, BIG_INDENT + e.toString());
    }
  }

  private static void printStaffMatch(BufferedWriter writer, SyncResult.StaffMatch staffMatch) {
    try {
      writer.newLine();
      printWithNewLineAfter(writer, SMALL_INDENT + ">>> Associated Staff");
      printSubsection(writer, "New Persons", staffMatch.getNewPersons());
      printSubsectionTitle(writer, "Matched Persons: " + staffMatch.getMatchedPersons().size());
      staffMatch.getMatchedPersons().stream()
          .sorted(Comparator.comparing(SyncResult.EntityMatch::isUpdate))
          .forEach(m -> printStaffEntityMatch(writer, m));
      printSubsection(writer, "Removed Persons", staffMatch.getRemovedPersons());
      printSubsection(writer, "Staff Conflicts", staffMatch.getConflicts());
      writer.newLine();
    } catch (IOException e) {
      log.warn("Couldn't print staff match {}", staffMatch, e);
    }
  }

  private static <T extends CollectionEntity> void printEntityMatch(
      BufferedWriter writer, SyncResult.EntityMatch<T> entityMatch) {
    try {
      writer.write(LINE_STARTER + " ");
      if (entityMatch.isUpdate()) {
        printWithNewLineAfter(writer, "Entity Updated:");
        printWithNewLineAfter(writer, SMALL_INDENT + "OLD: " + entityMatch.getMatched());
        writer.newLine();
        printWithNewLineAfter(writer, SMALL_INDENT + "NEW: " + entityMatch.getMerged());
      } else {
        printWithNewLineAfter(writer, "Entity No Change: " + entityMatch.getMatched());
      }
      writer.newLine();
    } catch (IOException e) {
      log.warn("Couldn't print entity match {}", entityMatch, e);
    }
  }

  private static <T extends CollectionEntity> void printStaffEntityMatch(
      BufferedWriter writer, SyncResult.EntityMatch<T> entityMatch) {
    try {
      if (entityMatch.isUpdate()) {
        printWithNewLineAfter(writer, BIG_INDENT + LINE_STARTER + " Staff Updated:");
        printWithNewLineAfter(writer, BIG_INDENT + "OLD: " + entityMatch.getMatched());
        writer.newLine();
        printWithNewLineAfter(writer, BIG_INDENT + "NEW: " + entityMatch.getMerged());
      } else {
        printWithNewLineAfter(
            writer, BIG_INDENT + LINE_STARTER + " Staff No Change: " + entityMatch.getMatched());
      }
      writer.newLine();
    } catch (IOException e) {
      log.warn("Couldn't print entity match {}", entityMatch, e);
    }
  }

  private static <T extends CollectionEntity> void printEntity(
      BufferedWriter writer, String text, T entity) {
    try {
      printWithNewLineAfter(writer, text);
      printWithNewLineAfter(writer, entity.toString());
    } catch (IOException e) {
      log.warn("Couldn't print entity {}", entity, e);
    }
  }

  private static void printWithNewLineAfter(BufferedWriter writer, String text) throws IOException {
    writer.write(text);
    writer.newLine();
  }

  private static Counts getSummaryCounts(SyncResult result) {
    Counts counts = new Counts();

    Consumer<SyncResult.StaffMatch> countStaff =
        staffMatch -> {
          if (staffMatch != null) {
            counts.countStaffMatch.personsCreated += staffMatch.getNewPersons().size();
            counts.countStaffMatch.personsRemoved += staffMatch.getRemovedPersons().size();
            counts.countStaffMatch.staffConflicts += staffMatch.getConflicts().size();
            for (SyncResult.EntityMatch<Person> p : staffMatch.getMatchedPersons()) {
              if (p.isUpdate()) {
                counts.countStaffMatch.personsUpdated++;
              } else {
                counts.countStaffMatch.personsNoChange++;
              }
            }
          }
        };

    for (SyncResult.CollectionOnlyMatch m : result.getCollectionOnlyMatches()) {
      if (m.getMatchedCollection().isUpdate()) {
        counts.collectionsUpdated++;
      } else {
        counts.collectionsNoChange++;
      }
      countStaff.accept(m.getStaffMatch());
    }

    for (SyncResult.InstitutionOnlyMatch m : result.getInstitutionOnlyMatches()) {
      if (m.getMatchedInstitution().isUpdate()) {
        counts.institutionsUpdated++;
      } else {
        counts.institutionsNoChange++;
      }

      if (m.getNewCollection() != null) {
        counts.collectionsCreated++;
      }
      countStaff.accept(m.getStaffMatch());
    }

    for (SyncResult.InstitutionAndCollectionMatch m : result.getInstAndCollMatches()) {
      if (m.getMatchedInstitution().isUpdate()) {
        counts.institutionsUpdated++;
      } else {
        counts.institutionsNoChange++;
      }

      if (m.getMatchedCollection().isUpdate()) {
        counts.collectionsUpdated++;
      } else {
        counts.collectionsNoChange++;
      }
      countStaff.accept(m.getStaffMatch());
    }

    for (SyncResult.NoEntityMatch m : result.getNoMatches()) {
      if (m.getNewInstitution() != null) {
        counts.institutionsCreated++;
      }
      if (m.getNewCollection() != null) {
        counts.collectionsCreated++;
      }
      countStaff.accept(m.getStaffMatch());
    }

    return counts;
  }

  private static class Counts {
    int institutionsCreated = 0;
    int institutionsUpdated = 0;
    int institutionsNoChange = 0;
    int collectionsCreated = 0;
    int collectionsUpdated = 0;
    int collectionsNoChange = 0;
    CountStaffMatch countStaffMatch = new CountStaffMatch();

    private static class CountStaffMatch {
      private int personsCreated;
      private int personsUpdated;
      private int personsNoChange;
      private int personsRemoved;
      private int staffConflicts;
    }
  }
}
