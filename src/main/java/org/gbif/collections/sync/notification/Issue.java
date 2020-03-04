package org.gbif.collections.sync.notification;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Issue {
  private String title;
  private String body;
  private List<String> assignees;
  private List<String> labels;
}
