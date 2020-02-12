package org.gbif.collections.sync.ih.model;

import lombok.Data;

/** Models the Index Herbariorum metadata that is used in the WS responses. */
@Data
public class IHMetadata {
  private int hits;
  private int code;
  private String message;
}
