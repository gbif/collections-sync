package org.gbif.collections.sync.common.converter;

import lombok.Builder;
import lombok.Data;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.descriptors.DescriptorGroup;

import java.nio.file.Path;

@Data
@Builder
public class ConvertedCollection {
  Collection collection;
  DescriptorGroup collectionSummary;
  Path collectionSummaryFile;
  DescriptorGroup importantCollectors;
  Path importantCollectorsFile;
}
