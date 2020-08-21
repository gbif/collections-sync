package org.gbif.collections.sync.idigbio.match.strategy;

import java.util.function.Function;

import org.gbif.collections.sync.idigbio.match.MatchResult;

public interface IDigBioMatchResultStrategy<R> extends Function<MatchResult, R> {}
