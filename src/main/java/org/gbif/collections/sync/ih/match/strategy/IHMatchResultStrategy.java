package org.gbif.collections.sync.ih.match.strategy;

import java.util.function.Function;

import org.gbif.collections.sync.ih.match.MatchResult;

public interface IHMatchResultStrategy<R> extends Function<MatchResult, R> {}
