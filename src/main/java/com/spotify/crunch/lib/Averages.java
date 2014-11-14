/*
 * Copyright 2014 Spotify AB. All rights reserved.
 *
 * The contents of this file are licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.crunch.lib;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.crunch.*;

import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PTypeFamily;

import javax.annotation.Nullable;
import java.util.*;

import static org.apache.crunch.fn.Aggregators.*;

public class Averages {

  /**
   * Calculate the mean average value by key for a table with numeric values.
   * @param table PTable of (key, value) pairs to operate on
   * @param <K> Key type, can be any type
   * @param <V> Value type, must be numeric (ie. extend java.lang.Number)
   * @return PTable&lt;K, Double&gt; of (key, mean(values)) pairs
   */
  public static <K, V extends Number> PTable<K, Double> meanValue(PTable<K, V> table) {
    PTypeFamily ptf = table.getTypeFamily();
    PTable<K, Pair<Double, Long>> withCounts = table.mapValues(new MapFn<V, Pair<Double, Long>>() {

      @Override
      public Pair<Double, Long> map(V input) {
        return Pair.of(input.doubleValue(), 1L);
      }
    }, ptf.pairs(ptf.doubles(), ptf.longs()));
    PGroupedTable<K, Pair<Double, Long>> grouped = withCounts.groupByKey();

    return grouped.combineValues(pairAggregator(SUM_DOUBLES(), SUM_LONGS()))
            .mapValues(new MapFn<Pair<Double, Long>, Double>() {
             @Override
             public Double map(Pair<Double, Long> input) {
               return input.first() / input.second();
             }
            }, ptf.doubles());
  }

  /**
   * Calculate a set of percentiles for each key in a numerically-valued table.
   *
   * Percentiles are calculated on a per-key basis by counting, joining and sorting. This is highly scalable, but takes
   * 2 more map-reduce cycles than if you can guarantee that the value set will fit into memory. Use inMemoryPercentiles
   * if you have less than the order of 10M values per key.
   *
   * The percentile definition that we use here is the "nearest rank" defined here:
   * http://en.wikipedia.org/wiki/Percentile#Definition
   *
   * @param table numerically-valued PTable
   * @param p1 First percentile (in the range 0.0 - 1.0)
   * @param pn More percentiles (in the range 0.0 - 1.0)
   * @param <K> Key type of the table
   * @param <V> Value type of the table (must extends java.lang.Number)
   * @return PTable of each key with a collection of pairs of the percentile provided and it's result.
   */
  public static <K, V extends Number> PTable<K, Collection<Pair<Double, V>>> scalablePercentiles(PTable<K, V> table,
          double p1, double... pn) {
    final List<Double> percentileList = createListFromVarargs(p1, pn);

    PTypeFamily ptf = table.getTypeFamily();
    PTable<K, Long> totalCounts = table.keys().count();
    PTable<K, Pair<Long, V>> countValuePairs = totalCounts.join(table);
    PTable<K, Pair<V, Long>> valueCountPairs = countValuePairs.mapValues(new MapFn<Pair<Long, V>, Pair<V, Long>>() {
      @Override
      public Pair<V, Long> map(Pair<Long, V> input) {
        return Pair.of(input.second(), input.first());
      }
    }, ptf.pairs(table.getValueType(), ptf.longs()));


    return SecondarySort.sortAndApply(valueCountPairs, new MapFn<Pair<K, Iterable<Pair<V, Long>>>, Pair<K, Collection<Pair<Double, V>>>>() {
      @Override
      public Pair<K, Collection<Pair<Double, V>>> map(Pair<K, Iterable<Pair<V, Long>>> input) {

        PeekingIterator<Pair<V, Long>> iterator = Iterators.peekingIterator(input.second().iterator());
        long count = iterator.peek().second();

        Iterator<V> valueIterator = Iterators.transform(iterator, new Function<Pair<V, Long>, V>() {
          @Override
          public V apply(@Nullable Pair<V, Long> input) {
            return input.first();
          }
        });

        Collection<Pair<Double, V>> output = findPercentiles(valueIterator, count, percentileList);
        return Pair.of(input.first(), output);
      }


    }, ptf.tableOf(table.getKeyType(), ptf.collections(ptf.pairs(ptf.doubles(), table.getValueType()))));
  }

  /**
   * Calculate a set of percentiles for each key in a numerically-valued table.
   *
   * Percentiles are calculated on a per-key basis by grouping, reading the data into memory, then sorting and
   * and calculating. This is much faster than the scalable option, but if you get into the order of 10M+ per key, then
   * performance might start to degrade or even cause OOMs.
   *
   * The percentile definition that we use here is the "nearest rank" defined here:
   * http://en.wikipedia.org/wiki/Percentile#Definition
   *
   * @param table numerically-valued PTable
   * @param p1 First percentile (in the range 0.0 - 1.0)
   * @param pn More percentiles (in the range 0.0 - 1.0)
   * @param <K> Key type of the table
   * @param <V> Value type of the table (must extends java.lang.Number)
   * @return PTable of each key with a collection of pairs of the percentile provided and it's result.
   */
  public static <K, V extends Comparable> PTable<K, Collection<Pair<Double, V>>> inMemoryPercentiles(PTable<K, V> table,
          double p1, double... pn) {
    final List<Double> percentileList = createListFromVarargs(p1, pn);

    PTypeFamily ptf = table.getTypeFamily();

    return table
            .groupByKey()
            .parallelDo(new MapFn<Pair<K, Iterable<V>>, Pair<K, Collection<Pair<Double, V>>>>() {
              @Override
              public Pair<K, Collection<Pair<Double, V>>> map(Pair<K, Iterable<V>> input) {
                List<V> values = Lists.newArrayList(input.second().iterator());
                Collections.sort(values);
                return Pair.of(input.first(), findPercentiles(values.iterator(), values.size(), percentileList));
              }
            }, ptf.tableOf(table.getKeyType(), ptf.collections(ptf.pairs(ptf.doubles(), table.getValueType()))));
  }

  private static List<Double> createListFromVarargs(double p1, double[] pn) {
    final List<Double> percentileList = Lists.newArrayList(p1);
    for (double p: pn) {
      percentileList.add(p);
    }
    return percentileList;
  }

  private static <V> Collection<Pair<Double, V>> findPercentiles(Iterator<V> sortedCollectionIterator,
          long collectionSize, List<Double> percentiles) {
    Collection<Pair<Double, V>> output = Lists.newArrayList();
    Map<Long, Double> percentileIndices = Maps.newHashMap();
    for (double percentile: percentiles) {
      long idx = Math.min((int) Math.floor(percentile * collectionSize), collectionSize - 1);
      percentileIndices.put(idx, percentile);
    }

    long index = 0;
    while (sortedCollectionIterator.hasNext()) {
      V value = sortedCollectionIterator.next();
      if (percentileIndices.containsKey(index)) {
        output.add(Pair.of(percentileIndices.get(index), value));
      }
      index++;
    }
    return output;
  }
}
