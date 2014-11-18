package com.spotify.crunch.lib;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PTypeFamily;

import javax.annotation.Nullable;
import java.util.*;

public class Percentiles {

  /**
   * Calculate a set of percentiles for each key in a numerically-valued table.
   *
   * Percentiles are calculated on a per-key basis by counting, joining and sorting. This is highly scalable, but takes
   * 2 more map-reduce cycles than if you can guarantee that the value set will fit into memory. Use inMemory
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
  public static <K, V extends Number> PTable<K, Collection<Pair<Double, V>>> distributed(PTable<K, V> table,
          double p1, double... pn) {
    final List<Double> percentileList = createListFromVarargs(p1, pn);

    PTypeFamily ptf = table.getTypeFamily();
    PTable<K, Long> totalCounts = table.keys().count();
    PTable<K, Pair<Long, V>> countValuePairs = totalCounts.join(table);
    PTable<K, Pair<V, Long>> valueCountPairs =
            countValuePairs.mapValues(new SwapPairComponents<Long, V>(), ptf.pairs(table.getValueType(), ptf.longs()));


    return SecondarySort.sortAndApply(
            valueCountPairs,
            new DistributedPercentiles<K, V>(percentileList),
            ptf.tableOf(table.getKeyType(), ptf.collections(ptf.pairs(ptf.doubles(), table.getValueType()))));
  }

  /**
   * Calculate a set of percentiles for each key in a numerically-valued table.
   *
   * Percentiles are calculated on a per-key basis by grouping, reading the data into memory, then sorting and
   * and calculating. This is much faster than the distributed option, but if you get into the order of 10M+ per key, then
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
  public static <K, V extends Comparable> PTable<K, Collection<Pair<Double, V>>> inMemory(PTable<K, V> table,
          double p1, double... pn) {
    final List<Double> percentileList = createListFromVarargs(p1, pn);

    PTypeFamily ptf = table.getTypeFamily();

    return table
            .groupByKey()
            .parallelDo(new InMemoryPercentiles<K, V>(percentileList),
                        ptf.tableOf(table.getKeyType(), ptf.collections(ptf.pairs(ptf.doubles(), table.getValueType()))));
  }

  private static List<Double> createListFromVarargs(double p1, double[] pn) {
    final List<Double> percentileList = Lists.newArrayList(p1);
    for (double p: pn) {
      percentileList.add(p);
    }
    return percentileList;
  }

  private static class SwapPairComponents<T1, T2> extends MapFn<Pair<T1, T2>, Pair<T2, T1>> {
    @Override
    public Pair<T2, T1> map(Pair<T1, T2> input) {
      return Pair.of(input.second(), input.first());
    }
  }

  private static <V> Collection<Pair<Double, V>> findPercentiles(Iterator<V> sortedCollectionIterator,
          long collectionSize, List<Double> percentiles) {
    Collection<Pair<Double, V>> output = Lists.newArrayList();
    Multimap<Long, Double> percentileIndices = ArrayListMultimap.create();

    for (double percentile: percentiles) {
      long idx = Math.max((int) Math.ceil(percentile * collectionSize) - 1, 0);
      percentileIndices.put(idx, percentile);
    }

    long index = 0;
    while (sortedCollectionIterator.hasNext()) {
      V value = sortedCollectionIterator.next();
      if (percentileIndices.containsKey(index)) {
        for (double percentile: percentileIndices.get(index)) {
          output.add(Pair.of(percentile, value));
        }
      }
      index++;
    }
    return output;
  }

  private static class InMemoryPercentiles<K, V extends Comparable> extends
          MapFn<Pair<K, Iterable<V>>, Pair<K, Collection<Pair<Double, V>>>> {
    private final List<Double> percentileList;

    public InMemoryPercentiles(List<Double> percentiles) {
      this.percentileList = percentiles;
    }

    @Override
    public Pair<K, Collection<Pair<Double, V>>> map(Pair<K, Iterable<V>> input) {
      List<V> values = Lists.newArrayList(input.second().iterator());
      Collections.sort(values);
      return Pair.of(input.first(), findPercentiles(values.iterator(), values.size(), percentileList));
    }
  }

  private static class DistributedPercentiles<K, V> extends
          MapFn<Pair<K, Iterable<Pair<V, Long>>>, Pair<K, Collection<Pair<Double, V>>>> {
    private final List<Double> percentileList;

    public DistributedPercentiles(List<Double> percentileList) {
      this.percentileList = percentileList;
    }

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


  }
}
