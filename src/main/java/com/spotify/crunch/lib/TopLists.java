package com.spotify.crunch.lib;

import com.google.common.collect.Lists;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import java.util.Collection;
import java.util.Iterator;

/**
 * Tools for creating top lists of items in PTables and PCollections
 */
public class TopLists {

  /**
   * Create a top-list of elements in the provided PTable, categorised by the key of the input table and using the count
   * of the value part of the input table. Example: if input = Table(Country, Track), then this will give you the most
   * common n tracks for each country.
   * @param input table of X Y pairs
   * @param n How many Y values to include in the toplist per X (this will be in memory, so don't make this ridiculous)
   * @param <X> group type
   * @param <Y> value type
   * @return table of each unique X value mapped to a collection of (count, Y) pairs
   */
  public static <X, Y> PTable<X, Collection<Pair<Long, Y>>> topNYbyX(PTable<X, Y> input, final int n) {
    final PType<X> xType = input.getKeyType();
    final PType<Y> yType = input.getValueType();
    PTypeFamily f = xType.getFamily();
    PTable<X, Pair<Long, Y>> counted = input.count().parallelDo(new MapFn<Pair<Pair<X, Y>, Long>, Pair<X, Pair<Long, Y>>>() {
      @Override
      public Pair<X, Pair<Long, Y>> map(Pair<Pair<X, Y>, Long> input) {
        return Pair.of(input.first().first(), Pair.of(-input.second(), input.first().second()));
      }
    }, f.tableOf(xType, f.pairs(f.longs(), yType)));
    return SecondarySort.sortAndApply(counted, new MapFn<Pair<X, Iterable<Pair<Long, Y>>>, Pair<X, Collection<Pair<Long, Y>>>>() {

      private PTableType<Long, Y> tableType;

      @Override
      public void initialize() {
        PTypeFamily ptf = yType.getFamily();
        tableType = ptf.tableOf(ptf.longs(), yType);
        tableType.initialize(getConfiguration());
      }

      @Override
      public Pair<X, Collection<Pair<Long, Y>>> map(Pair<X, Iterable<Pair<Long, Y>>> input) {
        Collection<Pair<Long, Y>> values = Lists.newArrayList();
        Iterator<Pair<Long, Y>> iter = input.second().iterator();
        for (int i = 0; i < n; i++) {
          if (!iter.hasNext()) {
            break;
          }
          Pair<Long, Y> pair = PTables.getDetachedValue(tableType, iter.next());
          values.add(Pair.of(-pair.first(), pair.second()));
        }
        return Pair.of(input.first(), values);
      }
    }, f.tableOf(xType, f.collections(f.pairs(f.longs(), yType))));
  }

  /**
   * Create a list of unique items in the input collection with their count, sorted descending by their frequency.
   * @param input input collection
   * @param <X> record type
   * @return global toplist
   */
  public static <X> PTable<X, Long> globalToplist(PCollection<X> input) {
    return SPTables.negateCounts(SPTables.negateCounts(input.count()).groupByKey(1).ungroup());
  }
}
