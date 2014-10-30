package com.spotify.crunch.lib;

import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTypeFamily;

/**
 * Extra high-level operations for working with PTables
 */
public class SPTables {

  /**
   * Swap the key and value part of a PTable. The original PTypes are used in the opposite order
   * @param table PTable to process
   * @param <K> Key type (will become value type)
   * @param <V> Value type (will become key type)
   * @return PType&lt;V, K&gt; containing the same data as the original
   */
  public static <K, V> PTable<V, K> swapKeyValue(PTable<K, V> table) {
    PTypeFamily ptf = table.getTypeFamily();
    return table.parallelDo(new MapFn<Pair<K, V>, Pair<V, K>>() {
      @Override
      public Pair<V, K> map(Pair<K, V> input) {
        return Pair.of(input.second(), input.first());
      }
    }, ptf.tableOf(table.getValueType(), table.getKeyType()));
  }

  /**
   * When creating toplists, it is often required to sort by count descending. As some sort operations don't support
   * order (such as SecondarySort), this method will negate counts so that a natural-ordered sort will produce a
   * descending order.
   * @param table PTable to process
   * @param <K> key type
   * @return PTable of the same format with the value negated
   */
  public static <K> PTable<K, Long> negateCounts(PTable<K, Long> table) {
    return table.parallelDo(new MapFn<Pair<K, Long>, Pair<K, Long>>() {
      @Override
      public Pair<K, Long> map(Pair<K, Long> input) {
        return Pair.of(input.first(), -input.second());
      }
    }, table.getPTableType());
  }
}
