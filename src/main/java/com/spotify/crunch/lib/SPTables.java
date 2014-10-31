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
