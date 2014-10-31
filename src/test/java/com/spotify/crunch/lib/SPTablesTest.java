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

import com.google.common.collect.ImmutableMap;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.Map;

import static org.apache.crunch.types.avro.Avros.*;

import static org.junit.Assert.*;

public class SPTablesTest {
  @Test
  public void testSwapKeyValue() {
    PTable<String, Long> table = MemPipeline.typedTableOf(tableOf(strings(), longs()), "hello", 14L, "goodbye", 21L);
    PTable<Long, String> actual = SPTables.swapKeyValue(table);
    Map<Long, String> expected = ImmutableMap.of(14L, "hello", 21L, "goodbye");
    assertEquals(expected, actual.materializeToMap());
  }

  @Test
  public void testNegateCounts() {
    PCollection<String> data = MemPipeline.typedCollectionOf(strings(), "a", "a", "a", "b", "b");
    Map<String, Long> actual = SPTables.negateCounts(data.count()).materializeToMap();
    Map<String, Long> expected = ImmutableMap.of("a", -3L, "b", -2L);
    assertEquals(expected, actual);
  }
}