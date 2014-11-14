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
import com.google.common.collect.Lists;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;
import static org.apache.crunch.types.avro.Avros.*;

public class AveragesTest {
  @Test
  public void testMeanValue() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 2,
            "a", 10,
            "b", 3,
            "c", 3,
            "c", 4,
            "c", 5);
    Map<String, Double> actual = Averages.meanValue(testTable).materializeToMap();
    Map<String, Double> expected = ImmutableMap.of(
            "a", 6.0,
            "b", 3.0,
            "c", 4.0
    );

    assertEquals(expected, actual);
  }

  @Test
  public void testPercentilesExact() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 5,
            "a", 2,
            "a", 3,
            "a", 4,
            "a", 1);
    Map<String, Collection<Pair<Double, Integer>>> actualS = Averages.scalablePercentiles(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> actualM = Averages.inMemoryPercentiles(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> expected = ImmutableMap.of(
            "a", (Collection<Pair<Double, Integer>>)Lists.newArrayList(Pair.of(0.0, 1), Pair.of(0.5, 3), Pair.of(1.0, 5))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

  @Test
  public void testPercentilesBetween() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 5,
            "a", 2,
            "a", 4, // We expect the 0.5 to correspond to this element, according to the "nearest rank" %ile definition.
            "a", 1);
    Map<String, Collection<Pair<Double, Integer>>> actualS = Averages.scalablePercentiles(testTable, 0.5).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> actualM = Averages.inMemoryPercentiles(testTable, 0.5).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> expected = ImmutableMap.of(
            "a", (Collection<Pair<Double, Integer>>)Lists.newArrayList(Pair.of(0.5, 4))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

}