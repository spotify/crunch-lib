package com.spotify.crunch.lib;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.junit.Assert.assertEquals;

public class PercentilesTest {

  @Test
  public void testPercentilesExact() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 5,
            "a", 2,
            "a", 3,
            "a", 4,
            "a", 1);
    Map<String, Collection<Pair<Double, Integer>>> actualS = Percentiles.distributed(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> actualM = Percentiles.inMemory(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> expected = ImmutableMap.of(
            "a", (Collection<Pair<Double, Integer>>) Lists.newArrayList(Pair.of(0.0, 1), Pair.of(0.5, 3), Pair.of(1.0, 5))
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
    Map<String, Collection<Pair<Double, Integer>>> actualS = Percentiles.distributed(testTable, 0.5).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> actualM = Percentiles.inMemory(testTable, 0.5).materializeToMap();
    Map<String, Collection<Pair<Double, Integer>>> expected = ImmutableMap.of(
            "a", (Collection<Pair<Double, Integer>>)Lists.newArrayList(Pair.of(0.5, 4))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }
}
