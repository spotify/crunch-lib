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

    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualS = Percentiles.distributed(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualM = Percentiles.inMemory(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> expected = ImmutableMap.of(
            "a", Pair.of((Collection<Pair<Double, Integer>>) Lists.newArrayList(Pair.of(0.0, 1), Pair.of(0.5, 3), Pair.of(1.0, 5)), 5L)
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

  @Test
  public void testPercentilesBetween() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 5,
            "a", 2, // We expect the 0.5 to correspond to this element, according to the "nearest rank" %ile definition.
            "a", 4,
            "a", 1);
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualS = Percentiles.distributed(testTable, 0.5).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualM = Percentiles.inMemory(testTable, 0.5).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> expected = ImmutableMap.of(
            "a", Pair.of((Collection<Pair<Double, Integer>>)Lists.newArrayList(Pair.of(0.5, 2)), 4L)
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

  @Test
  public void testPercentilesNines() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 10,
            "a", 20,
            "a", 30,
            "a", 40,
            "a", 50,
            "a", 60,
            "a", 70,
            "a", 80,
            "a", 90,
            "a", 100);
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualS = Percentiles.distributed(testTable, 0.9, 0.99).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualM = Percentiles.inMemory(testTable, 0.9, 0.99).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> expected = ImmutableMap.of(
            "a", Pair.of((Collection<Pair<Double, Integer>>)Lists.newArrayList(Pair.of(0.9, 90), Pair.of(0.99, 100)), 10L)
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }

  @Test
  public void testPercentilesLessThanOrEqual() {
    PTable<String, Integer> testTable = MemPipeline.typedTableOf(
            tableOf(strings(), ints()),
            "a", 10,
            "a", 20,
            "a", 30,
            "a", 40,
            "a", 50,
            "a", 60,
            "a", 70,
            "a", 80,
            "a", 90,
            "a", 100);
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualS = Percentiles.distributed(testTable, 0.5).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> actualM = Percentiles.inMemory(testTable, 0.5).materializeToMap();
    Map<String, Pair<Collection<Pair<Double, Integer>>, Long>> expected = ImmutableMap.of(
            "a", Pair.of((Collection<Pair<Double, Integer>>)Lists.newArrayList(Pair.of(0.5, 50)), 10L)
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }


}
