package com.spotify.crunch.lib;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import com.spotify.crunch.lib.Percentiles.Result;

import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.junit.Assert.assertEquals;

public class PercentilesTest {

  private static <T> Percentiles.Result<T> result(long count, Pair<Double, T>... percentiles) {
    return new Percentiles.Result<T>(count, Lists.newArrayList(percentiles));
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
    Map<String, Result<Integer>> actualS = Percentiles.distributed(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Result<Integer>> actualM = Percentiles.inMemory(testTable, 0, 0.5, 1.0).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(5, Pair.of(0.0, 1), Pair.of(0.5, 3), Pair.of(1.0, 5))
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
    Map<String, Result<Integer>> actualS = Percentiles.distributed(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> actualM = Percentiles.inMemory(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(4, Pair.of(0.5, 2))
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
    Map<String, Result<Integer>> actualS = Percentiles.distributed(testTable, 0.9, 0.99).materializeToMap();
    Map<String, Result<Integer>> actualM = Percentiles.inMemory(testTable, 0.9, 0.99).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(10, Pair.of(0.9, 90), Pair.of(0.99, 100))
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
    Map<String, Result<Integer>> actualS = Percentiles.distributed(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> actualM = Percentiles.inMemory(testTable, 0.5).materializeToMap();
    Map<String, Result<Integer>> expected = ImmutableMap.of(
            "a", result(10, Pair.of(0.5, 50))
    );

    assertEquals(expected, actualS);
    assertEquals(expected, actualM);
  }


}
