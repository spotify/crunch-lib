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