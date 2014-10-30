package com.spotify.crunch.lib;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.apache.crunch.types.avro.Avros.*;
import static org.junit.Assert.*;

public class TopListsTest {

  private <T> Collection<T> collectionOf(T... elements) {
    return Lists.newArrayList(elements);
  }

  @Test
  public void testTopNYbyX() {
    PTable<String, String> data = MemPipeline.typedTableOf(tableOf(strings(), strings()),
            "a","x",
            "a","x",
            "a","x",
            "a","y",
            "a","y",
            "a","z",
            "b","x",
            "b","x",
            "b","z");
    Map<String, Collection<Pair<Long, String>>> actual = TopLists.topNYbyX(data, 2).materializeToMap();
    Map<String, Collection<Pair<Long, String>>> expected = ImmutableMap.of(
            "a", collectionOf(Pair.of(3L, "x"), Pair.of(2L, "y")),
            "b", collectionOf(Pair.of(2L, "x"), Pair.of(1L, "z")));

    assertEquals(expected, actual);
  }

  @Test
  public void testGlobalToplist() {
    PCollection<String> data = MemPipeline.typedCollectionOf(strings(), "a", "a", "a", "b", "b", "c", "c", "c", "c");
    Map<String, Long> actual = TopLists.globalToplist(data).materializeToMap();
    Map<String, Long> expected = ImmutableMap.of("c", 4L, "a", 3L, "b", 2L);
    assertEquals(expected, actual);
  }
}