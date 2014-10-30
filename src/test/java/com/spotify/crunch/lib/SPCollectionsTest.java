package com.spotify.crunch.lib;

import com.google.common.collect.ImmutableMap;
import com.spotify.crunch.test.TestAvroRecord;
import org.apache.avro.util.Utf8;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import static org.junit.Assert.*;

public class SPCollectionsTest {
  @Test
  public void testKeyByAvroField() throws PlanTimeException {
    TestAvroRecord rec = TestAvroRecord.newBuilder().setFieldA(new Utf8("hello")).setFieldB("world").setFieldC(10L).build();
    PCollection<TestAvroRecord> collection =
            MemPipeline.typedCollectionOf(Avros.specifics(TestAvroRecord.class), rec);

    PTable<String, TestAvroRecord> table = SPCollections.keyByAvroField(collection, "fieldA", Avros.strings());
    assertEquals(ImmutableMap.of("hello", rec), table.materializeToMap());
  }
}