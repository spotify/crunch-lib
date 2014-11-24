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
import com.spotify.crunch.test.NestAvroRecord;
import com.spotify.crunch.test.TestAvroRecord;
import org.apache.avro.util.Utf8;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import static org.junit.Assert.*;

public class AvroCollectionsTest {
  @Test
  public void testKeyByAvroFieldSimple() throws PlanTimeException {
    TestAvroRecord rec = TestAvroRecord.newBuilder().setFieldA(new Utf8("hello")).setFieldB("world").setFieldC(10L).build();
    PCollection<TestAvroRecord> collection =
            MemPipeline.typedCollectionOf(Avros.specifics(TestAvroRecord.class), rec);

    PTable<String, TestAvroRecord> table = AvroCollections.keyByAvroField(collection, "fieldA");
    assertEquals(ImmutableMap.of("hello", rec), table.materializeToMap());
  }

  @Test
   public void testKeyByAvroFieldNull() throws PlanTimeException {
    TestAvroRecord rec = TestAvroRecord.newBuilder().setFieldA(new Utf8("hello")).setFieldB(null).setFieldC(10L).build();
    PCollection<TestAvroRecord> collection =
            MemPipeline.typedCollectionOf(Avros.specifics(TestAvroRecord.class), rec);

    PTable<String, TestAvroRecord> table = AvroCollections.keyByAvroField(collection, "fieldB");
    assertEquals(ImmutableMap.of("null", rec), table.materializeToMap());
  }

  @Test
  public void testKeyByAvroFieldNested() throws PlanTimeException {
    TestAvroRecord rec = TestAvroRecord.newBuilder().setFieldA(new Utf8("hello")).setFieldB("world").setFieldC(10L).build();
    NestAvroRecord nest = NestAvroRecord.newBuilder().setFieldX("eggs").setFieldY(rec).build();
    PCollection<NestAvroRecord> collection =
            MemPipeline.typedCollectionOf(Avros.specifics(NestAvroRecord.class), nest);

    PTable<String, NestAvroRecord> table = AvroCollections.keyByAvroField(collection, "fieldY.fieldA");
    assertEquals(ImmutableMap.of("hello", nest), table.materializeToMap());
  }
}