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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.*;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import static com.spotify.crunch.lib.MapFns.*;

import java.util.List;

public class AvroCollections {
  /**
   * Key a PCollection of Avro records by a String field name. This is less safe than writing a custom MapFn, but it
   * could significantly reduce code volume in cases that need a lot of disparate collections to be joined or processed
   * according to key values.
   * @param collection PCollection of Avro records to process
   * @param fieldPath The Avro schema field name of the field to key on. Use . separated names for nested records
   * @param fieldType PType of the field you wish to extract from the Avro record.
   * @param <T> record type
   * @return supplied collection keyed by the field named fieldName
   */
  public static <T extends SpecificRecord, F> PTable<F, T> keyByAvroField(PCollection<T> collection, String fieldPath, PType<F> fieldType) {
    Class<T> recordType = collection.getPType().getTypeClass();
    return collection.by(new AvroExtractMapFn<T, F>(recordType, fieldPath), fieldType);
  }

  /**
   * Extract a single field value from a PCollection of Avro records
   * @param collection PCollection of Avro records to process
   * @param pathA The Avro schema field name of the field to key on. Use . separated names for nested records
   * @param pType PType for the resulting PCollection
   * @param <R> Avro record type
   * @param <A> Field type
   * @return A PCollection containing the values extracted from the record
   */
  public static <R extends SpecificRecord, A> PCollection<A> extract(PCollection<R> collection, String pathA, PType<A> pType) {
    Class<R> recordType = collection.getPType().getTypeClass();
    return collection.parallelDo(new AvroExtractMapFn<R, A>(recordType, pathA), pType);
  }

  /**
   * Extract a Pair of field values from a PCollection of Avro records into a PTable
   * @param collection PCollection of Avro records to process
   * @param pathA The Avro schema field name of the first field. Use . separated names for nested records
   * @param pathB The Avro schema field name of the second field. Use . separated names for nested records
   * @param pType PTableType for the resulting PTable
   * @param <R> Avro record type
   * @param <A> Key field type
   * @param <B> Value field type
   * @return A PTable containing the values extracted from the record
   */
  public static <R extends SpecificRecord, A, B> PTable<A, B> extract(PCollection<R> collection, String pathA, String pathB, PTableType<A, B> pType) {
    Class<R> recordType = collection.getPType().getTypeClass();
    return collection.parallelDo(
            pairFn(new AvroExtractMapFn<R, A>(recordType, pathA), new AvroExtractMapFn<R, B>(recordType, pathB)), pType);
  }

  /**
   * Extract a Tuple3 of field values from a PCollection of Avro records into a PTable
   * @param collection PCollection of Avro records to process
   * @param pathA The Avro schema field name of the first field. Use . separated names for nested records
   * @param pathB The Avro schema field name of the second field. Use . separated names for nested records
   * @param pathC The Avro schema field name of the second field. Use . separated names for nested records
   * @param pType PType for the resulting PCollection
   * @param <R> Avro record type
   * @param <A> First field type
   * @param <B> Second field type
   * @param <B> Third field type
   * @return A PCollection of Tuple3s containing the values extracted from the record
   */
  public static <R extends SpecificRecord, A, B, C> PCollection<Tuple3<A, B, C>> extract(PCollection<R> collection, String pathA, String pathB, String pathC, PType<Tuple3<A, B, C>> pType) {
    Class<R> recordType = collection.getPType().getTypeClass();
    return collection.parallelDo(
            tuple3Fn(
                    new AvroExtractMapFn<R, A>(recordType, pathA),
                    new AvroExtractMapFn<R, B>(recordType, pathB),
                    new AvroExtractMapFn<R, C>(recordType, pathC)),
            pType);
  }

  /**
   * MapFn to extract a field from an Avro record
   * @param <T> Avro record class
   * @param <F> Field type
   */
  private static class AvroExtractMapFn<T extends SpecificRecord, F> extends MapFn<T, F> {
    private final List<Integer> indices;
    private boolean targetIsString = false;

    /**
     * Create a new AvroExtractMapFn
     * @param recordType Java class for the Avro record that we want to extract the field from
     * @param path field name, or for nested records a .-separated "path" to the field that you want to extract
     */
    public AvroExtractMapFn(Class<T> recordType, String path) {
      Schema recordSchema = ReflectionUtils.newInstance(recordType, new Configuration()).getSchema();
      indices = findIndices(recordSchema, Splitter.on(".").split(path));
    }

    private List<Integer> findIndices(Schema schema, Iterable<String> pieces) {
      List<Integer> indices = Lists.newArrayList();
      for (String piece: pieces) {
        schema = descendUnion(schema);
        int index = schema.getField(piece).pos();
        schema = schema.getField(piece).schema();
        indices.add(index);
      }
      schema = descendUnion(schema);
      if (schema.getType() == Schema.Type.STRING) {
        targetIsString = true;
      }
      return indices;
    }

    private static Schema descendUnion(Schema schema) {
      if (schema.getType() == Schema.Type.UNION) {
        return firstNotNull(schema.getTypes());
      } else {
        return schema;
      }
    }

    private static Schema firstNotNull(Iterable<Schema> schemas) {
      for (Schema s: schemas) {
        if (s.getType() != Schema.Type.NULL) {
          return s;
        }
      }
      throw new PlanTimeException("Union type had no non-null types");
    }

    @Override
    public F map(T record) {
      Object fieldValue = record;
      for (int i : indices) {
        fieldValue = ((IndexedRecord)fieldValue).get(i);
      }

      // This monstrosity is required so that we stringify any rogue CharSequences being extracted
      // If you've got a better idea for a cleaner implementation, please share
      if (targetIsString) {
        if (fieldValue == null) {
          return null;
        } else {
          return (F) fieldValue.toString();
        }
      } else {
        return (F) fieldValue;
      }
    }
  }
}
