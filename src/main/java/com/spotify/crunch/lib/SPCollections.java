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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.types.PType;

/**
 * Extra high-level operations for working with PCollections
 */
public class SPCollections {

  /**
   * Key a PCollection of Avro records by a String field name. This is less safe than writing a custom MapFn, but it
   * could significantly reduce code volume in cases that need a lot of disparate collections to be joined or processed
   * according to key values.
   * @param collection PCollection of Avro records to process
   * @param fieldName The Avro schema field name of the field to key on
   * @param keyType PType for the key which should be extracted
   * @param <K> key type
   * @param <T> record type
   * @return supplied collection keyed by the field named fieldName
   * @throws PlanTimeException if you request a non-existent field or your record is not constructable
   */
  public static <K, T extends SpecificRecord> PTable<K, T> keyByAvroField(PCollection<T> collection, String fieldName, PType<K> keyType) throws PlanTimeException {
    final Class<T> recordType = collection.getPType().getTypeClass();
    T record;
    try {
      record = recordType.getConstructor().newInstance();
    } catch (Exception e) {
      throw new PlanTimeException("Could not create an instance of the record to determine it's schema", e);
    }
    Schema.Field f = record.getSchema().getField(fieldName);

    if (f == null) {
      throw new PlanTimeException("Field " + fieldName + " does not exist in schema");
    }

    final int fieldIndex = f.pos();

    // We check for string so we can to a toString to prevent Utf8/CharSequence problems
    if (f.schema().getType() == Schema.Type.STRING) {
      return collection.by(new MapFn<T, K>() {
        @Override
        @SuppressWarnings("unchecked")
        public K map(T input) {
          return (K) input.get(fieldIndex).toString();
        }
      }, keyType);
    } else {
      return collection.by(new MapFn<T, K>() {
        @Override
        @SuppressWarnings("unchecked")
        public K map(T input) {
          return (K) input.get(fieldIndex);
        }
      }, keyType);
    }
  }
}
