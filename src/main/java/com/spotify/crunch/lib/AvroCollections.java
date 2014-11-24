package com.spotify.crunch.lib;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.List;

public class AvroCollections {
  /**
   * Key a PCollection of Avro records by a String field name. This is less safe than writing a custom MapFn, but it
   * could significantly reduce code volume in cases that need a lot of disparate collections to be joined or processed
   * according to key values.
   * @param collection PCollection of Avro records to process
   * @param fieldPath The Avro schema field name of the field to key on
   * @param <T> record type
   * @return supplied collection keyed by the field named fieldName
   * @throws PlanTimeException if you request a non-existent field or your record is not constructable
   */
  public static <T extends SpecificRecord> PTable<String, T> keyByAvroField(PCollection<T> collection, String fieldPath) throws PlanTimeException {
    Class<T> recordType = collection.getPType().getTypeClass();
    final AvroPath<T, CharSequence> path = new AvroPath<T, CharSequence>(recordType, fieldPath);

    return collection.by(new MapFn<T, String>() {
      @Override
      public String map(T input) {
        return Optional.fromNullable(path.get(input)).or("null").toString();
      }
    }, collection.getTypeFamily().strings());
  }

  public static class AvroPath<T extends SpecificRecord, F> {
    private final Schema recordSchema;
    private final List<Integer> indices;

    public AvroPath(Class<T> recordType, String path) {
      recordSchema = ReflectionUtils.newInstance(recordType, new Configuration()).getSchema();
      indices = findIndices(recordSchema, Splitter.on(".").split(path));
    }

    private List<Integer> findIndices(Schema schema, Iterable<String> pieces) {
      List<Integer> indices = Lists.newArrayList();
      for (String piece: pieces) {
        if (schema.getType() == Schema.Type.UNION) {
          schema = firstNotNull(schema.getTypes());
        }
        int index = schema.getField(piece).pos();
        schema = schema.getField(piece).schema();
        indices.add(index);
      }
      return indices;
    }

    private Schema firstNotNull(Iterable<Schema> schemas) {
      for (Schema s: schemas) {
        if (s.getType() != Schema.Type.NULL) {
          return s;
        }
      }
      throw new PlanTimeException("Union type had no non-null types");
    }

    public F get(T record) {
      Object fieldValue = record;
      for (int i : indices) {
        fieldValue = ((IndexedRecord)fieldValue).get(i);
      }
      return (F)fieldValue;
    }
  }
}
