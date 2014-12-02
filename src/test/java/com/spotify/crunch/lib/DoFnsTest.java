package com.spotify.crunch.lib;

import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.spotify.crunch.test.TestAvroRecord;
import org.apache.avro.util.Utf8;
import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

public class DoFnsTest {

  private static class AvroIterable implements Iterable<TestAvroRecord> {
    @Override
    public Iterator<TestAvroRecord> iterator() {
      final TestAvroRecord rec = new TestAvroRecord(new Utf8("something"), new Utf8(""), 1L);
      return new AbstractIterator<TestAvroRecord>() {
        private int n = 0;
        @Override
        protected TestAvroRecord computeNext() {
          n++;
          if (n > 3) return endOfData();
          rec.setFieldB(new Utf8(Strings.repeat("*", n)));
          return rec;
        }
      };
    }
  }

  private static class CollectingMapFn extends MapFn<Pair<String, Iterable<TestAvroRecord>>, Collection<TestAvroRecord>> {

    @Override
    public Collection<TestAvroRecord> map(Pair<String, Iterable<TestAvroRecord>> input) {
      return Lists.newArrayList(input.second());
    }
  }

  @Test
  public void testDetach() {
    Collection<TestAvroRecord> expected = Lists.newArrayList(
            new TestAvroRecord(new Utf8("something"), new Utf8("*"), 1L),
            new TestAvroRecord(new Utf8("something"), new Utf8("**"), 1L),
            new TestAvroRecord(new Utf8("something"), new Utf8("***"), 1L)
    );
    DoFn<Pair<String, Iterable<TestAvroRecord>>, Collection<TestAvroRecord>> doFn =
            DoFns.detach(new CollectingMapFn(), Avros.specifics(TestAvroRecord.class));
    Pair<String, Iterable<TestAvroRecord>> input = Pair.of("key", (Iterable<TestAvroRecord>) new AvroIterable());
    InMemoryEmitter<Collection<TestAvroRecord>> emitter = new InMemoryEmitter<Collection<TestAvroRecord>>();

    doFn.configure(new Configuration());
    doFn.initialize();
    doFn.process(input, emitter);
    doFn.cleanup(emitter);

    assertEquals(expected, emitter.getOutput().get(0));
  }

}