# crunch-lib

This repository contains useful reusable high-level components for common use-cases in processing data with
[Apache Crunch](http://crunch.apache.org)

If you want to try it, it's in the central Maven repo so you can use this snippet (or equivalent for gradle/sbt/...)

```xml
<dependency>
   <groupId>com.spotify.crunch</groupId>
   <artifactId>crunch-lib</artifactId>
   <version>0.0.3</version>
</dependency>
```

## AvroCollections
* `extract` pulls out individual fields from a `PCollection` of Avro records by their field names without the need for
   trivial `MapFn`s
* `keyByAvroField` keys a `PCollection` of Avro records by a specific field using it's name without the need for trivial
   `MapFn`s

## SPTables
* `swapKeyValue` swaps the key and the value parts of a `PTable`
* `negateCounts` negates the value part of a long-valued table to facilitate easy sort-descending

## TopLists
* `topNYbyX` Creates a top-list of elements in the provided `PTable`, categorised by the key of the input table and using
  the count of the value part of the input table.
* `globalTopList` Create a list of unique items in the input collection with their count, sorted descending by their
  frequency.

## Averages
* `meanValue` Calculates the mean value for each key in the provided numerically-valued `PTable`.

## Percentiles
* `distributed` / `inMemory` Calculates a set of percentiles for each key in the provided numerically-valued `PTable`.

## DoFns
* `detach` wrap a DoFn operating as a reducer such that each value given by the Iterable is already detached (preventing
  object reuse problems)