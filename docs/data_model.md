MAC is composed of one cube. Its base store is based on Chunks, which atomically
contain all the off-heap data used by ActivePivot.

Chunks are attributed to various higher level structures, that are represented
by MAC's hierarchies and dimensions and can be queried upon.

## Measures

### Application memory footprint
These measures come from statistics that were exported using the memory MBean of
the application.

Since they are application-wide, they have the same value for all locations.

* `UsedHeapMemory`: the amount of heap memory used by the application
* `CommittedHeapMemory`: the total size of the JVM heap
* `UsedDirectMemory`: the amount of off-heap memory used by the application
* `MaxDirectMemory`: the amount of off-heap memory reserved by the application

### Chunk memory footprint:
* `DirectMemory.SUM`: the off-heap size of the chunks
* `DirectMemory.Ratio`: the total ratio of off-heap memory consumed by the chunks
  relative to the total used chunk memory
* `UsedMemory.Ratio`: the total ratio of off-heap memory consumed by the chunks
  relative to the total used application committed memory
* `MaxMemory.Ratio`: the total ratio of off-heap memory consumed by the chunks
  relative to the total application committed memory
* `HeapMemory.SUM`: an estimate of the on-heap size of the chunks
  > **Warning**: do **NOT** assume total on-heap memory usage by ActivePivot
  > based on this measure.
  >
  > These estimates rely on manual calculations from within the application that
  > can not precisely capture on-heap memory consumption, and much less its
  > attribution to higher level structures.
  >
  > Please use another tool dedicated to analysis of on-heap memory for Java
  > applications instead.
* `HeapMemory.Ratio`: the total ratio of on-heap memory consumed by the chunks,
  relies on **`HeapMemory.SUM`**
* `CommittedChunkMemory.SUM`: the amount of memory in bytes used to store committed rows in
      chunks

### Chunk characteristics:
* `Chunks.COUNT`: the number of contributing chunks
  > `contributors.COUNT` does not correspond to `Chunks.COUNT`, so prefer using `Chunks.COUNT`.
* `ChunkSize.SUM`: a sum aggregation of the chunk sizes
* `CommittedRows.COUNT`: the number of committed (i.e. used) rows inside the chunks
* `CommittedRows.Ratio`: the ratio of committed (i.e. used) rows inside the
      chunks
* `NonWrittenRows.COUNT`: the number of unused rows within the chunks
* `NonWrittenRows.Ratio`: the ratio of unused rows within the chunks
* `DeletedRows.COUNT`: the number of freed rows within the chunks
* `DeletedRows.Ratio`: the ratio of freed rows within the chunks

### Miscellaneous
* `DictionarySize.SUM`: the number of entries in the corresponding dictionary, when
  relevant
* `VectorBlock.Length`: the length of the vector block, when relevant
* `VectorBlock.RefCount`: the number of references to the vector block, when
  relevant

## Owners

Chunks are always held by a higher-level structure, which is called the *owner*
in MAC.

The different owners of an ActivePivot application are stores of the datastore
and the different cubes of the application.

### Owner

The members of this single-level hierarchy are the different owners of the
exported application.

For example, if the exported application has two cubes `Query` and `Data` and
two stores `Trade` and `Product`, the `Owner` hierarchy will have the following
members:
* `Cube Query`
* `Cube Data`
* `Store Trade`
* `Store Product`

A chunk may be shared by multiple owners. For example, two stores with one
referencing the other can share the same dictionary for the fields used by the
reference. MAC will attribute the dictionary's chunks to both stores.

### Owner Type

This single-level bucketing hierarchy has two members, `Store` and `Cube`, and
can be used to distinguish between both owner types.

## Components

Chunks are used by a variety of higher-level structures, such as indexes,
references, dictionaries, etc. This hierarchy associates each chunk with one or
more of these *components*:

* DICTIONARY
* RECORDS
* INDEX
* REFERENCE
* VECTOR BLOCK
* LEVEL
* POINT_MAPPING
* POINT_INDEX
* BITMAP_MATCHER
* AGGREGATE_STORE *(for bitmap and leaf providers)*

A chunk may be attributed to several components. If a dictionary is used by an
index, its chunks will be attributed to both the *DICTIONARY* and *INDEX*
components for example.

## Fields

The Field single-level hierarchy contains:
* For store chunks: the field(s) the chunk can be attributed to
* For cube chunks: the measure(s) the chunk can be attributed to

A chunk may be attributed to multiple fields in the case of chunks of an index
over multiple fields of a store for example.

## Chunks

This dimension contains various hierarchies related to the monitored chunks.

For detailed analyses, its hierarchies give access to monitoring data at the
finest granularity, the chunks being the grain of MAC's internal base store.

### ChunkId

The internal ID that identifies the chunk.

### Class

The java class used by the chunk.

This can be useful to check whether chunk compression is used by looking
for classes using some sort of compression on their data.

### DicoID

If relevant, the ID of the parent dictionary the chunks are attributed to.

### IndexID

If relevant, the ID of the parent index the chunks are attributed to.

### ReferenceID

If relevant, the ID of the parent reference the chunks are attributed to.

### ParentID

An ID internal to MAC that identifies the parent structure owning the chunk (can
be a dictionary, an index or a reference).

### Type

The type of the structure owning the chunk. Can be one of:
* DICTIONARY
* RECORDS
* INDEX
* VECTOR_BLOCK
* REFERENCE
* AGGREGATE_STORE
* POINT_INDEX
* POINT_MAPPING

This hierarchy categorizes chunks in a way similar to the *Component* hierarchy,
but its members designate lower-level structures than the *Component* hierarchy.
For example, an *INDEX* **component** may have chunks of **type** *INDEX* and
*DICTIONARY*.

## Indices

This dimension contains information about indices. It is only relevant and
defined for chunks that can be attributed to an index.

### Index Type

This single-level hierarchy contains three members for each possible index type:
* Key: index on a store key field
* Primary: primary index of a store
* Secondary: secondary index of a store

### Indexed Fields

The members of this single-level hierarchy are all the different groups of store
fields the various indices are defined upon.

While related, this hierarchy should not be confused with the `[Fields].[Field]`
hierarchy, and allows for characterizing all the fields of an index.

For example, given the two following indices:
* index 1 on fields `Field A` and `Field B`
* index 2 on fields `Field A`, `Field B` and `Field C`

Index 1 will have its chunks attributed to *fields* `[Field A]` and `[Field B]`,
and to *indexed fields* `[Field A, Field B]`.

Index 2 will have its chunks attributed to *fields* `[Field A]`, `[Field B]` and
`[Field C]`, and to *indexed fields* `[Field A, Field B, Field C]`.

A query on the chunks attributed to *fields* `[Field A]` and `[Field B]` will
yield the chunks of both indices, while a query on *indexed fields* `[Field A,
Field B]` will only yield the chunks of index 1.

## Aggregate Provider

The hierarchies of these dimensions are relevant for chunks that are attributed
to a cube owner.

### Provider Category

Distinguishes between full and partial providers. Possible members are:
* Full
* Partial
* Unique *(the cube uses a single provider)*

### Provider Type

Distinguishes between the underlying aggregate provider type. Possible members
are:
* JIT
* Leaf
* Bitmap

### Manager

The name of the manager the chunks are attributed to.

### Provider ID

The ID of the aggregate provider the chunks are attributed to.

## Partitions

The store or aggregate provider partition the chunk belongs to, depending on
whether the chunk belongs to a cube or a store.

## Versions

### Branch

A single-level slicing hierarchy whose members are the branches of the exported
chunks. Its default member is the `master` branch.

### Epoch Id

A single-level hierarchy whose members are the epoch ids of the exported chunks.

> Important: if an epoch prior to the most recent version is exported using
> `IMemoryAnalysisService.exportVersions()`, the report will not be a "snapshot"
> of the memory footprint of the application at this epoch.
>
> The present chunks are always chunks that are still present at the time of the
> export either because they are still used by the most recent version or
> because they were not yet discarded.

### Used By Version

This single-level hierarchy describes, for each chunk and epoch id whether
the chunk is "used" by the corresponding version i.e. if it has not been
flagged as deleted in a previous version.

* `TRUE`: the chunk is still used by the expressed epoch
* `FALSE`: the chunk has been fully marked as deleted in a previous version
* `UNKNOWN`: the chunk cannot be classified - this can be the case for bitmap
  aggregate store chunks for example

## Import Info

This dimension contains metadata about the origin of the imported chunks, since
multiple statistics folder can be imported at once in the same MAC session.

### Date

The date at which the statistics have been imported.

### Import Info

The Import Info hierarchy contains the name of the folder that contained the
statistics. If the statistics had no parent folder, the name `"autoload-" +
LocalTime.now().toString()` will be attributed for the Import Info of the
statistic.

