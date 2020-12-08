MAC is composed of one cube. Its base store is based on Chunks, which atomically
contain all the off-heap data used by ActivePivot.

Chunks are attributed to various higher level structures, that are represented
by MAC's hierarchies and dimensions and can be queried upon.

## Hierarchies and Dimensions

### Owners

Chunks are always held by a higher-level structure, which is called the *owner*
in MAC.

The different owners of an ActivePivot application are stores of the datastore
and the different cubes of the application.

#### Owner

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

#### Owner Type

This single-level bucketing hierarchy has two members, `Store` and `Cube`, and
can be used to distinguish between both owner types.

### Components

Component: Stores and ActivePivots are made of several components - Indexes,
  References, PointIndex, etc. This hierarchy describes these components.

todo: common chunks for every component type
* Dictionary: chunks of dictionaries (used to dictionarize datastore fields for
  example)
* Records: chunks holding the records of a store, as well as the versioning data
* Index: 
* Reference: 
* Vector Block: contains vector data, considered as chunks by MAC because of the
  limitation of the base store although vector blocks are technically not chunks
* Level: chunks holding the members of a cube's level
* Point Mapping: 
* Point Index: 
* ~~Bitmap Matcher~~ (*todo: un-strikethrough? as of AP 5.9.3, no chunk falls
  into this component, but it should be supported*)
* Aggregate Store: for the bitmap and leaf provider, chunks holding
  pre-aggregated values for one or more measures

A chunk may be attributed to several components. If a dictionary is used by an
index, its chunks will be attributed to both the Dictionary and Index components
for example.

### Aggregate Provider

The hierarchies of these dimensions are relevant for chunks that are attributed
to a cube owner.

#### Provider Category

Distinguishes between full and partial providers. Possible members are:
* Full
* Partial

#### Provider Type

Distinguishes between the underlying aggregate provider type. Possible members
are:
* JIT
* Leaf
* Bitmap

#### Manager and Provider Id

Provides the manager name and the aggregate provider id the chunks are
attributed to.

### Versions

#### Branch

A single-level slicing hierarchy whose members are the branches of the exported
chunks. Its default member is the `master` branch.

#### Epoch Id

A single-level hierarchy whose members are the epoch ids of the exported chunks.

As chunk ids are not recycled through epochs, it is not a slicing hierarchy.

> Important: if an epoch prior to the most recent version is exported using
> `IMemoryAnalysisService.exportVersions()`, the report will not be a "snapshot"
> of the memory footprint of the application at this epoch.
>
> The present chunks are always chunks that are still present at the time of the
> export either because they are still used by the most recent version or
> because they were not yet discarded.

#### Used By Version

This single-level hierarchy describes, for each chunk and epoch id whether or
not the chunk is "used" by the corresponding version i.e. if it has not been
flagged as deleted in a previous version.

* `TRUE`: the chunk is still used by the expressed epoch
* `FALSE`: the chunk has been fully marked as deleted in a previous version
* `UNKNOWN`: the chunk cannot be classified - this can be the case for bitmap
  aggregate store chunks for example
  
  #TODO: all cases of unknown used by version
  chunks?

### Partitions

For datastore chunks, the store partition the chunk belongs to.

For cube chunks, the provider partition the chunk belongs to.

### Import Info

This dimension contains metadata about the origin of the imported chunks, since
multiple statistics folder can be imported at once in the same MAC session.

The Date hierarchy contains the date at which the statistics have been imported.

The Import Info hierarchy contains the name of the folder that contained the
statistics. If the statistics had no parent folder, the name `"autoload-" +
LocalTime.now().toString()` will be attributed for the Import Info of the
statistic.

## Fields

The Field single-level hierarchy contains:
* For store chunks: the field(s) the chunk can be attributed to
* For cube chunks: the measure(s) the chunk can be attributed to

A chunk may be attributed to multiple fields in the case of chunks of an index
over multiple fields of a store for example.

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

## Chunks

This dimension contains various hierarchies related to the monitored chunks.

For detailed analyses, its hierarchies give access to monitoring data at the
finest granularity, the chunks being the grain of MAC's internal base store.

todo: describe ChunkId, Class, DicoId, IndexId, ParentId, ReferenceId, Type

## Measures

* Off-heap memory footprint
* On-heap memory footprint
  > **Warning**: this measure only aggregates data on chunks, but not all
  > on-heap objects held by ActivePivot on heap are using chunks, do not use
  > this measure to assume total on-heap memory usage by ActivePivot)
* Amount/Ratio of free rows in chunks
* Amount/Ratio of deleted rows in chunks
* Dictionary Size
* Vector Block Size
* Vector Block Reference Count

