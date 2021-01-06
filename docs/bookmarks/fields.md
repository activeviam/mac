# Fields bookmark

This bookmark provides specialized views for analyzing the direct memory
consumption of the various stores of the application, with a focus on the fields
the chunks can be attributed to.

## Top-Consuming Fields

This view lists the most memory-consuming fields across all stores.

This page only aims to be an entry-point for further analysis, after drawing
attention to abnormally high memory consumptions.

In the case of a name conflict between fields of different stores, the memory
consumption is displayed per owner under each field.

Chunks may be shared across multiple fields (in the case of indexes over
multiple fields for example). As such, a very large index will impact the memory
consumption of all the fields it is attributed to.

## Field Analysis

This view displays the per-component memory consumption of each field per store.

The various possible components that can be attributed to a field are:
* `INDEX`: an index indexes the corresponding field. It is usually the highest
  direct-memory-consuming component for store fields.

  > All indices indexing this field are taken into account (e.g. an index on
  > fields `A, B` and another index on field `B` both contribute to field B's
  > memory consumption)
  >
  > An index attributes its chunks to all fields it indexes.

* `DICTIONARY`
* `RECORDS`: the chunks the stores use to store their fields' values fall into
  this category.
* `VECTOR_BLOCK`: specific to vector fields, the memory consumption of the
  corresponding vector blocks (**includes swapped memory**)

It is important to note that not all chunks of a store appear in this page.

Some store chunks are not be attributed to any field in particular:
* the chunks of the versioning column of a store (`RECORDS` component)
* partition mapping chunks used by references between stores (`REFERENCE`
  component)
