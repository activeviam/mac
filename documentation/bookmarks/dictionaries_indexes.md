# Dictionaries and indexes bookmark

This bookmark provides specialized views for analyzing the direct memory
consumption of the various dictionaries and indices of the application.

## Top Dictionaries

This view presents the most memory-consuming dictionaries, categorized per field
name, across all owners.

Some dictionaries may be shared across multiple fields of different stores.

Its level of detail is too coarse for in-depth analysis, and is only provided to
potentially draw attention to where the largest dictionaries may be.

> In case of a field name conflict between two stores, their dictionaries will
> both be taken into account even if there is not reference between these two
> fields.

## Top Indices

This view presents the most memory-consuming indices, categorized per field
name, across all owners.

Its level of detail is too coarse for in-depth analysis, and is only provided to
potentially draw attention to where the largest indices may be.

> In case of a field name conflict between two stores, their indices will
> both be taken into account even if there is not reference between these two
> fields.
> 
> An index that indexes several fields will contribute its memory consumption to
> all fields.

## Dictionary and Index per Store

This page presents more detailed views for analyzing the memory consumption of
indices and dictionaries.

The indices and dictionaries are detailed for each field and each store.

For indices, the hierarchy `Indexed Fields` is used, meaning that indexes are
separated by field groups: an index on fields A and B will only contribute to
the indexed fields *A and B*, and neither to only field *A* or only field *B*.
