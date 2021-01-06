# Vector bookmark

This bookmark provides specialized views for analyzing the direct memory
consumption of the various vector fields of the application.

## Across Owners

This view lists the most memory-consuming vectors across all stores and cubes.

This page only aims to be an entry-point for further analysis, after drawing
attention to abnormally high memory consumptions.

In the case of a name conflict between fields of different stores, the memory
consumption is displayed per owner under each field.

Vector blocks may be shared across multiple fields, thus a very large vector
block can make several fields appear to be very large.

## Per Owner

This view displays the per-component memory consumption of each field per owner.
