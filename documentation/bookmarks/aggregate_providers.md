This bookmark provides specialized views for analyzing the direct memory
consumption of the pivots of the application.

## Cubes

A general overview of each cube's direct memory consumption.

## Aggregate Providers

A view focused on aggregate providers. Each cube has its memory consumption
detailed according to its aggregate providers.

JIT aggregate providers typically will not consume any direct memory, contrary
to bitmap and leaf aggregate providers, which will have chunks for their
aggregate store and point index.

## Aggregate Store

A view focused on aggregate stores for leaf and bitmap aggregate providers.

For each provider type, memory consumption of chunks is categorized according to
the measure in the aggregate store it corresponds to.

## Levels

A view that exposes memory consumption of cubes associated with the dictionaries
used to store level information.
