This bookmark provides several general views summarizing where the application
consumes the most direct memory.

It aims to serve as an entry point for analysis.

Its view gives insight on the direct memory consumption of the application on
two axes: owners and components (as defined in the [data model
page](../data_model.md)).

## Charts

This page presents pie charts of the direct memory consumption for:

* the different owners (on the left)
* the different components, for the most memory-consuming owners (on the right)

## Owners

This page lists the direct memory consumption of the different owners.

> The total memory consumption ***is not*** equal to the sum of the memory
> consumption for every owner, since some chunks are shared across multiple
> owners.

## Components

This page lists the direct memory consumption per component for the different
owners.

> The total memory consumption of an owner ***is not*** equal to the sum of the
> memory consumption for every component, since some chunks are shared across
> multiple components.

## Detailed

This page is a more detailed version of the [components page](#components). Each
component has its memory consumption split further according the to type and
class of its chunks.
