# Memory Analysis Cube

The Memory Analysis Cube (MAC) is an ActivePivot project that aims to provide
the necessary tools to explore and analyze the data consumption from any other
ActivePivot application, starting from ActivePivot 5.8 onwards.

## Main Features

* Off Heap memory usage monitoring 
* Store/Field-related memory footprint
* Cube-related memory footprint
* Overview of structure-related memory footprint
* Loading and comparing several ActivePivot applications' exported memory
  dumpFiles

## Prepare your Application for Analysis

ActivePivot 5.8+ applications come with a number of ways to export their memory
usage in the form of memory report files, that can then be imported and analyzed
using MAC.

The process is explained below:

* [exporting your application](setting_up/exporting.md)
* [importing data in MAC](setting_up/importing.md)
* [structure of memory statistics](setting_up/statistics.md)

## MAC Data Model

To fully make use of MAC's capabilities for analyzing your project, it is
important to get familiar with its [data model](data_model.md).

## Bookmarks

MAC comes with a number of predefined bookmarks that each offer some insights to
various aspects of the monitored application.

* [Overview](bookmarks/overview.md)
* [Per-field Analysis](bookmarks/fields.md)
* [Vector Block Analysis](bookmarks/vectors.md)
* [Dictionary and Index Analysis](bookmarks/dictionaries_indexes.md)
* [Aggregate Provider Analysis](bookmarks/aggregate_providers.md)

## Widgets

A few predefined widgets are available to help the creation of custom
dashboards, with more details on the [widgets page](widgets.md).

## Known limitations

* This tool does not support projects with multiple datastores. It is possible
  to import data from multiple datastores, but there are no ways to assign a
  store to a given datastore nor prevent conflicts between store names.
* On-heap memory analysis is currently not fully implemented. The application
  only sums the memory used by Chunks. Please directly refer to the JMX beans of
  your application or other analysis tools for a deeper investigation.