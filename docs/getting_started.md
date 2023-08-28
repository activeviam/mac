# Memory Analysis Cube

The Memory Analysis Cube (MAC) is an Atoti project that aims to provide
the necessary tools to explore and analyze the data consumption from any other
Atoti application, starting from Atoti Server 5.8 onward.

The project is built as a standalone Spring Boot jar file with an embedded
Atoti UI application available on `localhost:9092` by default.

## Main Features

* Off Heap memory usage monitoring 
* Store/Field-related memory footprint
* Cube-related memory footprint
* Overview of structure-related memory footprint
* Loading and comparing several Atoti applications' exported memory
  dump files

## Prepare your Application for Analysis

Applications built using Atoti Server 5.8+ come with a number of ways to export their memory
usage in the form of memory report files, that can then be imported and analyzed
using MAC.

The process is explained below:

* [Exporting your application](setting_up/exporting.md)
* [Structure of memory statistics and decompressing generated files](setting_up/statistics.md)
* [Importing data in MAC](setting_up/importing.md)

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

## Known limitations

* This tool does not support projects with multiple datastores. It is possible
  to import data from multiple datastores, but there are no ways to assign a
  store to a given datastore nor prevent conflicts between store names.
* On-heap memory analysis is currently not fully implemented. The application
  only sums the memory used by Chunks. Please directly refer to the JMX beans of
  your application or other analysis tools for a deeper investigation.

## Planned Improvements

- [ ] Display and use the consumed On-heap memory reported by the Memory
  Analysis.
