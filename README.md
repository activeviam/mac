Memory Analysis Cube
==============

The Memory Analysis Cube is a project that allows the exploration and analysis of the data consumption from any ActivePivot application. 
This project builds as a stand-alone SpringBoot jar file that with an embedded ActiveUI app available on `localhost:9092`.

This version is compatible with ActivePivot 5.8. To use this project with other versions pick the right branch.

Features
--------

Off Heap memory usage monitoring 
- Store/Field related memory footprint
- Overview of structure-related memory footprint

Loading and comparing several ActivePivot applications' exported memory dumpFiles 

###  Cube structure

#### Memory Cube

This is the main cube, and is the entry point for data investigation.
This cube is based on Chunk facts, which atomically contain all the off-heap data used by ActivePivot.
Some important measures are provided :
- Off Heap memory footprint
- On heap memory footprint _(Warning, this measure only aggregates data on chunks, but not all on-heap objects held by 
ActivePivot on heap are using chunks, do not use this measure to assume total on-heap memory usage by Active Pivot)_
- Amount/Ratio of free rows in chunks
- Amount/Ratio of deleted rows in chunks
- Total direct memory footprint of an application
- Total on-heap memory footprint of an application 

Multi-dimentional analysis can be done on the loaded data through the following hierarchies:
- Owner: Chunks are raw data, but they are always held by a high-level structure. This can be a Store of the Datastore 
  or an ActivePivot. Some structures are shared by multiple Stores or a Store and a Pivot. In this case, the chunk is 
  marked as **shared**.
- Component: Stores and ActivePivots are made of several components - Indexes, References, PointIndex, etc. This 
  hierarchy describes these components. As for the owner, a chunk belonging to several components is marked as **shared**.
- Class: the actual class of the chunk.

#### Additional Cubes

These cubes can provide additional information about specific structures such as References, Indexes or Dictionaries.

### Data Import

The import path of the application can be configured through the _application.yml_ configuration file . The 
`statistic.folder` property defines the folder being watched by the application, the path can either be relative of 
absolute.

`.json` files present in the specified folder and all its child directories will be read by the application.
Imported data will be added separately in the `Imported Data` hierarchy between each folder. Note that files located at 
the root of the `statistic.folder` path will be given the name `autoload+ LocalTime.now().toString()`.

It is not required to manually uncompress the export files before using them in the application.

### Data export

From ActivePivot 5.8.0, the `MemoryAnalysisService` class allows users to export the memory statistics of their 
application as a compressed `.json` file.

It is possible to configure your application to expose the export methods of the `MemoryAnalysisService` through a MBean: 

```java
@Bean
public JMXEnabler JMXMemoryMonitoringServiceEnabler() {
return new JMXEnabler(new MemoryAnalysisService(
    this.datastoreConfig.datastore(),
    this.apConfig.activePivotManager(),
    this.datastoreConfig.datastore().getEpochManager(),
    Paths.get(System.getProperty("java.io.tmpdir"))));
}
```

This additional bean will create a new JMX endpoint named _MemoryAnalysisService_. It offers several operations to 
export your application memory reports.  
Basically, you can export the whole application or a series of specific epochs. All methods require a folder name, in 
which the statistics will be written. This folder name will be created under the export directory - the OS temp 
directory, in the case of the above code.

Once exported, you can drop the resulting folder into your defined _statistic.folder_ and MAC will automatically load 
your data in the application.

You will find our more detailed guide about exporting and importing memory reports in 
[our guide directory](./guides/export-data.md).

Additional tools
----------------

### Uncompress generated files

ActivePivot memory tools will generate reports compressed with [Snappy](https://google.github.io/snappy/). As there are 
no standalone tool to easily decompress such files, this project provides a command to do so.  
Run `mvn exec:java@unsnappy -Dunsnappy.file=<path/to/your/file>` to extract the content.

Known limitations
-----------------

 * This tool does not support projects with multiple datastores. It is possible to import data from multiple datastores,
   but there are no ways to assign a store to a given datastore nor prevent conflicts between store names.
 * On-heap memory analysis is currently not fully implemented. The application only sums the memory used by Chunks.
   Please directly refer to the JMX beans of your application or other analysis tools for a deeper investigation.

Planned Improvements
--------------------

 [ ] Create a new store for Vectors, to have statistics on their size, ref count, ...
 [ ] Display and use the consumed On-heap memory reported by the Memory Analysis.
 
