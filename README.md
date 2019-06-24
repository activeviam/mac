Memory Analysis Cube
==============

The Memory Analysis Cube is a project that allows the exploration and analysis of the data consumption from any ActivePivot application. 
This project builds as a stand-alone SpringBoot jar file that with an embedded ActiveUI app available on `localhost:9092`

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
- On heap memory footprint ( Warning, this measure only aggregates data on chunks, but not all on-heap objects held by ActivePivot on heap are using chunks, do not use this measure to assume total on-heap memory usage by Active Pivot)
- Amount/Ratio of free rows in chunks
- Amount/Ration of deleted rows in chunks
- Total direct memory footprint of an application
- Total on-heap memory footprint of an application 

Multi-dimentionnal analysis can be done of the loaded data through the following dimensions :
- Owner : Chunks are raw data, but they are always held by an underlying structure
- Component
- Class

#### Additional Cubes
These cubes can provide additionnal information about specific structures such as references, indices or dictionaries.


### Data Import

 The import path of the application can be configured through the `application.yml` configuration file . The `statistic.folder` property defines the folder being watched by the application, the path can either be relative of absolute.

 `.json` files present in the specified folder and all its child directories will be read by the application.
Imported data will be added separately in the `Imported Data` hierarchy between each folder, the root of the `statistic.folder` path will be given the name `autoload+ LocalTime.now().toString()`

It is not required to manually uncompress the export files before using them in the application.

### Data export

From ActivePivot 5.8.0, the `MemoryAnalysisService` class allows users to export the memory statistics of their application as a compressed`.json` file.

It is possible to configure your application to expose the export methods of the `MemoryAnalysisService` through a MBean : 

``` java
	@Bean
	public JMXEnabler JMXMemoryMonitoringServiceEnabler() {
		return new JMXEnabler(new MemoryAnalysisService(
				this.datastoreConfig.datastore(),
				this.apConfig.activePivotManager(),
				this.datastoreConfig.datastore().getEpochManager(),
				Paths.get(System.getProperty("java.io.tmpdir"))));
	}
```
 

Additional tools
-----

### Uncompress generated files

ActivePivot memory tools will generate reports compressed with [Snappy](https://google.github.io/snappy/). As there are no standalone tool
to easily decompress such files, this project provides a command to do so.  
Run `mvn exec:java@unsnappy -Dunsnappy.file=<path/to/your/file>` to extract the content.

Known limitations
-----

 * This tool does not support projects with multiple datastores.
 * On-heap memory analysis is currently not fully implemented. Please directly refer to the JMX beans of your application. 
