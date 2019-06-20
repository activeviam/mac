Memory Analysis Cube
==============

The Memory Analysis Cube application is a tool provided to allow some in-depth monitoring of the memory consumption of any ActivePivot application.


### Exporting memory statistics from your application

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

-----
Import configuration
 

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
