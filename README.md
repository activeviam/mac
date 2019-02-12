Memory Analysis Cube
==============

Additional tools
-----

### Uncompress generated files

ActivePivot memory tools will generate reports compressed with [Snappy](https://google.github.io/snappy/). As there are no standalone tool
to easily decompress such files, this project provides a command to do so.  
Run `mvn exec:java@unsnappy -Dunsnappy.file=<path/to/your/file>` to extract the content.

