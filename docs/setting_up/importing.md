# Data Import

The import path of the application can be configured through the
_application.yml_ configuration file . The `statistic.folder` property defines
the folder being watched by the application, the path can either be relative or
absolute. It is set to `./input_statistics` by default.

It is not required to manually decompress the exported files before using them
in the application.

## Importing Process

`.json` files present in the configured folder and all its child directories
will be read by the application.

Imported data will be tagged with the name of the enclosing subdirectory through
the `[Import info].[Import info]` hierarchy. Files located at the root of
statistics folder will be tagged with a generated name following the format
`autoload-<datetime>`.

For example, with the following data inside statistics:

```
statistics
 |- export-1
 |  |- store_Trade.json.sz
 |  '- store_Risk.json.sz
 '- export-2
    |- store_Trade.json.sz
    '- store_Risk.json.sz
```

MAC will load two batches of data tagged with _export-1_ and _export-2_. The two
batches can be selected and compared in the UI using the hierarchy `[Import
info].[Import info]`.

![loading-comparison](../assets/loading-comparison.png "Comparing the base
measures for two loaded reports")

## Automatic loading

MAC is monitoring the statistics directory for any changes. If you add a new
directory or new files in the directory, they will be automatically loaded.

This allows loading reports in several operations, dynamically add a new report
without restarting the application or update already imported reports.
