Some settings can be configured in the `application.yml` file in MAC's
*resources* folder.

The usual ActivePivot spring application properties can be set in this file. A
small number of MAC specific settings can also be set:

* `statistic.folder`: the path to the statistics folder, from which MAC extracts
  the memory reports to analyze (default: *./statistics*)

* `bookmarks.reloadOnStartup`: whether or not to force the reloading of the
  predefined bookmarks in MAC's content server on startup (*true* or *false*,
  default: *false*)

  When *false*, the predefined bookmarks are loaded into the content server only
  the first time the MAC application is started.

  When *true*, the bookmarks are loaded on every startup, regardless of the
  bookmarks that may be present on the content server. Note that this causes all
  existing bookmarks to be **overriden**, including custom bookmarks that are
  not included in the predefined ones.

  Enable with care.
