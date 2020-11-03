/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreCalculations;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.dic.ISchemaDictionaryProvider;
import com.qfs.jmx.JmxOperation;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.msg.csv.ICsvDataProvider;
import com.qfs.msg.csv.IFileEvent;
import com.qfs.msg.csv.filesystem.impl.DirectoryCSVTopic;
import com.qfs.msg.impl.WatcherService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.store.IDatastore;
import com.qfs.store.impl.Datastore;
import com.qfs.store.transaction.IDatastoreSchemaTransactionInformation;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

/**
 * Configuration for the loading of data.
 *
 * @author ActiveViam
 */
@Configuration
public class SourceConfig {

  /** Logger. */
  private static final Logger LOGGER = Logger.getLogger(Loggers.LOADING);

  /** Autowired {@link Datastore} to be fed by this source. */
  @Autowired protected IDatastore datastore;

  /** Spring environment, automatically wired. */
  @Autowired protected Environment env;

  /**
   * Provides a {@link DirectoryCSVTopic topic}.
   *
   * <p>The provided topic is based on the content of the folder defined by the {@code
   * statistic.folder} environment property. By default, the property is defined in the {@code
   * ./src/main/resources/application.yml } ressource file.
   *
   * @return a topic based on the content of the directory.
   * @throws IllegalStateException if the required {@code statistic.folder} property is not defined
   *     in the environment
   */
  @Bean
  @Lazy
  public DirectoryCSVTopic statisticTopic() throws IllegalStateException {
    final String statisticFolder = env.getRequiredProperty("statistic.folder");
    if (LOGGER.isLoggable(Level.CONFIG)) {
      final Path folderPath = Paths.get(statisticFolder);
      LOGGER.config(
          "Using directory `"
              + folderPath.toAbsolutePath().toString()
              + "` to load data into the application");
    }

    return new DirectoryCSVTopic(
        "StatisticTopic",
        null,
        statisticFolder,
        // Process json files, compressed or not
        FileSystems.getDefault().getPathMatcher("glob:**.json*"),
        new WatcherService());
  }

  /**
   * Resolve the directory path.
   *
   * @param name the path name
   * @return the corresponding path object
   */
  protected Path resolveDirectory(String name) {
    Path directory = Paths.get(name);
    if (!Files.isDirectory(directory)) {

      // Try as a classpath resource
      final ClassLoader cl = Thread.currentThread().getContextClassLoader();
      URL url = null;
      try {
        url = cl.getResource(name);
      } catch (Exception e) {
        // PIVOT-3965 Some class loaders (e.g. spring-boot LaunchedURLClassLoader) may throw this
        // exception when encountering absolute paths on Windows, instead of returning null.
        if (LOGGER.isLoggable(Level.FINEST)) {
          LOGGER.log(
              Level.FINEST,
              "Suppressed an exception because directory '"
                  + directory
                  + "' was not found in the classpath.",
              e);
        }
      }
      if (url == null
          || !Files.isDirectory(directory = Paths.get(URI.create(url.toExternalForm())))) {
        throw new IllegalArgumentException("'" + name + "' could not be resolved to a directory.");
      }
    }
    return directory;
  }

  /** Triggers the reload of the entire directory and the processing of any new file in it. */
  public void loadStatistics() {
    final IFileEvent<Path> initialEvent = statisticTopic().fullReload();
    processEvent(initialEvent);
  }

  /** Starts to watch the folder that contains statistics. */
  public void watchStatisticDirectory() {
    statisticTopic().listen((topic, event) -> processEvent(event));
  }

  /**
   * Processes file events.
   *
   * <p>Only file creations/additions to the directory watched are processed. File modifications and
   * deletions are not supported.
   *
   * @param event event to be processed
   */
  public void processEvent(final IFileEvent<Path> event) {
    if (event != null && event.created() != null) {
      // Load stat
      final Collection<? extends ICsvDataProvider<Path>> providers = event.created();
      final Path path = resolveDirectory(env.getRequiredProperty("statistic.folder"));
      final Map<String, List<ICsvDataProvider<Path>>> dumpNames =
          providers.stream()
              .collect(
                  Collectors.groupingBy(
                      e -> {
                        Path pathEvent = Paths.get(e.getFileInfo().getFullName());
                        // we assume this is a file
                        String currentRelFolder = path.relativize(pathEvent.getParent()).toString();
                        return currentRelFolder.equalsIgnoreCase("")
                            ? "autoload-" + LocalTime.now().toString().replaceAll("\\.[^.]*$", "")
                            : currentRelFolder;
                      }));

      dumpNames.forEach(
          (dumpName, entry) -> {
            try {
              final Stream<IMemoryStatistic> inputs =
                  entry.stream()
                      .map(provider -> provider.getFileInfo().getIdentifier().toFile())
                      .parallel()
                      .map(
                          file -> {
                            try {
                              if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("Reading statistics from " + file.getAbsolutePath());
                              }
                              final IMemoryStatistic read =
                                  MemoryStatisticSerializerUtil.readStatisticFile(file);
                              if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("Statistics read from " + file.getAbsolutePath());
                              }
                              return read;
                            } catch (final IOException ioe) {
                              throw new RuntimeException("Cannot read statistics from " + file);
                            }
                          });
              final String message = feedDatastore(inputs, dumpName);
              LOGGER.info(message);
            } catch (Exception e) {
              throw new ActiveViamRuntimeException(e);
            }
          });
    }
  }

  /**
   * Feeds the {@link SourceConfig#datastore datastore} with a stream of {@link IMemoryStatistic}.
   *
   * @param memoryStatistics {@link IMemoryStatistic} to load.
   * @param dumpName name of the statistics {@code dumpName} field value
   * @return message to the user
   */
  public String feedDatastore(
      final Stream<IMemoryStatistic> memoryStatistics, final String dumpName) {
    final Optional<IDatastoreSchemaTransactionInformation> info =
        this.datastore.edit(
            tm -> {
              memoryStatistics.forEach(
                  stat -> {
                    if (LOGGER.isLoggable(Level.FINE)) {
                      LOGGER.fine("Starting feed the application with " + stat);
                    }

                    stat.accept(new FeedVisitor(this.datastore.getSchemaMetadata(), tm, dumpName));

                    if (LOGGER.isLoggable(Level.FINE)) {
                      LOGGER.fine("Application processed " + stat);
                    }
                  });

              final ISchemaDictionaryProvider dictionaries = datastore.getDictionaries();
              DatastoreCalculations.fillLatestEpochColumn(tm,
                  dictionaries.getDictionary(
                      DatastoreConstants.CHUNK_STORE,
                      DatastoreConstants.VERSION__EPOCH_ID),
                  dictionaries.getDictionary(
                      DatastoreConstants.CHUNK_STORE,
                      DatastoreConstants.CHUNK_ID),
                  dictionaries.getDictionary(
                      DatastoreConstants.CHUNK_STORE,
                      DatastoreConstants.CHUNK__DUMP_NAME));
            });

    if (info.isPresent()) {
      return "Commit successful for dump " + dumpName + " at epoch " + info.get().getId() + ".";
    } else {
      return "Issue during the commit";
    }
  }

  /**
   * Loads a statistics file into the application datastore.
   *
   * @param path path to the statistics file
   * @return message to the user
   * @throws IOException if an If an I/O error occurred during the file reading.
   */
  @JmxOperation(
      desc = "Load statistic from file.",
      name = "Load a IMemoryStatistic",
      params = {"path"})
  public String loadDumpedStatistic(String path) throws IOException {
    return feedDatastore(
        Stream.of(MemoryStatisticSerializerUtil.readStatisticFile(Paths.get(path).toFile())),
        Paths.get(path).getFileName().toString().replaceAll("\\.[^.]*$", ""));
  }
}
