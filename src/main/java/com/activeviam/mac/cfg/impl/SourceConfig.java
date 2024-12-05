/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableList;

import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.internal.impl.Datastore;
import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
import com.activeviam.mac.statistic.memory.deserializer.RetroCompatibleDeserializer;
import com.activeviam.source.common.api.impl.WatcherService;
import com.activeviam.source.csv.api.DirectoryCsvTopic;
import com.activeviam.source.csv.api.ICsvDataProvider;
import com.activeviam.source.csv.api.IFileEvent;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import com.activeviam.tech.core.internal.monitoring.JmxOperation;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class SourceConfig {

  /** The name of the property that holds the path to the statistics folder. */
  public static final String STATISTIC_FOLDER_PROPERTY = "statistic.folder";

  private static final Logger LOGGER = Logger.getLogger(Loggers.LOADING);

  /** Autowired {@link Datastore} to be fed by this source. */
  private final IDatastore datastore;

  /** Spring environment, automatically wired. */
  private final Environment env;

  /**
   * Provides a {@link DirectoryCsvTopic topic}.
   *
   * <p>The provided topic is based on the content of the folder defined by the {@code
   * statistic.folder} environment property. By default, the property is defined in the {@code
   * ./src/main/resources/application.yml} resource file.
   *
   * @return a topic based on the content of the directory.
   * @throws IllegalStateException if the required {@code statistic.folder} property is not defined
   *     in the environment
   */
  @Bean
  @Lazy
  public DirectoryCsvTopic statisticTopic() throws IllegalStateException {
    final String statisticFolder = this.env.getRequiredProperty(STATISTIC_FOLDER_PROPERTY);
    final Path folderPath = Paths.get(statisticFolder);
    if (LOGGER.isLoggable(Level.INFO)) {
      LOGGER.info(
          "Using directory `"
              + folderPath.toAbsolutePath()
              + "` to load data into the application");
    }
    if (!Files.isDirectory(folderPath)) {
      throw new ActiveViamRuntimeException(
          "The path to the statistics folder : "
              + folderPath.toAbsolutePath()
              + " is not a correct path to a valid directory.");
    }
    return new DirectoryCsvTopic(
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
  protected Path resolveDirectory(final String name) {
    Path directory = Paths.get(name);
    if (!Files.isDirectory(directory)) {

      // Try as a classpath resource
      final ClassLoader cl = Thread.currentThread().getContextClassLoader();
      URL url = null;
      try {
        url = cl.getResource(name);
      } catch (final Exception e) {
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
      if (url != null) {
        directory = Paths.get(URI.create(url.toExternalForm()));
        if (!Files.isDirectory(directory)) {
          throw new IllegalArgumentException(
              "'" + name + "' could not be resolved to a directory.");
        }
      } else {
        throw new IllegalArgumentException("could not find  '" + name + "' in the classpath.");
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
    if (event != null) {
      // Load stat
      loadFromProviders(event);
    } else {
      // In the case where a new direction is added, we use the differential reload to access its
      // content and load it.
      final var diffEvent = statisticTopic().differentialReload();
      if (diffEvent != null) {
        loadFromProviders(diffEvent);
      }
    }
  }

  private void loadFromProviders(final IFileEvent<Path> event) {
    final var providers = collectProviders(event);
    if (!providers.isEmpty()) {
      final Map<String, List<Path>> dumpNames = collectDumpFiles(providers);
      loadDumps(dumpNames);
    }
  }

  private Collection<ICsvDataProvider<Path>> collectProviders(final IFileEvent<Path> event) {
    final var providers = new ArrayList<ICsvDataProvider<Path>>();
    if (event.created() != null) {
      providers.addAll(event.created());
    }
    if (event.modified() != null) {
      providers.addAll(event.modified());
    }
    return Collections.unmodifiableList(providers);
  }

  private Map<String, List<Path>> collectDumpFiles(
      final Collection<? extends ICsvDataProvider<Path>> providers) {
    final Path path = getStatisticFolder();
    return providers.stream()
        .collect(
            Collectors.groupingBy(
                e -> {
                  final Path pathEvent = e.getFileInfo().getIdentifier();
                  // we assume this is a file
                  final String currentRelFolder = path.relativize(pathEvent.getParent()).toString();
                  return currentRelFolder.equalsIgnoreCase("")
                      ? "autoload-" + LocalTime.now().toString().replaceAll("\\.[^.]*$", "")
                      : currentRelFolder;
                },
                mapping(e -> e.getFileInfo().getIdentifier(), toUnmodifiableList())));
  }

  private Path getStatisticFolder() {
    return resolveDirectory(this.env.getRequiredProperty(STATISTIC_FOLDER_PROPERTY));
  }

  private void loadDumps(final Map<String, List<Path>> dumpFiles) {
    dumpFiles.forEach(
        (dumpName, entry) -> {
          try {
            final Stream<AMemoryStatistic> inputs =
                entry.stream().parallel().map(RetroCompatibleDeserializer::readStatisticFile);
            final String message = feedDatastore(inputs, dumpName);
            LOGGER.info(message);
          } catch (final Exception e) {
            throw new ActiveViamRuntimeException(e);
          }
        });
  }

  /**
   * Feeds the {@link SourceConfig#datastore datastore} with a stream of {@link IMemoryStatistic}.
   *
   * @param memoryStatistics {@link IMemoryStatistic} to load.
   * @param dumpName name of the statistics {@code dumpName} field value
   * @return message to the user
   */
  public String feedDatastore(
      final Stream<AMemoryStatistic> memoryStatistics, final String dumpName) {
    final var info = new AnalysisDatastoreFeeder(dumpName, datastore).loadInto(memoryStatistics);
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
      name = "Load statistic directory",
      desc = "Load statistics from a full directory",
      params = {"path"})
  @SuppressWarnings("unused")
  public String loadDirectory(final String path) throws IOException {
    if (LOGGER.isLoggable(Level.INFO)) {
      LOGGER.info("Loading user data from " + path);
    }
    final long start = System.nanoTime();
    final Path dirPath = Paths.get(path);
    final String dumpName = dirPath.getFileName().toString();
    final List<Path> files = Files.list(dirPath).collect(toUnmodifiableList());
    loadDumps(Map.of(dumpName, files));
    final long end = System.nanoTime();

    if (LOGGER.isLoggable(Level.INFO)) {
      LOGGER.info("Loading complete for " + path);
    }
    return "Done (" + TimeUnit.NANOSECONDS.toMillis(end - start) + "ms)";
  }

  /**
   * Loads a statistics file into the application datastore.
   *
   * @param path path to the statistics file
   * @return message to the user
   */
  @JmxOperation(
      name = "Load statistic file",
      desc = "Load statistic from a single file.",
      params = {"path"})
  @SuppressWarnings("unused")
  public String loadFile(final String path) {
    if (LOGGER.isLoggable(Level.INFO)) {
      LOGGER.info("Loading user data from " + path);
    }
    final long start = System.nanoTime();
    final String dumpName = Paths.get(path).getFileName().toString().replaceAll("\\.[^.]*$", "");
    loadDumps(Map.of(dumpName, List.of(Paths.get(path))));
    final long end = System.nanoTime();

    if (LOGGER.isLoggable(Level.INFO)) {
      LOGGER.info("Loading complete for " + path);
    }
    return "Done (" + TimeUnit.NANOSECONDS.toMillis(end - start) + "ms)";
  }
}
