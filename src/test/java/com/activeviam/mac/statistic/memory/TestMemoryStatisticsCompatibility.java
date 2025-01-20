package com.activeviam.mac.statistic.memory;

import static org.assertj.core.api.Assertions.assertThat;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.server.impl.api.dataexport.DataExportServiceBuilder;
import com.activeviam.activepivot.server.intf.api.dataexport.IDataExportService;
import com.activeviam.activepivot.server.json.api.dataexport.IJsonOutputConfiguration;
import com.activeviam.activepivot.server.json.api.dataexport.JsonCsvTabularOutputConfiguration;
import com.activeviam.activepivot.server.json.api.dataexport.JsonDataExportOrder;
import com.activeviam.activepivot.server.json.api.query.JsonMdxQuery;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Checks that exports from various Atoti Server versions can be loaded and that querying them
 * returns the expected results.
 *
 * <p>To test a new version:
 *
 * <ol>
 *   <li>Create an export from the sandbox.
 *   <li>Copy it in a new subdirectory of the {@value BASE_DATA_DIRECTORY_NAME} resource directory.
 *   <li>Run {@link #testGenerateExpectedQueryResults(Path, Path)} to generate a file containing the
 *       results of {@link #STATISTICS_QUERY} and manually check that the generated file is correct.
 * </ol>
 */
public class TestMemoryStatisticsCompatibility extends ATestMemoryStatistic {

  private static final String STATISTICS_QUERY =
      """
      SELECT
        NON EMPTY {
          [Measures].[contributors.COUNT],
          [Measures].[HeapMemory.SUM],
          [Measures].[DirectMemory.SUM]
        } ON COLUMNS,
        NON EMPTY Crossjoin(
          Hierarchize(
            Descendants(
              {
                [Owners].[Owner].[ALL].[AllMember]
              },
              1,
              SELF_AND_BEFORE
            )
          ),
          Hierarchize(
            Descendants(
              {
                [Components].[Component].[ALL].[AllMember]
              },
              1,
              SELF_AND_BEFORE
            )
          )
        ) ON ROWS
        FROM [MemoryCube]
      """;
  private static final String BASE_DATA_DIRECTORY_NAME = "compatibility_exports";
  private static final String QUERY_RESULTS_SUFFIX = "_query_results.csv";
  private static Path baseDataDirectory;
  private ApplicationInTests<IInternalDatastore> monitoringApp;

  @BeforeAll
  public static void setupAll() {
    RegistryInitializationConfig.setupRegistry();

    final URL baseDataDirectoryUrl =
        TestMemoryStatisticsCompatibility.class
            .getClassLoader()
            .getResource(BASE_DATA_DIRECTORY_NAME);
    try {
      baseDataDirectory = Path.of(Objects.requireNonNull(baseDataDirectoryUrl).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  public void initializeMonitoringApplication() {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();
    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();
    this.resources.register(this.monitoringApp);
    this.monitoringApp.start();
  }

  private static List<Path> getExportDirectories() throws IOException {
    try (Stream<Path> children = Files.list(baseDataDirectory)) {
      return children.filter(Files::isDirectory).sorted().toList();
    }
  }

  @ParameterizedTest
  @MethodSource("getExportDirectories")
  public void testQueryWithExport(
      final Path exportDirectory, @TempDir final Path dataExportServiceDirectory)
      throws IOException {
    loadExport(exportDirectory);
    final String actualQueryResults = queryStatistics(dataExportServiceDirectory);
    final Path expectedQueryResultsPath =
        baseDataDirectory.resolve(getExpectedQueryResultsFileName(exportDirectory));
    final String expectedQueryResults = Files.readString(expectedQueryResultsPath);
    assertThat(actualQueryResults).isEqualTo(expectedQueryResults);
  }

  @ParameterizedTest
  @MethodSource("getExportDirectories")
  @Disabled("This test should only be run manually to generate files for expected results")
  public void testGenerateExpectedQueryResults(
      final Path exportDirectory, @TempDir final Path dataExportServiceDirectory)
      throws IOException {
    loadExport(exportDirectory);
    final String queryResults = queryStatistics(dataExportServiceDirectory);
    final String outputFileName = getExpectedQueryResultsFileName(exportDirectory);
    final Path outputPath =
        Path.of("src", "test", "resources", BASE_DATA_DIRECTORY_NAME, outputFileName);
    Files.writeString(outputPath, queryResults);
  }

  private void loadExport(final Path exportDirectory) throws IOException {
    final String dumpName = exportDirectory.getFileName().toString();
    final Collection<? extends AMemoryStatistic> statistics =
        retroCompatiblyLoadMemoryStatFromFolder(exportDirectory);
    feedMonitoringApplication(this.monitoringApp.getDatabase(), statistics, dumpName);
  }

  private String queryStatistics(final Path dataExportServiceDirectory) throws IOException {
    final IDataExportService dataExportService =
        new DataExportServiceBuilder()
            .withManager(this.monitoringApp.getManager())
            .withDirectory(dataExportServiceDirectory)
            .build();

    final StreamingResponseBody streamingResponseBody =
        dataExportService.streamMdxQuery(
            new JsonDataExportOrder(
                new JsonMdxQuery(STATISTICS_QUERY, Map.of()),
                Map.of(
                    IJsonOutputConfiguration.FORMAT_PROPERTY,
                    JsonCsvTabularOutputConfiguration.PLUGIN_KEY)));

    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      streamingResponseBody.writeTo(outputStream);
      return outputStream.toString();
    }
  }

  private String getExpectedQueryResultsFileName(final Path exportDirectory) {
    final String dumpName = exportDirectory.getFileName().toString();
    return dumpName + QUERY_RESULTS_SUFFIX;
  }
}
