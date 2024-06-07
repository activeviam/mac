/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.pivot.utils.ApplicationInTests;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.store.IDatastore;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.query.QueryException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The scenario this test produces has a cube level {@code id} with a nullable dictionary. */
public class TestMissingChunkId {

  protected static final Path TEMP_DIRECTORY =
      QfsFileTestUtils.createTempDirectory(TestMultipleFieldsDictionary.class);
  @RegisterExtension protected LocalResourcesExtension resources = new LocalResourcesExtension();
  protected IDatastore monitoredDatastore;
  protected IActivePivotManager monitoredManager;
  protected Path statisticsPath;
  protected IDatastore monitoringDatastore;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  protected ApplicationInTests createAnalysisApplication() {
    final IDatastoreSchemaDescription desc =
        new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription();
    final IActivePivotManagerDescription manager =
        new ManagerDescriptionConfig().managerDescription();
    return ApplicationInTests.builder().withDatastore(desc).withManager(manager).build();
  }

  protected Collection<IMemoryStatistic> loadMemoryStatistic(final Path path) throws IOException {
    return Files.list(path)
        .map(
            file -> {
              try {
                return MemoryStatisticSerializerUtil.readStatisticFile(file.toFile());
              } catch (IOException exception) {
                throw new ActiveViamRuntimeException(exception);
              }
            })
        .collect(Collectors.toList());
  }

  protected void loadStatisticsIntoDatastore(
      final Collection<? extends IMemoryStatistic> statistics, final IDatastore analysisDatastore) {
    ATestMemoryStatistic.feedMonitoringApplication(analysisDatastore, statistics, "test");
  }

  @Test
  public void testStatisticLoading() throws IOException {
    final Path statisticsPath =
        Path.of("src", "test", "resources", "stats_files_with_missing_chunk_id");
    final Collection<IMemoryStatistic> memoryStatistics = loadMemoryStatistic(statisticsPath);

    final IDatastore analysisDatastore = (IDatastore) createAnalysisApplication().getDatabase();

    Assertions.assertDoesNotThrow(
        () -> loadStatisticsIntoDatastore(memoryStatistics, analysisDatastore));
  }

  @Test
  public void testGeneratio0nOfMissingChunkId() throws IOException, QueryException {
    final Path statisticsPath =
        Path.of("src", "test", "resources", "stats_files_with_missing_chunk_id");
    final Collection<IMemoryStatistic> memoryStatistics = loadMemoryStatistic(statisticsPath);

    final ApplicationInTests analysisApplication = createAnalysisApplication();
    resources.register(analysisApplication).start();
    loadStatisticsIntoDatastore(memoryStatistics, (IDatastore) analysisApplication.getDatabase());

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY Hierarchize("
                + "    Descendants("
                + "      {"
                + "        [Chunks].[ChunkId].[ALL].[AllMember]"
                + "      },"
                + "      1,"
                + "      SELF_AND_BEFORE"
                + "    )"
                + "  ) ON ROWS"
                + "  FROM [MemoryCube]");

    final CellSetDTO totalResult = analysisApplication.getSingleCube().execute(query);
    final List<String> chunkIds =
        totalResult.getAxes().get(0).getPositions().stream()
            .map(p -> p.getMembers().get(0).getCaption())
            .collect(Collectors.toList());
    final List<String> expectedChunkIds = List.of("-1", "-2", "-3");

    org.assertj.core.api.Assertions.assertThat(chunkIds).containsAll(expectedChunkIds);
  }
}
