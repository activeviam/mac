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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The scenario this test produces has a cube level {@code id} with a nullable dictionary. */
public class TestMissingChunkId {

  @RegisterExtension protected LocalResourcesExtension resources = new LocalResourcesExtension();
  private Collection<IMemoryStatistic> memoryStatistics;
  private final ApplicationInTests analysisApplication = createAnalysisApplication();;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setup() throws IOException {
    final Path statisticsPath =
        Path.of("src", "test", "resources", "stats_files_with_missing_chunk_id");
    memoryStatistics = loadMemoryStatistic(statisticsPath);

    resources.register(analysisApplication).start();
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
  public void testGeneratio0nOfMissingChunkId() throws QueryException {
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
    final List<String> negativeChunkIds =
        totalResult.getAxes().get(0).getPositions().stream()
            .map(p -> p.getMembers().get(0).getCaption())
            .filter(c -> c.startsWith("-"))
            .collect(Collectors.toList());

    org.assertj.core.api.Assertions.assertThat(negativeChunkIds).hasSize(3);
  }

  @Test
  public void testStatisticLoading() {
    final IDatastore analysisDatastore = (IDatastore) analysisApplication.getDatabase();

    Assertions.assertDoesNotThrow(
        () -> loadStatisticsIntoDatastore(memoryStatistics, analysisDatastore));
  }
}
