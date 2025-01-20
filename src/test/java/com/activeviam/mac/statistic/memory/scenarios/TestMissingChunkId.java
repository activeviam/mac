/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.description.IActivePivotManagerDescription;
import com.activeviam.activepivot.server.impl.api.query.MDXQuery;
import com.activeviam.activepivot.server.impl.api.query.MdxQueryUtil;
import com.activeviam.activepivot.server.intf.api.dto.CellSetDTO;
import com.activeviam.database.datastore.api.description.IDatastoreSchemaDescription;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.tech.core.api.query.QueryException;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.test.internal.junit.resources.Resources;
import com.activeviam.tech.test.internal.junit.resources.ResourcesExtension;
import com.activeviam.tech.test.internal.junit.resources.ResourcesHolder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** The scenario this test produces has a cube level {@code id} with a nullable dictionary. */
@ExtendWith({ResourcesExtension.class})
public class TestMissingChunkId {

  @Resources public ResourcesHolder resources;
  private Collection<AMemoryStatistic> memoryStatistics;
  private final ApplicationInTests analysisApplication = createAnalysisApplication();

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @BeforeEach
  public void setup() throws IOException {
    final Path statisticsPath =
        Path.of("src", "test", "resources", "stats_files_with_missing_chunk_id");
    memoryStatistics = ATestMemoryStatistic.retroCompatiblyLoadMemoryStatFromFolder(statisticsPath);

    resources.register(analysisApplication).start();
  }

  protected ApplicationInTests createAnalysisApplication() {
    final IDatastoreSchemaDescription desc =
        new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription();
    final IActivePivotManagerDescription manager =
        new ManagerDescriptionConfig().managerDescription();
    return ApplicationInTests.builder().withDatastore(desc).withManager(manager).build();
  }

  protected void loadStatisticsIntoDatastore(
      final Collection<? extends AMemoryStatistic> statistics,
      final IInternalDatastore analysisDatastore) {
    ATestMemoryStatistic.feedMonitoringApplication(analysisDatastore, statistics, "test");
  }

  @Test
  public void testGeneratio0nOfMissingChunkId() throws QueryException {
    loadStatisticsIntoDatastore(
        memoryStatistics, (IInternalDatastore) analysisApplication.getDatabase());

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

    final CellSetDTO totalResult = MdxQueryUtil.execute(analysisApplication.getManager(), query);
    final List<String> negativeChunkIds =
        totalResult.getAxes().get(0).getPositions().stream()
            .map(p -> p.getMembers().get(0).getCaption())
            .filter(c -> c.startsWith("-"))
            .collect(Collectors.toList());

    org.assertj.core.api.Assertions.assertThat(negativeChunkIds).hasSize(3);
  }

  @Test
  public void testStatisticLoading() {
    final IInternalDatastore analysisDatastore =
        (IInternalDatastore) analysisApplication.getDatabase();

    Assertions.assertDoesNotThrow(
        () -> loadStatisticsIntoDatastore(memoryStatistics, analysisDatastore));
  }
}
