package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IMultiVersionActivePivot;
import com.activeviam.activepivot.server.impl.api.query.MDXQuery;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.intf.api.dto.CellSetDTO;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.core.api.query.QueryException;
import com.activeviam.tech.core.api.registry.Registry;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRatioMeasures extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 20;
  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;

  @BeforeAll
  public static void setupRegistry() {
    Registry.initialize(Registry.RegistryContributions.builder().build());
  }

  @BeforeEach
  public void setup() throws AgentException {
    this.monitoredApp = createMicroApplicationWithIsolatedStoreAndKeepAllEpochPolicy();

    this.monitoredApp
        .getDatabase()
        .edit(
            tm ->
                IntStream.range(0, ADDED_DATA_SIZE)
                    .forEach(
                        i -> {
                          tm.add("A", i, i / 10D);
                          tm.add("B", i, i / 10D);
                        }));

    // Force to discard all versions
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    // perform GCs before exporting the store data
    performGC();
    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testOverview");

    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);

    // Start a monitoring datastore with the exported data
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();

    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();

    resources.register(this.monitoringApp).start();

    // Fill the monitoring datastore
    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringApp.getDatabase(), List.of(stats), "storeA");

    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  @Test
  public void testDirectMemoryRatio() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery totalDirectMemory =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember]");

    final MDXQuery storeADirectMemory =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery storeADirectMemoryRatio =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.Ratio] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final CellSetDTO total = pivot.execute(totalDirectMemory);
    final CellSetDTO storeA = pivot.execute(storeADirectMemory);
    final CellSetDTO ratio = pivot.execute(storeADirectMemoryRatio);

    Assertions.assertThat(CellSetUtils.extractDoubleValueFromSingleCellDTO(ratio).doubleValue())
        .isEqualTo(0.5D);
    Assertions.assertThat(CellSetUtils.extractDoubleValueFromSingleCellDTO(ratio).doubleValue())
        .isEqualTo(
            CellSetUtils.extractValueFromSingleCellDTO(storeA)
                / CellSetUtils.extractValueFromSingleCellDTO(total).doubleValue());
  }

  @Test
  public void testCommittedRowsRatio() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery storeAcommittedRows =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[Used rows] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery storeAchunkSize =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery storeAcommittedRowsRatio =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[CommittedRows.Ratio] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final CellSetDTO committedRows = pivot.execute(storeAcommittedRows);
    final CellSetDTO chunkSize = pivot.execute(storeAchunkSize);
    final CellSetDTO ratio = pivot.execute(storeAcommittedRowsRatio);

    Assertions.assertThat(CellSetUtils.extractDoubleValueFromSingleCellDTO(ratio).doubleValue())
        .isNotIn(0D, 1D);
    Assertions.assertThat(CellSetUtils.extractDoubleValueFromSingleCellDTO(ratio).doubleValue())
        .isEqualTo(
            CellSetUtils.extractValueFromSingleCellDTO(committedRows)
                / CellSetUtils.extractValueFromSingleCellDTO(chunkSize).doubleValue());
  }
}
