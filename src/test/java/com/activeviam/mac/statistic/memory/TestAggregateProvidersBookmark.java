/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.pivot.utils.ApplicationInTests;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.server.cfg.IDatastoreSchemaDescriptionConfig;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAggregateProvidersBookmark extends ATestMemoryStatistic {

  protected ApplicationInTests<IDatastore> monitoredApp;
  protected ApplicationInTests<IDatastore> monitoringApp;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setup() throws AgentException {
    this.monitoredApp = createMicroApplicationWithPartialProviders();

    this.monitoredApp
        .getDatabase()
        .edit(tm -> IntStream.range(0, 20).forEach(i -> tm.add("A", i, i, i, i)));

    // Force to discard all versions
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    // perform GCs before exporting the store data
    performGC();
    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testOverview");

    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);

    // Start a monitoring datastore with the exported data
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();

    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();

    resources.register(monitoringApp).start();

    // Fill the monitoring datastore
    ATestMemoryStatistic.feedMonitoringApplication(
        this.monitoringApp.getDatabase(), List.of(stats), "storeA");

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  @Test
  public void testPresentPartials() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY Crossjoin("
                + "  [Aggregate Provider].[ProviderCategory].[ProviderCategory].Members,"
                + "  [Aggregate Provider].[ProviderType].[ProviderType].Members"
                + ") ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM ("
                + "  SELECT Except("
                + "    [Aggregate Provider].[ProviderCategory].[ProviderCategory].Members,"
                + "    [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[N/A]"
                + "  ) ON COLUMNS "
                + "FROM [MemoryCube]"
                + ")");

    final CellSetDTO result = pivot.execute(recordQuery);

    final String[][][] positions =
        result.getAxes().get(1).getPositions().stream()
            .map(
                p ->
                    p.getMembers().stream()
                        .map(m -> m.getPath().getPath())
                        .toArray(String[][]::new))
            .toArray(String[][][]::new);

    Assertions.assertThat(positions)
        .isEqualTo(
            new String[][][] {
              new String[][] {
                new String[] {"AllMember", "Full"}, new String[] {"AllMember", "BITMAP"}
              },
              new String[][] {
                new String[] {"AllMember", "Partial"},
                new String[] {"AllMember", "BITMAP"}
              },
              new String[][] {
                new String[] {"AllMember", "Partial"},
                new String[] {"AllMember", "LEAF"}
              }
            });
  }

  @Test
  public void testFullAggregateStoreFields() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Fields].[Field].[Field].Members ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Full],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[BITMAP]"
                + ")");

    final CellSetDTO result = pivot.execute(recordQuery);

    final String[][] positions =
        result.getAxes().get(1).getPositions().stream()
            .map(p -> p.getMembers().get(0).getPath().getPath())
            .toArray(String[][]::new);

    Assertions.assertThat(positions)
        .isEqualTo(
            new String[][] {
              new String[] {"AllMember", "contributors.COUNT"},
              new String[] {"AllMember", "measure1.SUM"},
              new String[] {"AllMember", "measure2.SUM"},
              new String[] {"AllMember", "update.TIMESTAMP"}
            });
  }

  @Test
  public void testFullAggregateStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY Except("
                + "  [Fields].[Field].[Field].Members,"
                + "  [Fields].[Field].[ALL].[AllMember].[N/A]"
                + ") ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Full],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[BITMAP]"
                + ")");

    final MDXQuery totalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Full],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[BITMAP]"
                + ")");

    final CellSetDTO result = pivot.execute(recordQuery);
    final CellSetDTO totalResult = pivot.execute(totalQuery);

    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(result))
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testPartialBitmapAggregateStoreFields() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Fields].[Field].[Field].Members ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Partial],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[BITMAP]"
                + ")");

    final CellSetDTO result = pivot.execute(recordQuery);

    final String[][] positions =
        result.getAxes().get(1).getPositions().stream()
            .map(p -> p.getMembers().get(0).getPath().getPath())
            .toArray(String[][]::new);

    Assertions.assertThat(positions)
        .isEqualTo(
            new String[][] {
              new String[] {"AllMember", "contributors.COUNT"},
              new String[] {"AllMember", "measure1.SUM"},
              new String[] {"AllMember", "update.TIMESTAMP"}
            });
  }

  @Test
  public void testPartialBitmapAggregateStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY Except("
                + "  [Fields].[Field].[Field].Members,"
                + "  [Fields].[Field].[ALL].[AllMember].[N/A]"
                + ") ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Partial],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[BITMAP]"
                + ")");

    final MDXQuery totalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Partial],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[BITMAP]"
                + ")");

    final CellSetDTO result = pivot.execute(recordQuery);
    final CellSetDTO totalResult = pivot.execute(totalQuery);

    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(result))
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testPartialLeafAggregateStoreFields() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Fields].[Field].[Field].Members ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Partial],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[LEAF]"
                + ")");

    final CellSetDTO result = pivot.execute(recordQuery);

    final String[][] positions =
        result.getAxes().get(1).getPositions().stream()
            .map(p -> p.getMembers().get(0).getPath().getPath())
            .toArray(String[][]::new);

    Assertions.assertThat(positions)
        .isEqualTo(
            new String[][] {
              new String[] {"AllMember", "contributors.COUNT"},
              new String[] {"AllMember", "measure2.SUM"},
              new String[] {"AllMember", "update.TIMESTAMP"}
            });
  }

  @Test
  public void testPartialLeafAggregateStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY Except("
                + "  [Fields].[Field].[Field].Members,"
                + "  [Fields].[Field].[ALL].[AllMember].[N/A]"
                + ") ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Partial],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[LEAF]"
                + ")");

    final MDXQuery totalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE ("
                + "  [Owners].[Owner Type].[ALL].[AllMember].[Cube],"
                + "  [Components].[Component].[ALL].[AllMember].[AGGREGATE_STORE],"
                + "  [Aggregate Provider].[ProviderCategory].[ALL].[AllMember].[Partial],"
                + "  [Aggregate Provider].[ProviderType].[ALL].[AllMember].[LEAF]"
                + ")");

    final CellSetDTO result = pivot.execute(recordQuery);
    final CellSetDTO totalResult = pivot.execute(totalQuery);

    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(result))
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testCubeLevels() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Fields].[Field].[Field].Members ON ROWS,"
                + "NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Components].[Component].[ALL].[AllMember].[LEVEL]");

    final CellSetDTO result = pivot.execute(recordQuery);

    final String[][] positions =
        result.getAxes().get(1).getPositions().stream()
            .map(p -> p.getMembers().get(0).getPath().getPath())
            .toArray(String[][]::new);

    Assertions.assertThat(positions)
        .isEqualTo(
            new String[][] {
              new String[] {"AllMember", "hierId@hierId@hierId"},
              new String[] {"AllMember", "id@id@id"}
            });
  }
}
