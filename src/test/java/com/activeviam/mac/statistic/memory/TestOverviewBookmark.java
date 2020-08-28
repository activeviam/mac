/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplication;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.query.QueryException;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOverviewBookmark extends ASingleAppMonitoringTest {

  public static final int ADDED_DATA_SIZE = 20;

  @Override
  protected IDatastoreSchemaDescription datastoreSchema() {
    return MicroApplication.datastoreDescription();
  }

  @Override
  protected IActivePivotManagerDescription managerDescription(
      IDatastoreSchemaDescription datastoreSchema) {
    return MicroApplication.managerDescription(datastoreSchema);
  }

  @Override
  protected void beforeExport(
      IDatastore datastore, IActivePivotManager manager) {
    datastore.edit(tm ->
        IntStream.range(0, ADDED_DATA_SIZE).forEach(i ->
            tm.add("A", i * i)));
  }

  @Test
  public void testOverviewGrandTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery totalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS " + "FROM [MemoryCube]");

    final CellSetDTO totalResult = pivot.execute(totalQuery);

    final StatisticsSummary summary = computeStatisticsSummary(statistics);

    Assertions.assertThat(CellSetUtils.<Long>extractValueFromSingleCellDTO(totalResult))
        .isEqualTo(summary.offHeapMemory);
  }

  @Test
  public void testOverviewOwnerTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery totalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
                + " FROM [MemoryCube]");

    final MDXQuery perOwnerQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
                + "[Owners].[Owner].[ALL].[AllMember].Children ON ROWS "
                + "FROM [MemoryCube]");

    final MDXQuery excessMemoryQuery =
        new MDXQuery(
            "WITH"
                + " MEMBER [Measures].[ExcessDirectMemory] AS"
                + " Sum("
                + "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "   ([Measures].[Owner.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
                + " )"
                + " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
                + " FROM [MemoryCube]");

    final CellSetDTO totalResult = pivot.execute(totalQuery);
    final CellSetDTO perOwnerResult = pivot.execute(perOwnerQuery);
    final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

    Assertions.assertThat(
        CellSetUtils.<Long>extractValuesFromCellSetDTO(perOwnerResult)
            .stream().reduce(0L, Long::sum)
            - CellSetUtils.<Double>extractValueFromSingleCellDTO(excessMemoryResult)
            .longValue())
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testOverviewStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery storeTotalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery perComponentsStoreQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
                + "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery excessMemoryQuery =
        new MDXQuery(
            "WITH"
                + " MEMBER [Measures].[ExcessDirectMemory] AS"
                + " Sum("
                + "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "   ([Measures].[Component.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
                + " )"
                + " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final CellSetDTO storeTotalResult = pivot.execute(storeTotalQuery);
    final CellSetDTO perComponentStoreResult = pivot.execute(perComponentsStoreQuery);
    final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

    Assertions.assertThat(
        CellSetUtils.<Long>extractValuesFromCellSetDTO(perComponentStoreResult)
            .stream().reduce(0L, Long::sum)
            - CellSetUtils.<Double>extractValueFromSingleCellDTO(excessMemoryResult)
            .longValue())
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(storeTotalResult));
  }

  @Test
  public void testOverviewCubeTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery cubeTotalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

    final MDXQuery perComponentCubeQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS,"
                + "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

    final MDXQuery excessMemoryQuery =
        new MDXQuery(
            "WITH"
                + " MEMBER [Measures].[ExcessDirectMemory] AS"
                + " Sum("
                + "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "   ([Measures].[Component.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
                + " )"
                + " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

    final CellSetDTO cubeTotalResult = pivot.execute(cubeTotalQuery);
    final CellSetDTO perComponentCubeResult = pivot.execute(perComponentCubeQuery);
    final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

    Assertions.assertThat(
        CellSetUtils.<Long>extractValuesFromCellSetDTO(perComponentCubeResult)
            .stream().reduce(0L, Long::sum)
            - CellSetUtils.<Double>extractValueFromSingleCellDTO(excessMemoryResult)
            .longValue())
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(cubeTotalResult));
  }
}
