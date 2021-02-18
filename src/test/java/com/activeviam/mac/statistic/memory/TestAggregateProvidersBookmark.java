/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescriptionWithPartialProviders;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestAggregateProvidersBookmark {

  @RegisterExtension
  protected static ActiveViamPropertyExtension propertyExtension =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  protected final LocalResourcesExtension resources = new LocalResourcesExtension();

  protected static Path tempDir =
      QfsFileTestUtils.createTempDirectory(TestAggregateProvidersBookmark.class);

  protected Application monitoredApplication;
  protected Application monitoringApplication;

  protected CubeTester tester;

  @BeforeEach
  public void setup() throws AgentException {
    monitoredApplication =
        MonitoringTestUtils.setupApplication(
            new MicroApplicationDescriptionWithPartialProviders(),
            resources,
            MicroApplicationDescriptionWithPartialProviders::fillWithGenericData);

    final Path exportPath =
        MonitoringTestUtils.exportMostRecentVersion(
            monitoredApplication.getDatastore(),
            monitoredApplication.getManager(),
            tempDir,
            this.getClass().getSimpleName());

    final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
    monitoringApplication = MonitoringTestUtils.setupMonitoringApplication(stats, resources);

    tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
  }

  @Test
  public void testPresentPartials() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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
        .containsExactlyInAnyOrder(
            new String[][] {
              new String[] {"AllMember", "Full"},
              new String[] {"AllMember", "BITMAP"}
            },
            new String[][] {
              new String[] {"AllMember", "Partial"},
              new String[] {"AllMember", "BITMAP"}
            },
            new String[][] {
              new String[] {"AllMember", "Partial"},
              new String[] {"AllMember", "LEAF"}
            });
  }

  @Test
  public void testFullAggregateStoreFields() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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
        .containsExactlyInAnyOrder(
            new String[] {"AllMember", "contributors.COUNT"},
            new String[] {"AllMember", "measure1.SUM"},
            new String[] {"AllMember", "measure2.SUM"},
            new String[] {"AllMember", "update.TIMESTAMP"});
  }

  @Test
  public void testFullAggregateStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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

    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(result, 0L, Long::sum))
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testPartialBitmapAggregateStoreFields() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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
        .containsExactlyInAnyOrder(
            new String[] {"AllMember", "contributors.COUNT"},
            new String[] {"AllMember", "measure1.SUM"},
            new String[] {"AllMember", "update.TIMESTAMP"});
  }

  @Test
  public void testPartialBitmapAggregateStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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

    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(result, 0L, Long::sum))
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testPartialLeafAggregateStoreFields() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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
        .containsExactlyInAnyOrder(
            new String[] {"AllMember", "contributors.COUNT"},
            new String[] {"AllMember", "measure2.SUM"},
            new String[] {"AllMember", "update.TIMESTAMP"});
  }

  @Test
  public void testPartialLeafAggregateStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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

    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(result, 0L, Long::sum))
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testCubeLevels() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

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
        .containsExactlyInAnyOrder(
            new String[] {"AllMember", "hierId@hierId@hierId"},
            new String[] {"AllMember", "id@id@id"});
  }
}
